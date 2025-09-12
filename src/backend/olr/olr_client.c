/*
 * olr_client.c
 *
 * Implementation of Openlog Replicator Client
 *
 * Copyright (c) Hornetlabs Technology, Inc.
 *
 */

#include "olr/OraProtoBuf.pb-c.h"
#include "olr/olr_client.h"
#include "converter/olr_event_handler.h"
#include "storage/fd.h"
#include "utils/timestamp.h"
#include "utils/datetime.h"
#include "access/xact.h"
#include "utils/snapmgr.h"

/* extern globals */
extern int myConnectorId;
extern int dbz_offset_flush_interval_ms;
extern bool synchdb_log_event_on_error;
extern char * g_eventStr;
extern int olr_read_buffer_size;

/* static globals */
static NetioContext g_netioCtx = {0};
static orascn g_scn = 0;
static orascn g_c_scn = 0;
static orascn g_c_idx = 0;
static StringInfoData g_strinfo;
static int g_offset = 0;
static int g_read_buffer_size = 64 * 1024 * 1024;

int
olr_client_init(const char * hostname, unsigned int port)
{
	g_read_buffer_size = olr_read_buffer_size * 1024 * 1024;

	initStringInfo(&g_strinfo);
	if (netio_connect(&g_netioCtx, hostname, port))
	{
		elog(WARNING, "failed to connect to OLR");
		return -1;
	}
	return 0;
}

int
olr_client_start_or_cont_replication(char * source, bool which)
{
	StringInfoData strinfo;
	ssize_t nbytes = 0;
	size_t len;
	unsigned char *buf = NULL;
	int retcode = -1;

	OpenLogReplicator__Pb__RedoRequest request =
			OPEN_LOG_REPLICATOR__PB__REDO_REQUEST__INIT;

	if (!g_netioCtx.is_connected)
	{
		elog(WARNING, "no connection established to openlog replicator");
		return -1;
	}

	request.code = which ? OPEN_LOG_REPLICATOR__PB__REQUEST_CODE__START :
			OPEN_LOG_REPLICATOR__PB__REQUEST_CODE__CONTINUE;
	request.database_name = source;
	request.tm_val_case = OPEN_LOG_REPLICATOR__PB__REDO_REQUEST__TM_VAL_SCN;
	request.scn = olr_client_get_scn() == 0 ? olr_client_get_scn() :
			olr_client_get_scn() + 1;
	request.c_scn = olr_client_get_c_scn() == 0 ? olr_client_get_c_scn() :
			olr_client_get_c_scn() + 1; /* resume beyond last know c_scn */

	elog(WARNING, "requested scn %lu c_scn %lu, nschema %ld",
			request.scn, request.c_scn, request.n_schema);

	len = open_log_replicator__pb__redo_request__get_packed_size(&request);
	buf = palloc0(len + 4);

	/* message length - 4 bytes */
	memcpy(buf, &len, 4);

	/* encode message with protobuf-c */
	open_log_replicator__pb__redo_request__pack(&request, buf + 4);

	/* send encoded message to olr */
	nbytes = netio_write(&g_netioCtx, buf, len + 4);
	pfree(buf);

	/* read 4 byte length header from olr first */
	initStringInfo(&strinfo);
	nbytes = netio_read(&g_netioCtx, &strinfo, 4);
	if (nbytes == 4)
	{
		/* then read the payload */
		int size = -1;
		memcpy(&size, strinfo.data, sizeof(int));
		nbytes += netio_read(&g_netioCtx, &strinfo, size);
	}

	/* decode received message with protobuf-c */
	if (nbytes > 4)
	{
		/* skips 4 byte message length field */
		OpenLogReplicator__Pb__RedoResponse *response =
			open_log_replicator__pb__redo_response__unpack(NULL, nbytes - 4 ,
					(unsigned char*)strinfo.data + 4);
		pfree(strinfo.data);

		if (response)
		{
			retcode = response->code;
			return retcode;
		}
		else
		{
			elog(WARNING,"malformed protobuf message - NULL response");
			return -1;
		}
	}
	return -1;
}

int
olr_client_get_change(int myConnectorId, bool * dbzExitSignal, SynchdbStatistics * myBatchStats,
		bool * sendconfirm)
{
	ssize_t nbytes = 0;
	int ret = -1, curr = 0;
	int batchsize = 0;
	int tmp_offset = g_offset;

	if (!g_netioCtx.is_connected)
	{
		elog(WARNING, "no connection established to openlog replicator");
		return -2;
	}

	nbytes = netio_read(&g_netioCtx, &g_strinfo, g_read_buffer_size);
	if (nbytes > 0)
	{
		elog(DEBUG1, "%ld bytes read", nbytes);

		/*
		 * pre-scan - count how many complete json payloads in one read.
		 * This is needed to clearly identify first and last change events
		 * for the purpose of recording batch statistics.
		 */
		while (tmp_offset + 4 <= g_strinfo.len)
		{
			int json_len = 0;
			memcpy(&json_len, g_strinfo.data + tmp_offset, 4);

			if (tmp_offset + 4 + json_len > g_strinfo.len)
				break;

			batchsize++;
			tmp_offset += 4 + json_len;
		}
		elog(DEBUG1, "there are %d records in this batch", batchsize);

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		while (g_offset + 4 <= g_strinfo.len && curr < batchsize)
		{
			int json_len = 0;
			char * json_payload;
			memcpy(&json_len, g_strinfo.data + g_offset, 4);

			elog(DEBUG1, "json len %d", json_len);

			if (g_offset + 4 + json_len > g_strinfo.len)
			{
				/*
				 * not enough payload data, exit for now. More data is expected
				 * to be read in the next call
				 */
				elog(DEBUG1, "json_len is %d, but only %ld bytes left in buffer %ld/%d",
						json_len, (long)(g_strinfo.len - g_offset - 4), (long)g_offset,
						g_strinfo.len);
				break;
			}
			/*
			 * XXX: payload is not null-terminated so we make a copy of it first - kind of
			 * inefficient here.
			 */
			json_payload = pnstrdup(g_strinfo.data + g_offset + 4, json_len);

			if (synchdb_log_event_on_error)
				g_eventStr = json_payload;

			/* process it */
			ret = fc_processOLRChangeEvent(json_payload, myBatchStats,
					get_shm_connector_name_by_id(myConnectorId), sendconfirm,
					(curr == 0), (curr == batchsize - 1));

			pfree(json_payload);

			if (synchdb_log_event_on_error)
				g_eventStr = NULL;

			g_offset += 4 + json_len;
			curr++;
		}

		PopActiveSnapshot();
		CommitTransactionCommand();

		/* compact or reuse the buffer if available */
		if (g_offset >= g_strinfo.len)
		{
			resetStringInfo(&g_strinfo);
			g_offset = 0;
			elog(DEBUG1, "reset g_strinfo buffer");
		}
		else if (g_offset > 0)
		{
			memmove(g_strinfo.data, g_strinfo.data + g_offset, g_strinfo.len - g_offset);
			g_strinfo.len -= g_offset;
			g_strinfo.data[g_strinfo.len] = '\0';
			g_offset = 0;
			elog(DEBUG1, "reset and moved g_strinfo buffer content");
		}
		increment_connector_statistics(myBatchStats, STATS_TOTAL_CHANGE_EVENT, batchsize);

		/* this ret could be -1 for general failure or 0 for success */
		return ret;
	}
	return -2;
}

void
olr_client_shutdown(void)
{
	if (g_netioCtx.is_connected)
	{
		netio_disconnect(&g_netioCtx);
		elog(WARNING, "OLR disconnected");
		memset(&g_netioCtx, 0, sizeof(g_netioCtx));
	}
}

int
olr_client_confirm_scn(char * source)
{
	ssize_t nbytes = 0;
	size_t len;
	unsigned char *buf = NULL;
	OpenLogReplicator__Pb__RedoRequest request =
			OPEN_LOG_REPLICATOR__PB__REDO_REQUEST__INIT;

	if (olr_client_get_c_scn() == 0)
	{
		elog(WARNING, "no scn to confirm");
		return -1;
	}

	if (!g_netioCtx.is_connected)
	{
		elog(WARNING, "no connection established to openlog replicator");
		return -1;
	}

	request.code = OPEN_LOG_REPLICATOR__PB__REQUEST_CODE__CONFIRM;
	request.database_name = source;
	request.tm_val_case = OPEN_LOG_REPLICATOR__PB__REDO_REQUEST__TM_VAL_SCN;
	request.scn = olr_client_get_scn();
	request.c_scn = olr_client_get_c_scn();
	request.c_idx = olr_client_get_c_idx();

	len = open_log_replicator__pb__redo_request__get_packed_size(&request);
	buf = palloc0(len + 4);

	/* message length - 4 bytes */
	memcpy(buf, &len, 4);

	/* encode message with protobuf-c */
	open_log_replicator__pb__redo_request__pack(&request, buf + 4);

	/* send encoded message to olr */
	nbytes = netio_write(&g_netioCtx, buf, len + 4);
	elog(DEBUG1, "olr client sent %ld bytes to olr", nbytes);
	pfree(buf);

	return 0;
}

void
olr_client_set_scns(orascn scn, orascn c_scn, orascn c_idx)
{
	g_scn = scn > 0? scn : g_scn;
	g_c_scn = c_scn > 0 ? c_scn : g_c_scn;
	g_c_idx = c_idx > 0 ? c_idx : g_c_idx;
}

orascn
olr_client_get_c_scn(void)
{
	return g_c_scn;
}

orascn
olr_client_get_scn(void)
{
	return g_scn;
}

orascn
olr_client_get_c_idx(void)
{
	return g_c_idx;
}

bool
olr_client_write_scn_state(ConnectorType type, const char * name, const char * dstdb, bool force)
{
	int fd;
	static TimestampTz last_flush_time = 0;
	orascn buf[3] = {g_scn, g_c_scn, g_c_idx};
	char * filename = psprintf(SYNCHDB_OFFSET_FILE_PATTERN,
			get_shm_connector_name(type), name, dstdb);

	TimestampTz now = GetCurrentTimestamp();

    /* Only return early if !force and not enough time has passed */
    if (!force && last_flush_time != 0 &&
        !TimestampDifferenceExceeds(last_flush_time, now, dbz_offset_flush_interval_ms))
    {
        return false;
    }

    elog(DEBUG1, "flushing scn file %s...", filename);
	fd = OpenTransientFile(filename, O_WRONLY | O_CREAT | O_TRUNC);
	if (fd < 0)
	{
		set_shm_connector_errmsg(myConnectorId, "cannot open scn file to write!");
		elog(ERROR, "can not open file \"%s\" for writing: %m", filename);
	}

	elog(DEBUG1, "flushing... scn %llu, c_scn %llu", g_scn, g_c_scn);
	if (write(fd, buf, sizeof(buf)) != sizeof(buf))
	{
		int save_errno = errno;
		CloseTransientFile(fd);
		errno = save_errno;
		set_shm_connector_errmsg(myConnectorId, "cannot write to scn file");
		elog(ERROR, "cannot write to file \"%s\": %m", filename);
	}
	CloseTransientFile(fd);
	last_flush_time = now;
	return true;
}

bool
olr_client_init_scn_state(ConnectorType type, const char * name, const char * dstdb)
{
	int fd;
	orascn buf[3];
	char * filename = psprintf(SYNCHDB_OFFSET_FILE_PATTERN,
			get_shm_connector_name(type), name, dstdb);

	elog(DEBUG1, "reading scn file %s...", filename);
	fd = OpenTransientFile(filename, O_RDONLY);
	if (fd < 0)
		return false;

	if (read(fd, buf, sizeof(buf)) != sizeof(buf))
	{
		int save_errno = errno;
		CloseTransientFile(fd);
		errno = save_errno;
		set_shm_connector_errmsg(myConnectorId, "cannot read scn from file");
		elog(ERROR, "cannot read from file \"%s\": %m", filename);
	}

	CloseTransientFile(fd);

	g_scn = buf[0];
	g_c_scn = buf[1];
	g_c_idx = buf[2];
	elog(LOG, "initialize scn = %llu, c_scn = %llu, c_idx = %llu", g_scn, g_c_scn, g_c_idx);
	return true;
}

bool
olr_client_get_connect_status(void)
{
	return g_netioCtx.is_connected;
}

bool
olr_client_write_snapshot_state(ConnectorType type, const char * name, const char * dstdb, bool done)
{
	int fd;
	char * filename = psprintf(SYNCHDB_SCHEMA_FILE_PATTERN,
			get_shm_connector_name(type), name, dstdb);
	unsigned char state = done ? 't' : 'f';

    elog(DEBUG1, "writing snapshot state '%c' to %s...", state, filename);
	fd = OpenTransientFile(filename, O_WRONLY | O_CREAT | O_TRUNC);
	if (fd < 0)
	{
		set_shm_connector_errmsg(myConnectorId, "cannot open snapshot file to write!");
		elog(ERROR, "can not open file \"%s\" for writing: %m", filename);
	}

	if (write(fd, &state, sizeof(state)) != sizeof(state))
	{
		int save_errno = errno;
		CloseTransientFile(fd);
		errno = save_errno;
		set_shm_connector_errmsg(myConnectorId, "cannot write to snapshot file");
		elog(ERROR, "cannot write to file \"%s\": %m", filename);
	}
	CloseTransientFile(fd);
	return true;
}

bool
olr_client_read_snapshot_state(ConnectorType type, const char * name, const char * dstdb, bool * done)
{
	int fd;
	unsigned char state;
	char * filename = psprintf(SYNCHDB_SCHEMA_FILE_PATTERN,
			get_shm_connector_name(type), name, dstdb);

	elog(DEBUG1, "reading snapshot state file %s...", filename);
	fd = OpenTransientFile(filename, O_RDONLY);
	if (fd < 0)
		return false;

	if (read(fd, &state, sizeof(state)) != sizeof(state))
	{
		int save_errno = errno;
		CloseTransientFile(fd);
		errno = save_errno;
		set_shm_connector_errmsg(myConnectorId, "cannot read snapshot state from file");
		elog(ERROR, "cannot read from file \"%s\": %m", filename);
	}
	CloseTransientFile(fd);

	*done = state == 't' ? true : false;
	elog(LOG, "snapshot state read = %d", *done);
	return true;
}
