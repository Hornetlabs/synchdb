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
#include "utils/builtins.h"
#include "runtime/config.h"
#include "olr/olr_handoff.h"
#include "miscadmin.h"
#include "utils/jsonfuncs.h"
#include "utils/memutils.h"
#include "runtime/worker_runtime.h"

/* static globals */
static NetioContext g_netioCtx = {.sockfd = -1};
static orascn g_scn = 0;
static orascn g_c_scn = 0;
static orascn g_c_idx = 0;
static StringInfoData g_strinfo;
static int g_offset = 0;
static int g_read_buffer_size = 64 * 1024 * 1024;
static orascn g_start_scn;
static uint64 g_start_sequence;
static bool g_fresh_handoff;
static uint64 g_maximum_start_scn;
static uint64 g_source_boundary;

int
olr_client_init(const char * hostname, unsigned int port)
{
	g_read_buffer_size = olr_read_buffer_size * 1024 * 1024;
	netio_set_timeouts(olr_connect_timeout_ms, olr_read_timeout_ms);
	if (g_strinfo.data)
		resetStringInfo(&g_strinfo);
	else
		initStringInfo(&g_strinfo);
	g_offset = 0;
	if (netio_connect(&g_netioCtx, hostname, port))
	{
		elog(WARNING, "failed to connect to OLR");
		return -1;
	}
	return 0;
}

int
olr_client_start_or_cont_replication(const char * source, bool which)
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
	request.database_name = (char *) source;
	request.tm_val_case = OPEN_LOG_REPLICATOR__PB__REDO_REQUEST__TM_VAL_SCN;
	request.scn = g_start_scn ? g_start_scn : olr_client_get_scn();
	request.seq = g_start_sequence;
	request.c_scn = olr_client_get_c_scn();
	request.c_idx = olr_client_get_c_idx();

	elog(WARNING, "requested scn %lu c_scn %lu, nschema %ld",
			request.scn, request.c_scn, request.n_schema);

	len = open_log_replicator__pb__redo_request__get_packed_size(&request);
	/* The pinned server uses proto2 presence. Our proto3 encoder omits zero
	 * c_idx, which otherwise makes CONTINUE inherit the server's old index. */
	buf = palloc0(len + 6);
	if (!request.c_idx)
		len += 2;

	/* message length - 4 bytes */
	memcpy(buf, &len, 4);

	/* encode message with protobuf-c */
	open_log_replicator__pb__redo_request__pack(&request, buf + 4);
	if (!request.c_idx)
	{
		buf[len + 2] = 0x48;
		buf[len + 3] = 0;
	}

	/* send encoded message to olr */
	nbytes = netio_write(&g_netioCtx, buf, len + 4);
	pfree(buf);

	/* read 4 byte length header from olr first */
	initStringInfo(&strinfo);
	{
		TimestampTz deadline = GetCurrentTimestamp() + 120 * USECS_PER_SEC;
		while (strinfo.len < 4 && g_netioCtx.is_connected && GetCurrentTimestamp() < deadline)
		{
			CHECK_FOR_INTERRUPTS();
			netio_read(&g_netioCtx, &strinfo, 4 - strinfo.len);
		}
		nbytes = strinfo.len;
	}
	if (nbytes == 4)
	{
		/* then read the payload */
		int size = -1;
		memcpy(&size, strinfo.data, sizeof(int));
		if (size <= 0 || size > 1024 * 1024)
			elog(ERROR, "invalid OLR handshake frame size %d", size);
		{
			TimestampTz deadline = GetCurrentTimestamp() + 120 * USECS_PER_SEC;
			while (strinfo.len < size + 4 && g_netioCtx.is_connected && GetCurrentTimestamp() < deadline)
			{
				CHECK_FOR_INTERRUPTS();
				netio_read(&g_netioCtx, &strinfo, size + 4 - strinfo.len);
			}
			if (strinfo.len != size + 4)
				elog(ERROR, "incomplete OLR handshake response");
			nbytes = strinfo.len;
		}
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
			if (which && g_start_scn && (retcode == RES_REPLICATE || retcode == RES_ALREADY_STARTED))
			{
				if (response->scn_val_case != OPEN_LOG_REPLICATOR__PB__REDO_RESPONSE__SCN_VAL_SCN ||
					response->scn > g_maximum_start_scn ||
					(!g_fresh_handoff && response->c_scn != UINT64_MAX &&
					 (response->c_scn > g_c_scn ||
					  (response->c_scn == g_c_scn && response->c_idx > g_c_idx))))
					elog(ERROR, "OLR server has advanced beyond the owned replay boundary");
				g_source_boundary = response->scn;
				if (g_fresh_handoff)
				{
					/* LWN can precede a transaction's commit SCN. Never seek
					 * using an Oracle SCN as if it were an OLR coordinate. */
					g_c_scn = response->c_scn == UINT64_MAX ? 0 : response->c_scn;
					g_c_idx = response->c_scn == UINT64_MAX ? 0 : response->c_idx;
				}
			}
			open_log_replicator__pb__redo_response__free_unpacked(response, NULL);
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

/* Scan only complete source transactions. The buffer is retained across reads;
 * neither data nor its cursor is committed at an arbitrary TCP boundary. */
static int
complete_prefix(void)
{
    int offset = 0;
    int safe = 0;
    bool transaction = false;
    uint64 commit_scn = 0;

    while (offset + 4 <= g_strinfo.len)
    {
        uint32 size;
        char *json;
        Jsonb *record;
        char *op;
        uint64 scn;

        memcpy(&size, g_strinfo.data + offset, 4);
        if (size == 0 || size > 64 * 1024 * 1024)
            elog(ERROR, "invalid OLR JSON frame size %u", size);
        if (size > g_strinfo.len - offset - 4)
            break;
        json = pnstrdup(g_strinfo.data + offset + 4, size);
        record = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(json)));
        op = olr_json_text(olr_json_payload(record), "op");
        scn = olr_json_uint(record, "scn");
        if (!strcmp(op, "begin"))
        {
            if (transaction) elog(ERROR, "nested OLR source transaction");
            transaction = true;
            commit_scn = scn;
        }
        else if (!strcmp(op, "chkpt"))
        {
            if (transaction) elog(ERROR, "OLR checkpoint split a source transaction");
        }
        else
        {
            if (!transaction || scn != commit_scn)
                elog(ERROR, "OLR handoff requires transaction commit SCN on every payload (format.scn-type=3)");
            if (!strcmp(op, "commit")) transaction = false;
        }
        offset += 4 + size;
        if (!transaction) safe = offset;
        pfree(json);
        pfree(op);
        pfree(record);
    }
    return safe;
}

void
olr_client_verified_connect(const ConnectionInfo *connection, Jsonb *proof, bool resume)
{
    int ret;
    int offset = 0;
    bool found = false;
    bool committed = false;
    uint64 marker_scn = 0;
    char *token = olr_json_text(proof, "token");
    char *owner = olr_json_text(proof, "owner");
    TimestampTz deadline = GetCurrentTimestamp() + 180 * USECS_PER_SEC;
    MemoryContext scan;

    g_start_scn = olr_json_uint(proof, "start_scn");
    g_start_sequence = olr_json_uint(proof, "redo_sequence");
    g_fresh_handoff = !resume;
    g_maximum_start_scn = olr_json_uint(proof, resume ? "olr_first_data_scn" : "maximum_start_scn");
    olr_client_shutdown();
    if (olr_client_init(connection->olr.olr_host, connection->olr.olr_port))
        elog(ERROR, "could not connect to OLR for verified handoff");
    ret = olr_client_start_or_cont_replication(connection->olr.olr_source, true);
    if (ret == RES_ALREADY_STARTED)
        ret = olr_client_start_or_cont_replication(connection->olr.olr_source, false);
    if (ret != RES_REPLICATE)
        elog(ERROR, "OLR could not establish the owned replay boundary (response %d)", ret);
    scan = AllocSetContextCreate(CurrentMemoryContext, "OLR fence verification", ALLOCSET_DEFAULT_SIZES);
    while (GetCurrentTimestamp() < deadline && g_netioCtx.is_connected)
    {
        CHECK_FOR_INTERRUPTS();
        netio_read(&g_netioCtx, &g_strinfo, 64 * 1024);
        if (g_strinfo.len > g_read_buffer_size)
            elog(ERROR, "OLR source verification exceeded the configured replay buffer; no changes were acknowledged");
        while (offset + 4 <= g_strinfo.len)
        {
            uint32 size;
            Jsonb *record;
            Jsonb *payload;
            char *op;
            MemoryContext caller;

            memcpy(&size, g_strinfo.data + offset, 4);
            if (!size || size > 64 * 1024 * 1024) elog(ERROR, "invalid OLR fence frame size");
            if (size > g_strinfo.len - offset - 4) break;
            caller = MemoryContextSwitchTo(scan);
            record = DatumGetJsonbP(DirectFunctionCall1(jsonb_in,
                CStringGetDatum(pnstrdup(g_strinfo.data + offset + 4, size))));
            payload = olr_json_payload(record);
            op = olr_json_text(payload, "op");
            elog(DEBUG2, "OLR fence scan: op %s SCN %llu position %llu/%llu", op,
                (unsigned long long) olr_json_uint(record, "scn"),
                (unsigned long long) olr_json_uint(record, "c_scn"),
                (unsigned long long) olr_json_uint(record, "c_idx"));
            if (!strcmp(op, "c"))
            {
                Datum path[] = {CStringGetTextDatum("schema")};
                Jsonb *schema = GET_JSONB_ELEM(payload, path, 1);
                if (schema && !strcmp(olr_json_text(schema, "owner"), owner) &&
                    !strcmp(olr_json_text(schema, "table"), "SYNCHDB_OLR_FENCE"))
                {
                    Datum after_path[] = {CStringGetTextDatum("after")};
                    Jsonb *after = GET_JSONB_ELEM(payload, after_path, 1);
                    if (after && !strcmp(olr_json_text(after, "TOKEN"), token))
                    {
                        if (olr_json_uint(after, "SOURCE_DBID") != olr_json_uint(proof, "dbid") ||
                            olr_json_uint(after, "RESETLOGS_SCN") != olr_json_uint(proof, "resetlogs_scn") ||
                            olr_json_uint(after, "CON_ID") != olr_json_uint(proof, "con_id"))
                            elog(ERROR, "OLR fence source identity does not match Oracle");
                        found = true;
                        marker_scn = olr_json_uint(record, "scn");
                    }
                }
            }
            offset += size + 4;
            if (found && !strcmp(op, "commit"))
            {
                if (olr_json_uint(record, "scn") != marker_scn ||
                    marker_scn <= g_start_scn || marker_scn > olr_json_uint(proof, "fence_scn_upper_bound"))
                    elog(ERROR, "OLR fence does not carry its transaction commit SCN; set format.scn-type=3");
                committed = true;
            }
            if (committed && !strcmp(op, "chkpt"))
            {
                (void) complete_prefix();
                MemoryContextSwitchTo(caller);
                MemoryContextDelete(scan);
                elog(LOG, "OLR source fence verified at commit SCN %llu", (unsigned long long) marker_scn);
                return;
            }
            MemoryContextSwitchTo(caller);
            MemoryContextReset(scan);
        }
    }
    MemoryContextDelete(scan);
    elog(ERROR, "OLR did not deliver the committed Oracle source fence and checkpoint; require scn-type=3 and flags including 4096");
}

int
olr_client_get_change(int connector_id, bool *exit_signal, SynchdbStatistics *statistics,
                      bool *sendconfirm)
{
    int safe;
    int count = 0;
    MemoryContext batch;
    MemoryContext caller;
    bool previous_interruptible = synchdb_worker_sql_interruptible();

    if (!g_netioCtx.is_connected) return -2;
    netio_read(&g_netioCtx, &g_strinfo, 64 * 1024);
    if (g_strinfo.len > g_read_buffer_size)
        elog(ERROR, "OLR source transaction exceeds the configured replay buffer");
    batch = AllocSetContextCreate(CurrentMemoryContext, "OLR atomic source batch", ALLOCSET_DEFAULT_SIZES);
    caller = MemoryContextSwitchTo(batch);
    safe = complete_prefix();
    if (!safe)
    {
        MemoryContextSwitchTo(caller);
        MemoryContextDelete(batch);
        return -2;
    }
    MemoryContextSwitchTo(caller);
    synchdb_worker_sql_enter();
    PG_TRY();
    {
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    while (g_offset < safe)
    {
        uint32 size;
        void *event;
        int ret;

        memcpy(&size, g_strinfo.data + g_offset, 4);
#if SYNCHDB_PG_MAJOR_VERSION >= 1700
        event = cstring_to_text_with_len(g_strinfo.data + g_offset + 4, size);
#else
        event = pnstrdup(g_strinfo.data + g_offset + 4, size);
#endif
        ret = fc_processOLRChangeEvent(event, statistics,
            get_shm_connector_name_by_id(connector_id), sendconfirm, count == 0,
            g_offset + 4 + size == safe);
        if (ret < 0)
            elog(ERROR, "OLR source transaction could not be applied; its cursor was not committed or acknowledged");
        pfree(event);
        g_offset += 4 + size;
        count++;
    }
    olr_handoff_store_position();
    PopActiveSnapshot();
    CommitTransactionCommand();
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        AbortCurrentTransaction();
        MemoryContextSwitchTo(caller);
        synchdb_worker_sql_restore(previous_interruptible);
        MemoryContextDelete(batch);
        PG_RE_THROW();
    }
    PG_END_TRY();
    synchdb_worker_sql_restore(previous_interruptible);
    *sendconfirm = true;
    memmove(g_strinfo.data, g_strinfo.data + safe, g_strinfo.len - safe);
    g_strinfo.len -= safe;
    g_strinfo.data[g_strinfo.len] = '\0';
    g_offset = 0;
    increment_connector_statistics(statistics, STATS_TOTAL_CHANGE_EVENT, count);
    MemoryContextSwitchTo(caller);
    MemoryContextDelete(batch);
    return 0;
}

void
olr_client_shutdown(void)
{
	if (g_netioCtx.sockfd >= 0)
	{
		netio_disconnect(&g_netioCtx);
		elog(WARNING, "OLR disconnected");
		memset(&g_netioCtx, 0, sizeof(g_netioCtx));
		g_netioCtx.sockfd = -1;
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
	g_scn = scn;
	g_c_scn = c_scn;
	g_c_idx = c_idx;
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
olr_client_get_connect_status(void)
{
	return g_netioCtx.is_connected;
}

uint64
olr_client_get_source_boundary(void)
{
	return g_source_boundary;
}
