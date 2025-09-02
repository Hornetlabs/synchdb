/*
 * olr_client.h
 *
 * Implementation of Openlog Replicator Client
 *
 * Copyright (c) Hornetlabs Technology, Inc.
 *
 */

#ifndef SYNCHDB_OLR_CLIENT_H_
#define SYNCHDB_OLR_CLIENT_H_

#include "postgres.h"
#include "synchdb/synchdb.h"
#include "utils/netio_utils.h"

/**
 * RedoResponseCode - Enum representing response code from OLR
 */
typedef enum _ResponseCode
{
	RES_READY = 0,
	RES_FAILED_START,
	RES_STARTING,
	RES_ALREADY_STARTED,
	RES_REPLICATE,
	RES_PAYLOAD,
	RES_INVALID_DATABASE,
	RES_INVALID_COMMAND
} ResponseCode;

int olr_client_start_or_cont_replication(char * source, bool which);
int olr_client_init(const char * hostname, unsigned int port);
void olr_client_shutdown(void);
int olr_client_get_change(int myConnectorId, bool * dbzExitSignal,
		SynchdbStatistics * myBatchStats, bool * sendconfirm);
void olr_client_set_scns(orascn scn, orascn c_scn);
orascn olr_client_get_c_scn(void);
orascn olr_client_get_scn(void);
int olr_client_confirm_scn(char * source);
bool olr_client_write_scn_state(ConnectorType type, const char * name, const char * srcdb, bool force);
bool olr_client_init_scn_state(ConnectorType type, const char * name, const char * srcdb);
bool olr_client_get_connect_status(void);
bool olr_client_write_snapshot_state(ConnectorType type, const char * name, const char * dstdb, bool done);
bool olr_client_read_snapshot_state(ConnectorType type, const char * name, const char * dstdb, bool * done);

#endif /* SYNCHDB_OLR_CLIENT_H_ */
