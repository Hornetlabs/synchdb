/*
 * netio_utils.h
 *
 * Implementation of general network IO routines
 *
 * Copyright (c) Hornetlabs Technology, Inc.
 *
 */
#ifndef SYNCHDB_NETIO_UTILS_H_
#define SYNCHDB_NETIO_UTILS_H_

#include "synchdb/synchdb.h"

#define NETIO_NODATA				-1
#define NETIO_PEER_DISCONNECTED 	-2
#define NETIO_FATAL_ERROR			-3

typedef struct
{
	int sockfd;
	char host[SYNCHDB_CONNINFO_HOSTNAME_SIZE];
	int port;
	bool is_connected;
	int errcode;
} NetioContext;

int netio_connect(NetioContext *ctx, const char *host, int port);
ssize_t netio_write(NetioContext *ctx, const void *buf, size_t len);
ssize_t netio_read(NetioContext *ctx, StringInfoData * buf, int size);
void netio_disconnect(NetioContext *ctx);

#endif /* SYNCHDB_NETIO_UTILS_H_ */
