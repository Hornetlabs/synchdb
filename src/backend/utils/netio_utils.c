/*
 * netio_utils.c
 *
 * Implementation of general network IO routines
 *
 * Copyright (c) Hornetlabs Technology, Inc.
 *
 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>

#include "postgres.h"
#include "utils/netio_utils.h"

int
netio_connect(NetioContext *ctx, const char *host, int port)
{
	int flags, optval = 1, ret = -1;
	struct addrinfo hints = {0}, *res = NULL;
	char portstr[16];

	snprintf(portstr, sizeof portstr, "%d", port);
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

	ret = getaddrinfo(host, portstr, &hints, &res);
	if (ret != 0 || !res)
	{
		elog(WARNING, "getaddrinfo(%s:%s): %s", host, portstr, gai_strerror(ret));
		return -1;
	}

	ctx->sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
	if (ctx->sockfd < 0)
	{
		return ret;
	}

	if (setsockopt(ctx->sockfd, SOL_SOCKET, SO_REUSEADDR,
			&optval, sizeof(optval)) < 0)
	{
		goto error;
	}


	if (setsockopt(ctx->sockfd, SOL_SOCKET, SO_KEEPALIVE,
			&optval, sizeof(optval)) < 0)
	{
		goto error;
	}


	if ((flags = fcntl(ctx->sockfd, F_GETFL, 0)) < 0 ||
		fcntl(ctx->sockfd, F_SETFL, flags | O_NONBLOCK) < 0)
	{
		goto error;
	}

	if (connect(ctx->sockfd, res->ai_addr, res->ai_addrlen) < 0)
	{
		int errnum = errno;
		if (errnum == EINPROGRESS)
		{
			struct pollfd pfd = { .fd = ctx->sockfd, .events = POLLOUT };
			if (poll(&pfd, 1, 5000) > 0)	/* todo: configurable conn timeout */
			{
				int err = 0;
				socklen_t len = sizeof(err);
				if (getsockopt(ctx->sockfd, SOL_SOCKET, SO_ERROR, &err, &len) == 0 && err == 0)
				{
					/* Connection established */
					elog(DEBUG1, "OLR connected");
				}
				else
				{
					/* Connection failed: err has errno-like value */
					elog(WARNING, "connect failed: %s (errno=%d)", strerror(err), err);
					ret = -1;
					goto error;
				}
			}
			else
			{
				elog(WARNING, "connect timed out or poll error");
				ret = -1;
				goto error;
			}
		}
		else
		{
			elog(WARNING, "connect failed errno = %d", errnum);
			ret = -1;
			goto error;
		}
	}

	strncpy(ctx->host, host, sizeof(ctx->host) - 1);
	ctx->port = port;
	ctx->is_connected = true;

	/* success if code reaches here */
	ret = 0;
error:
	/* close the socket on error */
	if (ret)
		close(ctx->sockfd);
	return ret;
}

ssize_t
netio_write(NetioContext *ctx, const void *buf, size_t len)
{
	if (!ctx->is_connected)
		return -1;

	return send(ctx->sockfd, buf, len, 0);
}

ssize_t
netio_read(NetioContext *ctx, StringInfoData * buf, int size)
{
	fd_set readfds;
	struct timeval timeout = {2, 0};
	char tmp[8192];
	ssize_t total_read = 0;
	int sel = -1;

	if (!ctx || !ctx->is_connected || !buf)
		return -1;

	FD_ZERO(&readfds);
	FD_SET(ctx->sockfd, &readfds);

	sel = select(ctx->sockfd + 1, &readfds, NULL, NULL,
			&timeout);
	if (sel <= 0)
	{
		/* No data to read or error */
		return -1;
	}

	/* unspecified size - read as much as possible until EAGAIN, EWOULDBLOCK or error */
	if (size == -1)
	{
		while (true)
		{
			ssize_t n = recv(ctx->sockfd, tmp, sizeof(tmp), 0);
			if (n > 0)
			{
				appendBinaryStringInfo(buf, tmp, n);
				total_read += n;
			}
			else if (n == 0)
			{
				/* Peer closed connection */
				elog(WARNING, "peer disconnected");
				ctx->is_connected = false;
				break;
			}
			else
			{
				if (errno == EAGAIN || errno == EWOULDBLOCK)
					break;  /* no more data to read or now */
				if (errno == EINTR)
					continue;  /* try again */

				/* recv error */
				elog(WARNING, "recv error");
				ctx->is_connected = false;
				return -1;
			}
		}
	}
	else
	{
		ssize_t remaining = size;
		while (remaining > 0)
		{
			ssize_t to_read = remaining < sizeof(tmp) ? remaining : sizeof(tmp);

			ssize_t n = recv(ctx->sockfd, tmp, to_read, 0);
			if (n > 0)
			{
				appendBinaryStringInfo(buf, tmp, n);
				total_read += n;
				remaining -= n;
			}
			else if (n == 0)
			{
				elog(WARNING, "peer disconnected");
				ctx->is_connected = false;
				break;
			}
			else
			{
				if (errno == EAGAIN || errno == EWOULDBLOCK)
					break;
				if (errno == EINTR)
					continue;

				/* recv error */
				elog(WARNING, "recv error");
				ctx->is_connected = false;
				return -1;
			}
		}
	}

	if (total_read == 0)
		return -1;

	return total_read;
}

void
netio_disconnect(NetioContext *ctx)
{
    if (ctx->is_connected)
    {
        close(ctx->sockfd);
        ctx->is_connected = false;
    }
}
