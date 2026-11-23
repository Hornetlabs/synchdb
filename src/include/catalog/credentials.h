/* Connector credentials initialized with the 1.0-compatible built-in key. */
#ifndef SYNCHDB_CREDENTIALS_H
#define SYNCHDB_CREDENTIALS_H

#include "utils/jsonb.h"

typedef struct SynchdbCredentials SynchdbCredentials;

/* A context owns one key collection, including its active key, per operation. */
extern SynchdbCredentials *synchdb_credentials_open(void);
extern void synchdb_credentials_close(SynchdbCredentials *credentials);
extern Jsonb *synchdb_credentials_encrypt(SynchdbCredentials *credentials,
										 const char *plaintext);
extern char *synchdb_credentials_decrypt(SynchdbCredentials *credentials,
										 Jsonb *stored);

#endif
