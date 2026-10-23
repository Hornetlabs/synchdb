/* Versioned connector credentials backed by a server-owned keyring file. */
#ifndef SYNCHDB_CREDENTIALS_H
#define SYNCHDB_CREDENTIALS_H

#include "utils/jsonb.h"

typedef struct SynchdbCredentials SynchdbCredentials;

extern void synchdb_credentials_register_guc(void);
/* A context pins one complete keyring, including its active key, per operation. */
extern SynchdbCredentials *synchdb_credentials_open(void);
extern void synchdb_credentials_close(SynchdbCredentials *credentials);
extern Jsonb *synchdb_credentials_encrypt(SynchdbCredentials *credentials,
										 const char *plaintext);
extern char *synchdb_credentials_decrypt(SynchdbCredentials *credentials,
										 Jsonb *envelope);

#endif
