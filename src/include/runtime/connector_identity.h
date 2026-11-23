/*
 * connector_identity.h
 *
 * Stable catalog identity, distinct from a transient shared-memory slot.
 */
#ifndef SYNCHDB_CONNECTOR_IDENTITY_H
#define SYNCHDB_CONNECTOR_IDENTITY_H

#include "postgres.h"
#include "utils/uuid.h"

typedef struct SynchdbConnectorKey
{
	Oid database_oid;
	pg_uuid_t connector_id;
} SynchdbConnectorKey;

static inline bool
synchdb_connector_key_is_valid(const SynchdbConnectorKey *key)
{
	static const unsigned char zero[UUID_LEN] = {0};

	return key != NULL && OidIsValid(key->database_oid) &&
		memcmp(key->connector_id.data, zero, UUID_LEN) != 0;
}

/* Compare the represented fields, never struct padding. Invalid keys are not identities. */
static inline bool
synchdb_connector_key_equal(const SynchdbConnectorKey *left,
						  const SynchdbConnectorKey *right)
{
	return synchdb_connector_key_is_valid(left) && synchdb_connector_key_is_valid(right) &&
		left->database_oid == right->database_oid &&
		memcmp(left->connector_id.data, right->connector_id.data, UUID_LEN) == 0;
}

#endif /* SYNCHDB_CONNECTOR_IDENTITY_H */
