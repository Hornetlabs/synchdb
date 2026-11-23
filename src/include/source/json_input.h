/* Source protocols carry UTF8 JSON, independently of database encoding. */
#ifndef SYNCHDB_SOURCE_JSON_INPUT_H
#define SYNCHDB_SOURCE_JSON_INPUT_H

#include "postgres.h"
#include "utils/jsonb.h"

/* Caller owns the transaction and CurrentMemoryContext; input is wire UTF8. */
extern Jsonb *synchdb_source_json_from_utf8(const char *json);

#endif
