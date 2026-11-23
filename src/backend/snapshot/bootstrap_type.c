/* FDW schema planning uses the same immutable rules and type renderer as DDL. */
#include "postgres.h"
#include "fmgr.h"
#include <limits.h>

#include "access/xact.h"
#include "converter/ddl_plan.h"
#include "converter/rule_snapshot.h"
#include "source/debezium_schema.h"
#include "synchdb/synchdb.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/snapmgr.h"

PG_FUNCTION_INFO_V1(synchdb_bootstrap_type);
Datum synchdb_bootstrap_type(PG_FUNCTION_ARGS);

static JsonbValue *
bootstrap_type_member(JsonbContainer *object, const char *key, JsonbValue *value)
{
    return getKeyJsonValueFromContainer(object, key, strlen(key), value);
}

static char *
bootstrap_type_string(JsonbContainer *object, const char *key, Size maximum)
{
    JsonbValue value;
    JsonbValue *found = bootstrap_type_member(object, key, &value);

    if (found == NULL || found->type != jbvString ||
        found->val.string.len <= 0 || (Size) found->val.string.len > maximum ||
        memchr(found->val.string.val, '\0', found->val.string.len) != NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid bootstrap type metadata: %s", key)));
    return pnstrdup(found->val.string.val, found->val.string.len);
}

static bool
bootstrap_type_integer(JsonbContainer *object, const char *key, int32 *result)
{
    JsonbValue value;
    JsonbValue *found = bootstrap_type_member(object, key, &value);
    Datum converted;
    Datum roundtrip;

    if (found == NULL || found->type == jbvNull)
        return false;
    if (found->type != jbvNumeric)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("bootstrap type metadata %s must be an integer", key)));
    converted = DirectFunctionCall1(numeric_int4, NumericGetDatum(found->val.numeric));
    roundtrip = DirectFunctionCall1(int4_numeric, converted);
    if (!DatumGetBool(DirectFunctionCall2(numeric_eq,
                                        NumericGetDatum(found->val.numeric), roundtrip)))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("bootstrap type metadata %s must be an exact integer", key)));
    *result = DatumGetInt32(converted);
    return true;
}

static void
bootstrap_type_source(Jsonb *metadata, SynchdbDbzSchemaColumn *column)
{
    JsonbValue value;
    JsonbValue *found;

    if (!JB_ROOT_IS_OBJECT(metadata))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("bootstrap column metadata must be an object")));
    column->name = bootstrap_type_string(&metadata->root, "name", SYNCHDB_SOURCE_ID_MAX_BYTES);
    column->type_name = bootstrap_type_string(&metadata->root, "type_name", SYNCHDB_SOURCE_ID_MAX_BYTES);
    column->has_length = bootstrap_type_integer(&metadata->root, "length", &column->length);
    column->has_scale = bootstrap_type_integer(&metadata->root, "scale", &column->scale);
    found = bootstrap_type_member(&metadata->root, "auto_incremented", &value);
    if (found && found->type != jbvBool)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("bootstrap auto_incremented metadata must be a boolean")));
    if (found && found->val.boolean)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("FDW bootstrap does not establish source sequence state")));
    /* FDW's common metadata has no verified auto-increment discriminator. */
    column->auto_incremented = false;
}

static ObjectMap *
bootstrap_type_rules(Jsonb *document, int *count)
{
    JsonbIterator *iterator;
    JsonbIteratorToken token;
    JsonbValue value;
    ObjectMap *rules;
    uint32 size;
    int position = 0;

    if (!JB_ROOT_IS_ARRAY(document) || JB_ROOT_IS_SCALAR(document))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("bootstrap frozen rules must be an array")));
    size = JB_ROOT_COUNT(document);
    if (size > INT_MAX || size > MaxAllocSize / sizeof(ObjectMap))
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("too many bootstrap mapping rules")));
    rules = palloc0(sizeof(ObjectMap) * Max(size, 1));
    iterator = JsonbIteratorInit(&document->root);
    while ((token = JsonbIteratorNext(&iterator, &value, true)) != WJB_DONE)
    {
        ObjectMap *rule;
        char *kind;
        char *destination;
        char *suffix;

        if (token != WJB_ELEM)
            continue;
        if (value.type != jbvBinary || !JsonContainerIsObject(value.val.binary.data) ||
            position >= (int) size)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("bootstrap mapping rule must be an object")));
        rule = &rules[position++];
        kind = bootstrap_type_string(value.val.binary.data, "kind", sizeof(rule->objtype) - 1);
        if (strcmp(kind, "table") != 0 && strcmp(kind, "column") != 0 &&
            strcmp(kind, "datatype") != 0 && strcmp(kind, "transform") != 0)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("unknown bootstrap mapping rule kind")));
        rule->srcobj = bootstrap_type_string(value.val.binary.data, "source", SYNCHDB_SOURCE_ID_MAX_BYTES);
        destination = bootstrap_type_string(value.val.binary.data, "destination", sizeof(rule->dstobj) - 1);
        memcpy(rule->objtype, kind, strlen(kind) + 1);
        memcpy(rule->dstobj, destination, strlen(destination) + 1);
        rule->enabled = true;
        suffix = strchr(rule->srcobj, '|');
        if (strcmp(kind, "datatype") == 0 && suffix && pg_strcasecmp(suffix + 1, "true") == 0)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("FDW bootstrap cannot apply auto-increment datatype rules without verified source sequence metadata")));
    }
    *count = position;
    return rules;
}

Datum
synchdb_bootstrap_type(PG_FUNCTION_ARGS)
{
    MemoryContext caller = CurrentMemoryContext;
    MemoryContext scratch;
    text *volatile result = NULL;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("bootstrap type planning requires provider, source identity, column and frozen rules")));
    if (!IsTransactionState() || !ActiveSnapshotSet())
        ereport(ERROR,
                (errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
                 errmsg("bootstrap type planning requires a caller-owned transaction and snapshot")));
    scratch = AllocSetContextCreate(caller, "synchdb bootstrap type planning", ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(scratch);
    PG_TRY();
    {
        char *provider = text_to_cstring(PG_GETARG_TEXT_PP(0));
        char *source_table = text_to_cstring(PG_GETARG_TEXT_PP(1));
        ConnectorType type = stringToConnectorType(provider);
        SynchdbDbzSchemaColumn column = {0};
        SynchdbRuleSnapshot *snapshot;
        ObjectMap *rules;
        int count;
        char *name;
        char *column_key;

        if (type != TYPE_POSTGRES && type != TYPE_MYSQL && type != TYPE_ORACLE &&
            type != TYPE_OLR && type != TYPE_SQLSERVER)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("unsupported bootstrap type provider")));
        if (source_table[0] == '\0' || strlen(source_table) > SYNCHDB_SOURCE_ID_MAX_BYTES)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("invalid bootstrap source table identity")));
        bootstrap_type_source(PG_GETARG_JSONB_P(2), &column);
        column_key = synchdb_source_column_key(source_table, column.name);
        if (strlen(column_key) > SYNCHDB_SOURCE_ID_MAX_BYTES)
            ereport(ERROR,
                    (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                     errmsg("bootstrap source column identity exceeds its supported length")));
        rules = bootstrap_type_rules(PG_GETARG_JSONB_P(3), &count);
        snapshot = synchdb_rule_snapshot_build("fdw-bootstrap", type, rules, count, scratch);
        /* PostgreSQL's information_schema spells every array "ARRAY". The
         * prepared FT has already resolved its actual element type. Preserve
         * that verified PG input type when no explicit frozen rule overrides
         * the column/global ARRAY token; this is not a Debezium array codec. */
        if (type == TYPE_POSTGRES && pg_strcasecmp(column.type_name, "ARRAY") == 0 &&
            synchdb_rule_type(snapshot, type, column_key, column.type_name, false) == NULL)
            column.type_name = bootstrap_type_string(&PG_GETARG_JSONB_P(2)->root,
                                                     "physical_type", SYNCHDB_SOURCE_ID_MAX_BYTES);
        name = synchdb_ddl_render_column_type(source_table, &column, type, snapshot);
        if (pg_strcasecmp(name, "serial") == 0 || pg_strcasecmp(name, "serial4") == 0 ||
            pg_strcasecmp(name, "bigserial") == 0 || pg_strcasecmp(name, "serial8") == 0 ||
            pg_strcasecmp(name, "smallserial") == 0 || pg_strcasecmp(name, "serial2") == 0)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("FDW bootstrap does not establish serial sequence state")));
        MemoryContextSwitchTo(caller);
        result = cstring_to_text(name);
        MemoryContextSwitchTo(scratch);
        synchdb_rule_snapshot_destroy(snapshot);
    }
    PG_FINALLY();
    {
        MemoryContextSwitchTo(caller);
        MemoryContextDelete(scratch);
    }
    PG_END_TRY();
    PG_RETURN_TEXT_P(result);
}
