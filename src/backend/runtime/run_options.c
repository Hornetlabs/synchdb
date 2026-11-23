/* A portable, versioned options snapshot; credentials never enter control rows. */
#include "postgres.h"
#include "runtime/controller.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/json.h"

typedef enum OptionKind { OPTION_INT, OPTION_BOOL, OPTION_STRING } OptionKind;
typedef struct OptionField
{
    const char *name;
    Size offset;
    OptionKind kind;
} OptionField;

#define INT_OPTION(f) {#f, offsetof(SynchdbRunOptions, f), OPTION_INT}
#define BOOL_OPTION(f) {#f, offsetof(SynchdbRunOptions, f), OPTION_BOOL}
#define TEXT_OPTION(f) {#f, offsetof(SynchdbRunOptions, f), OPTION_STRING}
static const OptionField fields[] = {
    INT_OPTION(dbz_batch_size), INT_OPTION(dbz_queue_size),
    INT_OPTION(dbz_connect_timeout_ms), INT_OPTION(dbz_query_timeout_ms),
    TEXT_OPTION(dbz_skipped_operations), INT_OPTION(dbz_snapshot_thread_num),
    INT_OPTION(dbz_snapshot_fetch_size), INT_OPTION(dbz_snapshot_min_row_to_stream_results),
    INT_OPTION(dbz_incremental_snapshot_chunk_size),
    TEXT_OPTION(dbz_incremental_snapshot_watermarking_strategy),
    INT_OPTION(dbz_offset_flush_interval_ms), BOOL_OPTION(dbz_capture_only_selected_table_ddl),
    INT_OPTION(dbz_log_level), INT_OPTION(dbz_logminer_stream_mode),
    INT_OPTION(cdc_start_delay_ms), INT_OPTION(jvm_max_heap_size),
    INT_OPTION(jvm_max_direct_buffer_size), BOOL_OPTION(synchdb_fdw_use_subtx),
    INT_OPTION(synchdb_letter_casing_strategy)
};

Jsonb *
synchdb_run_options_to_json(const SynchdbRunOptions *options)
{
    StringInfoData out;
    int i;

    initStringInfo(&out);
    appendStringInfo(&out, "{\"version\":%u", options->version);
    for (i = 0; i < lengthof(fields); ++i)
    {
        const OptionField *field = &fields[i];
        const char *value = (const char *) options + field->offset;

        appendStringInfo(&out, ",\"%s\":", field->name);
        if (field->kind == OPTION_INT)
            appendStringInfo(&out, "%d", *(const int *) value);
        else if (field->kind == OPTION_BOOL)
            appendStringInfoString(&out, *(const bool *) value ? "true" : "false");
        else
            escape_json(&out, value);
    }
    appendStringInfoChar(&out, '}');
    return DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(out.data)));
}

static JsonbValue *
option_value(Jsonb *json, const char *name)
{
    JsonbValue key;
    JsonbValue *value;

    key.type = jbvString;
    key.val.string.val = (char *) name;
    key.val.string.len = strlen(name);
    value = findJsonbValueFromContainer(&json->root, JB_FOBJECT, &key);
    if (!value)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("missing persisted run option \"%s\"", name)));
    return value;
}

void
synchdb_run_options_from_json(Jsonb *json, SynchdbRunOptions *options)
{
    JsonbValue *version;
    int i;

    if (!JB_ROOT_IS_OBJECT(json) || JB_ROOT_COUNT(json) != lengthof(fields) + 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid persisted run options object")));
    MemSet(options, 0, sizeof(*options));
    version = option_value(json, "version");
    if (version->type != jbvNumeric ||
        DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(version->val.numeric))) !=
            SYNCHDB_RUN_OPTIONS_VERSION)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("unsupported persisted run options version")));
    options->version = SYNCHDB_RUN_OPTIONS_VERSION;
    for (i = 0; i < lengthof(fields); ++i)
    {
        const OptionField *field = &fields[i];
        JsonbValue *value = option_value(json, field->name);
        char *destination = (char *) options + field->offset;

        if (field->kind == OPTION_INT && value->type == jbvNumeric)
            *(int *) destination = DatumGetInt32(DirectFunctionCall1(numeric_int4,
                                                NumericGetDatum(value->val.numeric)));
        else if (field->kind == OPTION_BOOL && value->type == jbvBool)
            *(bool *) destination = value->val.boolean;
        else if (field->kind == OPTION_STRING && value->type == jbvString &&
                 value->val.string.len < SYNCHDB_RUN_OPTION_TEXT_SIZE)
        {
            memcpy(destination, value->val.string.val, value->val.string.len);
            destination[value->val.string.len] = '\0';
        }
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("invalid persisted run option \"%s\"", field->name)));
    }
}
