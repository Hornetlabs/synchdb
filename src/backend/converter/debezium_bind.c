/*
 * Bind source rows without retaining relation or source-schema metadata across
 * events. Only target Datums leave this module; the legacy column codec's
 * metadata is local to the binding operation.
 */
#include "postgres.h"

#include "access/table.h"
#include "access/sysattr.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type_d.h"
#include "commands/dbcommands.h"
#include "converter/debezium_bind.h"
#include "converter/debezium_event_handler.h"
#include "converter/format_converter.h"
#include "converter/rule_snapshot.h"
#include "executor/row_identity.h"
#include "executor/target_validate.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "runtime/config.h"
#include "source/debezium_schema.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgrprotos.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

typedef struct BindAttributeEntry
{
	char name[NAMEDATALEN];
	AttrNumber position;
} BindAttributeEntry;

typedef struct BindAttributeLookup
{
	HTAB *index;
	int linear_lookups;
} BindAttributeLookup;

typedef struct BindColumn
{
	DBZ_DML_COLUMN_VALUE value; /* first: shared with the scalar codec */
	const SynchdbDbzField *array;
} BindColumn;

static DbzType
bind_wire_type(const char *name)
{
	static const struct
	{
		const char *name;
		DbzType type;
	} types[] = {
		{"float32", DBZTYPE_FLOAT32}, {"float64", DBZTYPE_FLOAT64},
		{"float", DBZTYPE_FLOAT}, {"double", DBZTYPE_DOUBLE},
		{"bytes", DBZTYPE_BYTES}, {"int8", DBZTYPE_INT8},
		{"int16", DBZTYPE_INT16}, {"int32", DBZTYPE_INT32},
		{"int64", DBZTYPE_INT64}, {"struct", DBZTYPE_STRUCT},
		{"string", DBZTYPE_STRING}
	};
	size_t index;

	for (index = 0; index < lengthof(types); index++)
		if (strcmp(name, types[index].name) == 0)
			return types[index].type;
	return DBZTYPE_UNDEF;
}

static TimeRep
bind_logical_type(const char *name)
{
	static const struct
	{
		const char *name;
		TimeRep type;
	} types[] = {
		{"io.debezium.time.Date", TIME_DATE},
		{"io.debezium.time.Time", TIME_TIME},
		{"io.debezium.time.ZonedTime", TIME_ZONEDTIME},
		{"io.debezium.time.MicroTime", TIME_MICROTIME},
		{"io.debezium.time.NanoTime", TIME_NANOTIME},
		{"io.debezium.time.Timestamp", TIME_TIMESTAMP},
		{"io.debezium.time.MicroTimestamp", TIME_MICROTIMESTAMP},
		{"io.debezium.time.NanoTimestamp", TIME_NANOTIMESTAMP},
		{"io.debezium.time.ZonedTimestamp", TIME_ZONEDTIMESTAMP},
		{"io.debezium.time.MicroDuration", TIME_MICRODURATION},
		{"io.debezium.data.VariableScaleDecimal", DATA_VARIABLE_SCALE},
		{"org.apache.kafka.connect.data.Decimal", DATA_VARIABLE_SCALE},
		{"io.debezium.data.geometry.Geometry", DATA_GEOMETRY},
		{"io.debezium.data.geometry.Geography", DATA_GEOMETRY},
		{"io.debezium.data.Enum", DATA_ENUM},
		{"io.debezium.data.Uuid", DATA_UUID},
		{"io.debezium.data.Json", DATA_JSON},
		{"io.debezium.data.Xml", DATA_XML},
		{"io.debezium.data.Bits", DATA_BITS},
		{"io.debezium.data.geometry.Point", DATA_POINT}
	};
	size_t index;

	if (name == NULL)
		return TIME_UNDEF;
	for (index = 0; index < lengthof(types); index++)
		if (strcmp(name, types[index].name) == 0)
			return types[index].type;
	return TIME_UNDEF;
}

static int
bind_scale(const SynchdbDbzField *field)
{
	JsonbValue buffer;
	JsonbValue *scale;
	char *text;
	int result;

	if (field->parameters == NULL)
		return 0;
	scale = getKeyJsonValueFromContainer(&field->parameters->root,
										   "scale", 5, &buffer);
	if (scale == NULL || scale->type == jbvNull)
		return 0;
	if (scale->type != jbvString)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("Debezium scale parameter for column \"%s\" must be a string",
						field->name)));
	text = pnstrdup(scale->val.string.val, scale->val.string.len);
	result = pg_strtoint32(text);
	pfree(text);
	return result;
}

static char *
bind_value(const SynchdbDbzField *field)
{
	const JsonbValue *raw = &field->value.raw;

	switch (field->value.kind)
	{
		case SYNCHDB_DBZ_VALUE_NULL:
			return NULL;
		case SYNCHDB_DBZ_VALUE_STRING:
			return pnstrdup(raw->val.string.val, raw->val.string.len);
		case SYNCHDB_DBZ_VALUE_NUMBER:
			return DatumGetCString(DirectFunctionCall1(numeric_out,
														 PointerGetDatum(raw->val.numeric)));
		case SYNCHDB_DBZ_VALUE_BOOLEAN:
			return pstrdup(raw->val.boolean ? "true" : "false");
		case SYNCHDB_DBZ_VALUE_ARRAY:
		case SYNCHDB_DBZ_VALUE_OBJECT:
			return JsonbToCString(NULL, raw->val.binary.data,
								  raw->val.binary.len);
		case SYNCHDB_DBZ_VALUE_MISSING:
		case SYNCHDB_DBZ_VALUE_UNAVAILABLE:
			break;
	}
	elog(ERROR, "cannot bind a missing or unavailable Debezium field value");
	return NULL;
}

/* Array input has its own quoting and NULL rules. Convert each item using
 * its declared source schema, then let the final target input enforce shape,
 * element typmods and domains. User expressions run once on the whole column. */
static char *
bind_array_text(const SynchdbDbzField *field, Oid element_type, int32 typmod,
				ConnectorType type)
{
	JsonbContainer *array = field->value.raw.val.binary.data;
	JsonbIterator *iterator = JsonbIteratorInit(array);
	SynchdbDbzField *item = synchdb_dbz_array_element_field(field);
	JsonbValue raw;
	JsonbIteratorToken token;
	StringInfoData output;
	char delimiter;
	char alignment;
	int16 length;
	bool by_value;
	Oid input_parameter;
	Oid input_function;
	bool first = true;

	check_stack_depth();
	get_type_io_data(element_type, IOFunc_input, &length, &by_value,
					 &alignment, &delimiter, &input_parameter, &input_function);
	initStringInfo(&output);
	appendStringInfoChar(&output, '{');
	while ((token = JsonbIteratorNext(&iterator, &raw, true)) != WJB_DONE)
	{
		DBZ_DML_COLUMN_VALUE scalar = {0};
		char *converted;
		char *at;

		if (token != WJB_ELEM)
			continue;
		CHECK_FOR_INTERRUPTS();
		if (!first)
			appendStringInfoChar(&output, delimiter);
		first = false;
		synchdb_dbz_decode_element_value(item, &raw);
		if (item->value.kind == SYNCHDB_DBZ_VALUE_NULL)
		{
			appendStringInfoString(&output, "NULL");
			continue;
		}
		if (item->value.kind == SYNCHDB_DBZ_VALUE_ARRAY)
		{
			converted = bind_array_text(item, element_type, typmod, type);
			appendStringInfoString(&output, converted);
			pfree(converted);
			continue;
		}
		scalar.name = item->name;
		scalar.datatype = element_type;
		scalar.typemod = typmod;
		scalar.typname = format_type_be(element_type);
		get_type_category_preferred(element_type, &scalar.typcategory,
								   &scalar.typispreferred);
		scalar.dbztype = bind_wire_type(item->wire_type);
		scalar.timerep = bind_logical_type(item->logical_name);
		scalar.scale = bind_scale(item);
		scalar.value = bind_value(item);
		converted = fc_convert_value_text(&scalar, type);
		appendStringInfoChar(&output, '"');
		for (at = converted; *at; at++)
		{
			if (*at == '"' || *at == '\\')
				appendStringInfoChar(&output, '\\');
			appendStringInfoChar(&output, *at);
		}
		appendStringInfoChar(&output, '"');
		pfree(converted);
		pfree(scalar.value);
		pfree(scalar.typname);
	}
	appendStringInfoChar(&output, '}');
	return output.data;
}

/* Dense wide rows otherwise rescan the descriptor for each image field. This
 * index lives only for one binding: DDL, permissions and values remain fresh. */
static HTAB *
bind_attribute_index(Relation relation)
{
	TupleDesc descriptor = RelationGetDescr(relation);
	HASHCTL control = {0};
	HTAB *index;
	int position;
	control.keysize = NAMEDATALEN;
	control.entrysize = sizeof(BindAttributeEntry);
	control.hcxt = CurrentMemoryContext;
	index = hash_create("Debezium bound target attributes", descriptor->natts,
						&control, HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
	for (position = 0; position < descriptor->natts; position++)
	{
		Form_pg_attribute attribute = TupleDescAttr(descriptor, position);
		BindAttributeEntry *entry;

		if (attribute->attisdropped)
			continue;
		entry = hash_search(index, NameStr(attribute->attname), HASH_ENTER, NULL);
		entry->position = attribute->attnum;
	}
	return index;
}

static Form_pg_attribute
bind_attribute(Relation relation, BindAttributeLookup *attributes,
			   const char *name)
{
	TupleDesc descriptor = RelationGetDescr(relation);
	int index;

	if (strlen(name) >= NAMEDATALEN)
		ereport(ERROR,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("target column name exceeds PostgreSQL's identifier limit")));
	/* Count only actual lookups in the existing binding loop. Sparse images
	 * never incur a separate schema walk or allocate an unused index. */
	if (attributes->index == NULL && descriptor->natts >= 64 &&
		++attributes->linear_lookups > 32)
		attributes->index = bind_attribute_index(relation);
	if (attributes->index != NULL)
	{
		BindAttributeEntry *entry = hash_search(attributes->index, name, HASH_FIND, NULL);

		if (entry != NULL)
			return TupleDescAttr(descriptor, entry->position - 1);
	}
	else
	{
		for (index = 0; index < descriptor->natts; index++)
		{
			Form_pg_attribute attribute = TupleDescAttr(descriptor, index);

			if (strcmp(NameStr(attribute->attname), name) == 0)
			{
				if (attribute->attisdropped)
					break;
				return attribute;
			}
		}
	}
	ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_COLUMN),
			 errmsg("target column \"%s\" does not exist", name)));
	return NULL;
}

static List *
bind_image(const SynchdbDbzImage *image, const char *remote_object_id,
		   Relation relation, const Bitmapset *keys,
		   BindAttributeLookup *attributes)
{
	List *bound = NIL;
	ListCell *cell;
	Bitmapset *seen = NULL;

	foreach(cell, image->fields)
	{
		const SynchdbDbzField *field = lfirst(cell);
		DBZ_DML_COLUMN_VALUE *value;
		BindColumn *column;
		char *qualified;
		char *mapped;
		Form_pg_attribute attribute;

		if (field->value.kind == SYNCHDB_DBZ_VALUE_MISSING)
			continue;
		if (field->value.kind == SYNCHDB_DBZ_VALUE_UNAVAILABLE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unavailable source value for column \"%s\" is not supported",
							field->name)));
		column = palloc0(sizeof(*column));
		value = &column->value;
		value->remoteColumnName = pstrdup(field->name);
		value->name = pstrdup(field->name);
		fc_normalize_name(synchdb_letter_casing_strategy, value->name,
						  strlen(value->name));
		qualified = synchdb_source_column_key(remote_object_id, field->name);
		mapped = transform_object_name(qualified, "column");
		pfree(qualified);
		if (mapped != NULL)
		{
			pfree(value->name);
			value->name = mapped;
		}
		attribute = bind_attribute(relation, attributes, value->name);
		if (bms_is_member(attribute->attnum, seen))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_COLUMN),
					 errmsg("multiple source columns map to target column \"%s\" in one row image",
							value->name)));
		seen = bms_add_member(seen, attribute->attnum);
		if (attribute->attgenerated != '\0' ||
			attribute->attidentity == ATTRIBUTE_IDENTITY_ALWAYS)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("explicit source value for generated or identity-always target column \"%s\" is not supported",
							value->name)));
		value->datatype = attribute->atttypid;
		value->position = attribute->attnum;
		value->typemod = attribute->atttypmod;
		value->ispk = bms_is_member(attribute->attnum - FirstLowInvalidHeapAttributeNumber,
									  keys);
		get_type_category_preferred(value->datatype, &value->typcategory,
										&value->typispreferred);
		value->dbztype = bind_wire_type(field->wire_type);
		value->timerep = bind_logical_type(field->logical_name);
		value->scale = bind_scale(field);
		value->is_null = field->value.kind == SYNCHDB_DBZ_VALUE_NULL;
		if (!value->is_null && OidIsValid(get_element_type(getBaseType(value->datatype))))
		{
			if (field->value.kind != SYNCHDB_DBZ_VALUE_ARRAY ||
				strcmp(field->wire_type, "array") != 0)
				ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
								errmsg("target array column \"%s\" requires a source array", value->name)));
			column->array = field;
		}
		else if (!value->is_null && field->value.kind == SYNCHDB_DBZ_VALUE_ARRAY &&
			value->datatype != JSONOID && value->datatype != JSONBOID)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("source array conversion for target column \"%s\" is not supported",
							value->name)));
		value->value = bind_value(field);
		bound = lappend(bound, value);
	}
	bms_free(seen);
	list_sort(bound, list_sort_cmp);
	return bound;
}

static void
bind_require_keys(List *image, const Bitmapset *keys)
{
	Bitmapset *present = NULL;
	ListCell *cell;

	foreach(cell, image)
	{
		DBZ_DML_COLUMN_VALUE *value = lfirst(cell);
		int member = value->position - FirstLowInvalidHeapAttributeNumber;

		if (bms_is_member(member, keys) && !value->is_null)
			present = bms_add_member(present, member);
	}
	if (!bms_equal(present, keys))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("source row image must contain every target identity column with a nonnull value")));
	bms_free(present);
}

static List *
bind_metadata_image(List *columns, const Bitmapset *identity_attributes)
{
	List *metadata = NIL;
	ListCell *cell;

	foreach(cell, columns)
	{
		DBZ_DML_COLUMN_VALUE *source = lfirst(cell);
		SynchdbTargetColumn *column;

		if (identity_attributes != NULL &&
			!bms_is_member(source->position - FirstLowInvalidHeapAttributeNumber,
						   identity_attributes))
			continue;
		column = palloc0(sizeof(*column));
		/* This short-lived permission preflight borrows the mapped name and
		 * deliberately has no evaluated Datum. It never reaches a Writer. */
		column->name = source->name;
		column->datatype = source->datatype;
		column->typmod = source->typemod;
		column->typbyval = get_typbyval(source->datatype);
		column->position = source->position;
		column->is_null = source->is_null;
		metadata = lappend(metadata, column);
	}
	return metadata;
}

static void
bind_validate_permissions(Relation relation, const SynchdbTargetChange *change,
						 List *before, List *after,
						 const SynchdbRowIdentity *identity,
						 bool copy_after_identity)
{
	SynchdbTargetChange metadata = *change;

	metadata.columnValuesAfter = bind_metadata_image(after, NULL);
	metadata.columnValuesBefore = identity == NULL ? NIL :
		bind_metadata_image(copy_after_identity ? after : before,
							identity->attributes);
	synchdb_target_validate_binding(relation, &metadata);
	/* Only the small wrappers are owned here; source names remain in use. */
	list_free_deep(metadata.columnValuesBefore);
	list_free_deep(metadata.columnValuesAfter);
}

static List *
bind_convert_image(List *columns, const char *remote_object_id,
				   Relation relation, ConnectorType type,
				   const Bitmapset *identity_attributes)
{
	List *converted = NIL;
	ListCell *cell;

	foreach(cell, columns)
	{
		DBZ_DML_COLUMN_VALUE *column = lfirst(cell);
		SynchdbTargetColumn *target;

		if (identity_attributes != NULL &&
			!bms_is_member(column->position - FirstLowInvalidHeapAttributeNumber,
						   identity_attributes))
			continue;
		/* Before nonkeys never reach the codec, and NULL bypasses it. Do not
		 * format a type name which neither path will consume. */
		if (!column->is_null)
			column->typname = format_type_be(column->datatype);
		if (((BindColumn *) column)->array != NULL)
		{
			int32 typmod = column->typemod;
			Oid base = getBaseTypeAndTypmod(column->datatype, &typmod);
			char *input = bind_array_text(((BindColumn *) column)->array,
										 get_element_type(base), typmod, type);

			pfree(column->value);
			column->value = input;
			column->dbztype = DBZTYPE_ARRAY_TEXT;
		}
		target = fc_convert_target_column(column, remote_object_id, relation, type);
		if (identity_attributes != NULL && target->is_null)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("target identity transformation returned NULL")));
		converted = lappend(converted, target);
	}
	return converted;
}

/* An unchanged source key must use the same evaluated value for lookup and
 * assignment. Copy the owned Datum instead of executing its expression twice. */
static List *
bind_copy_identity(List *after, const Bitmapset *identity_attributes)
{
	List *before = NIL;
	ListCell *cell;

	foreach(cell, after)
	{
		SynchdbTargetColumn *column = lfirst(cell);
		SynchdbTargetColumn *copy;

		if (!bms_is_member(column->position - FirstLowInvalidHeapAttributeNumber,
						   identity_attributes))
			continue;
		if (column->is_null)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("target identity transformation returned NULL")));
		copy = palloc(sizeof(*copy));
		*copy = *column;
		copy->name = pstrdup(column->name);
		copy->value = datumCopy(column->value, column->typbyval,
							   get_typlen(column->datatype));
		before = lappend(before, copy);
	}
	return before;
}

static void
bind_free_image(List *columns)
{
	ListCell *cell;

	foreach(cell, columns)
	{
		DBZ_DML_COLUMN_VALUE *column = lfirst(cell);

		pfree(column->name);
		pfree(column->remoteColumnName);
		if (column->value != NULL)
			pfree(column->value);
		if (column->typname != NULL)
			pfree(column->typname);
		pfree(column);
	}
	list_free(columns);
}

Relation
synchdb_bind_source_table(const char *database, const char *source_schema,
						  const char *source_table, LOCKMODE lockmode)
{
	char *remote_object_id;
	char *schema;
	char *table;
	char *mapped;
	Relation relation;

	if (database == NULL || database[0] == '\0' ||
		source_table == NULL || source_table[0] == '\0')
		elog(ERROR, "target binding requires a complete source table identity");
	remote_object_id = synchdb_source_table_key(database, source_schema, source_table);
	mapped = transform_object_name(remote_object_id, "table");
	if (mapped != NULL)
	{
		char *mapped_database;

		synchdb_rule_parse_target_name(mapped, &mapped_database, &schema, &table);
		if (mapped_database != NULL)
		{
			char *current = get_database_name(MyDatabaseId);

			if (current == NULL || strcmp(mapped_database, current) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cross-database target table mappings are not supported")));
			pfree(mapped_database);
			pfree(current);
		}
		pfree(mapped);
	}
	else
	{
		schema = pstrdup(database);
		table = pstrdup(source_table);
		fc_normalize_name(synchdb_letter_casing_strategy, schema, strlen(schema));
		fc_normalize_name(synchdb_letter_casing_strategy, table, strlen(table));
	}
	if (strlen(schema) >= NAMEDATALEN || strlen(table) >= NAMEDATALEN)
		ereport(ERROR,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("target schema or table name exceeds PostgreSQL's identifier limit")));
	relation = table_openrv(makeRangeVar(schema, table, -1), lockmode);
	synchdb_target_validate_relation(relation);
	pfree(remote_object_id);
	pfree(schema);
	pfree(table);
	return relation;
}

SynchdbTargetChange *
synchdb_bind_dbz_row(const SynchdbDbzRow *row, ConnectorType type)
{
	SynchdbTargetChange *bound = palloc0(sizeof(*bound));
	char *remote_object_id;
	Relation relation;
	Bitmapset *keys;
	BindAttributeLookup attributes = {0};
	SynchdbRowIdentity *identity = NULL;
	List *before = NIL;
	List *after = NIL;
	bool copy_after_identity = false;

	if (row == NULL)
		elog(ERROR, "target binding requires a decoded Debezium source row");
	bound->op = row->op;
	remote_object_id = synchdb_source_table_key(row->db, row->schema, row->table);
	relation = synchdb_bind_source_table(row->db, row->schema, row->table,
										RowExclusiveLock);
	bound->tableoid = RelationGetRelid(relation);
	bound->natts = RelationGetDescr(relation)->natts;
	keys = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_PRIMARY_KEY);
	if (row->op == 'u' || row->op == 'd')
	{
		identity = synchdb_resolve_row_identity(relation);
		bound->identity_index_oid = identity->index_oid;
	}
	if (row->op == 'c' || row->op == 'r' || row->op == 'u')
	{
		if (row->after.state != SYNCHDB_DBZ_IMAGE_OBJECT)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("insert, read and update require an object after image")));
		after = bind_image(&row->after, remote_object_id, relation, keys, &attributes);
	}
	if (row->op == 'd' || (row->op == 'u' &&
							 row->before.state == SYNCHDB_DBZ_IMAGE_OBJECT))
	{
		if (row->before.state != SYNCHDB_DBZ_IMAGE_OBJECT)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("delete requires an object before image")));
		before = bind_image(&row->before, remote_object_id, relation, keys, &attributes);
		bind_require_keys(before, identity->attributes);
	}
	else if (row->op == 'u')
	{
		if (row->before.state != SYNCHDB_DBZ_IMAGE_NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("update requires an object or explicit null before image")));
		/* A null PostgreSQL before image historically denotes an unchanged
		 * source key. Do not extend that assumption to another connector or
		 * to a target identity outside the existing primary-key column set. */
		if (type != TYPE_POSTGRES || !identity->primary_equivalent)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("update without a before image requires a PostgreSQL primary-equivalent identity")));
		bind_require_keys(after, identity->attributes);
		copy_after_identity = true;
	}
	else if (row->op != 'c' && row->op != 'r')
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unsupported source row operation '%c'", row->op)));
	/* Bind both images before evaluating any user expression. Unused before
	 * fields remain validated source/metadata, never expression inputs. */
	bind_validate_permissions(relation, bound, before, after,
							  identity, copy_after_identity);
	if (attributes.index != NULL)
		hash_destroy(attributes.index);
	bound->columnValuesAfter = bind_convert_image(after, remote_object_id,
												 relation, type, NULL);
	if (identity != NULL)
	{
		if (copy_after_identity)
			bound->columnValuesBefore = bind_copy_identity(bound->columnValuesAfter,
														 identity->attributes);
		else
			bound->columnValuesBefore = bind_convert_image(before, remote_object_id,
														   relation, type, identity->attributes);
	}
	synchdb_target_validate_change(relation, bound);
	bind_free_image(before);
	bind_free_image(after);
	bms_free(keys);
	pfree(remote_object_id);
	/* Keep the lock while the batch applies changes, including later events. */
	table_close(relation, NoLock);
	return bound;
}
