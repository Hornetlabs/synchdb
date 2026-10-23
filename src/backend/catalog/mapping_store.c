/* Object-mapping persistence and checked legacy target bindings. */
#include "postgres.h"

#include "catalog/catalog_access.h"
#include "catalog/mapping_store.h"
#include "common/keywords.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "storage/shmem.h"
#include "utils/builtins.h"

static const char *const mapping_names[] = {"table", "column", "datatype", "transform"};

static const char *
mapping_kind_name(SynchdbMappingKind kind)
{
	if (kind < SYNCHDB_MAPPING_TABLE || kind > SYNCHDB_MAPPING_TRANSFORM)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("unsupported object mapping kind")));
	return mapping_names[kind];
}

/* Source IDs remain opaque and case sensitive.  Match the old space trimming. */
static char *
mapping_trim(const char *value)
{
	const char *end;

	if (!value)
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				errmsg("object mapping value must not be NULL")));
	while (*value == ' ')
		value++;
	end = value + strlen(value);
	while (end > value && end[-1] == ' ')
		end--;
	if (end == value)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("object mapping value must not be empty")));
	return pnstrdup(value, end - value);
}

SynchdbMappingKind
synchdb_mapping_kind(const char *value)
{
	char *normalized = mapping_trim(value);
	int i;

	for (i = 0; i < (int) lengthof(mapping_names); i++)
	{
		if (pg_strcasecmp(normalized, mapping_names[i]) == 0)
		{
			pfree(normalized);
			return (SynchdbMappingKind) i;
		}
	}
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("unsupported object mapping kind \"%s\"", normalized)));
	pg_unreachable();
}

static void
mapping_check_length(const char *value, Size capacity, const char *field)
{
	if (strlen(value) >= capacity)
		ereport(ERROR, (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				errmsg("object mapping field \"%s\" exceeds its supported length", field),
				errdetail("The current representation supports at most %zu bytes.", capacity - 1)));
}

static void
mapping_check_source(SynchdbMappingKind kind, const char *source)
{
	const char *separator;
	Size token_length;

	if (!source || source[0] == '\0')
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("object mapping source must not be empty")));
	mapping_check_length(source, SYNCHDB_SOURCE_ID_SIZE, "source");
	if (kind != SYNCHDB_MAPPING_DATATYPE)
		return;
	separator = strchr(source, '|');
	token_length = separator ? (Size) (separator - source) : strlen(source);
	/* Global datatype tokens still use the legacy fixed-width type registry. */
	if (!memchr(source, '.', token_length) && token_length >= SYNCHDB_DATATYPE_NAME_SIZE)
		ereport(ERROR, (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				errmsg("global datatype mapping source exceeds its supported length"),
				errdetail("A global datatype token supports at most %d bytes.",
						  SYNCHDB_DATATYPE_NAME_SIZE - 1)));
}

/* Validate the token shape before the legacy loader's strtok/atoi parser. */
static void
mapping_check_datatype(const char *source, const char *destination)
{
	char *copy = pstrdup(destination);
	char *separator = strchr(copy, '|');
	const char *source_separator = strchr(source, '|');
	char *type;

	if (separator)
	{
		int32 length;

		*separator++ = '\0';
		if (strchr(separator, '|'))
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("datatype mapping has too many destination components")));
		if (*separator)
		{
			length = pg_strtoint32(separator);
			if (length < -1)
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("datatype mapping length must be -1 or nonnegative")));
		}
	}
	type = mapping_trim(copy);
	/* The loader copies the raw token, including any surrounding whitespace. */
	mapping_check_length(copy, SYNCHDB_DATATYPE_NAME_SIZE, "destination type");
	pfree(type);
	pfree(copy);
	if (source_separator)
	{
		if (source_separator == source || strchr(source_separator + 1, '|') ||
			(source_separator[1] &&
			 pg_strcasecmp(source_separator + 1, "true") != 0 &&
			 pg_strcasecmp(source_separator + 1, "false") != 0))
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("invalid datatype mapping source components")));
	}
}

/* Follow PostgreSQL's lexical/keyword rules without quote_all_identifiers. */
static bool
mapping_simple_identifier(const char *value)
{
	const char *p;
	int keyword;

	if (!((value[0] >= 'a' && value[0] <= 'z') || value[0] == '_'))
		return false;
	for (p = value; *p; p++)
		if (!((*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9') || *p == '_'))
			return false;
	keyword = ScanKeywordLookup(value, &ScanKeywords);
	return keyword < 0 || ScanKeywordCategories[keyword] == UNRESERVED_KEYWORD;
}

static void
mapping_check_target_name(SynchdbMappingKind kind, const char *value)
{
	char *copy = pstrdup(value);
	char *separator = strchr(copy, '.');
	const char *components[2] = {copy, NULL};
	int i;

	if (separator && kind == SYNCHDB_MAPPING_TABLE)
	{
		*separator++ = '\0';
		components[1] = separator;
	}
	for (i = 0; i < (int) lengthof(components); i++)
	{
		const char *part = components[i];

		if (!part)
			continue;
		if (!mapping_simple_identifier(part))
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("legacy mapping targets require simple unquoted identifiers"),
					errdetail("A table target may be table or schema.table; a column target must be a single name.")));
		mapping_check_length(part, NAMEDATALEN, "target identifier");
	}
	pfree(copy);
}

static void
mapping_check_destination(SynchdbMappingKind kind, const char *source, const char *value)
{
	if (!value || value[0] == '\0')
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("object mapping destination must not be empty")));
	mapping_check_length(value, SYNCHDB_TRANSFORM_EXPRESSION_SIZE, "destination");
	if (kind == SYNCHDB_MAPPING_TABLE || kind == SYNCHDB_MAPPING_COLUMN)
	{
		mapping_check_length(value, SYNCHDB_OBJ_NAME_SIZE, "target name");
	}
	else if (kind == SYNCHDB_MAPPING_DATATYPE)
		mapping_check_datatype(source, value);
}

bool
synchdb_mapping_store_upsert(const char *connector, SynchdbMappingKind kind,
							 const char *source, const char *destination)
{
	Oid types[4] = {TEXTOID, TEXTOID, TEXTOID, TEXTOID};
	Datum values[4];
	char *normalized = mapping_trim(source);
	const char *kind_name = mapping_kind_name(kind);
	char *relation;
	char *query;
	bool affected;

	if (!connector || !connector[0])
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("connector name must not be empty")));
	mapping_check_source(kind, normalized);
	mapping_check_destination(kind, normalized, destination);
	synchdb_store_connect();
	relation = synchdb_store_relation(SYNCHDB_OBJECT_MAPPING_TABLE, RowExclusiveLock);
	query = psprintf("INSERT INTO %s (name,objtype,enabled,srcobj,dstobj) "
			"VALUES ($1::pg_catalog.name,$2::pg_catalog.name,true,$3,$4) "
			"ON CONFLICT (name,objtype,srcobj) DO UPDATE SET enabled=EXCLUDED.enabled,dstobj=EXCLUDED.dstobj", relation);
	mapping_check_length(connector, SYNCHDB_CONNINFO_NAME_SIZE, "connector");
	values[0] = CStringGetTextDatum(connector);
	values[1] = CStringGetTextDatum(kind_name);
	values[2] = CStringGetTextDatum(normalized);
	values[3] = CStringGetTextDatum(destination);
	if (SPI_execute_with_args(query, 4, types, values, NULL, false, 0) != SPI_OK_INSERT || SPI_processed > 1)
		elog(ERROR, "could not write object mapping");
	affected = SPI_processed == 1;
	synchdb_store_finish();
	pfree(normalized);
	return affected;
}

bool
synchdb_mapping_store_disable(const char *connector, SynchdbMappingKind kind, const char *source)
{
	Oid types[3] = {TEXTOID, TEXTOID, TEXTOID};
	Datum values[3];
	char *normalized = mapping_trim(source);
	const char *kind_name = mapping_kind_name(kind);
	char *relation;
	char *query;
	bool affected;

	if (!connector || !connector[0])
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("connector name must not be empty")));
	mapping_check_length(connector, SYNCHDB_CONNINFO_NAME_SIZE, "connector");
	mapping_check_source(kind, normalized);
	synchdb_store_connect();
	relation = synchdb_store_relation(SYNCHDB_OBJECT_MAPPING_TABLE, RowExclusiveLock);
	query = psprintf("UPDATE %s SET enabled=false WHERE name::pg_catalog.text OPERATOR(pg_catalog.=) $1 "
			"AND objtype::pg_catalog.text OPERATOR(pg_catalog.=) $2 "
			"AND srcobj::pg_catalog.text COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.=) $3", relation);
	values[0] = CStringGetTextDatum(connector);
	values[1] = CStringGetTextDatum(kind_name);
	values[2] = CStringGetTextDatum(normalized);
	if (SPI_execute_with_args(query, 3, types, values, NULL, false, 0) != SPI_OK_UPDATE || SPI_processed > 1)
		elog(ERROR, "could not disable object mapping");
	affected = SPI_processed == 1;
	synchdb_store_finish();
	pfree(normalized);
	return affected;
}

static char *
mapping_text(HeapTuple tuple, TupleDesc desc, int column, bool required)
{
	bool isnull;
	Datum value;

	if (SPI_gettypeid(desc, column) != TEXTOID)
		ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("invalid object mapping catalog type")));
	value = SPI_getbinval(tuple, desc, column, &isnull);
	if (isnull)
	{
		if (required)
			ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("required object mapping field is NULL")));
		return NULL;
	}
	return TextDatumGetCString(value);
}

/* The legacy ALTER helpers cannot parse quoted identifiers safely. */
static void
mapping_legacy_identifier(const char *value)
{
	if (!mapping_simple_identifier(value))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("legacy mapping reload cannot bind a target requiring quoted identifiers"),
				errdetail("Typed target binding is required for this target.")));
}

static void
mapping_bind(ObjectMap *map, SynchdbMappingKind kind, const char *attributes, const char *connector)
{
	Oid types[2] = {TEXTOID, TEXTOID};
	Datum values[2];
	char *query;
	char *schema;
	char *table;
	char *qualified;
	HeapTuple tuple;
	TupleDesc desc;

	if (kind != SYNCHDB_MAPPING_TABLE && kind != SYNCHDB_MAPPING_COLUMN && kind != SYNCHDB_MAPPING_DATATYPE)
		return;
	if (kind == SYNCHDB_MAPPING_TABLE)
		query = psprintf("SELECT DISTINCT a.attrelid,n.nspname::pg_catalog.text,c.relname::pg_catalog.text "
			"FROM %s a JOIN pg_catalog.pg_class c ON c.oid OPERATOR(pg_catalog.=) a.attrelid "
			"JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace "
			"WHERE a.name::pg_catalog.text OPERATOR(pg_catalog.=) $1 AND "
			"a.ext_tbname::pg_catalog.text COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.=) $2", attributes);
	else
		query = psprintf("SELECT DISTINCT a.attrelid,n.nspname::pg_catalog.text,c.relname::pg_catalog.text,"
			"p.attnum,p.attname::pg_catalog.text,pg_catalog.format_type(p.atttypid,NULL) "
			"FROM %s a JOIN pg_catalog.pg_class c ON c.oid OPERATOR(pg_catalog.=) a.attrelid "
			"JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace "
			"JOIN pg_catalog.pg_attribute p ON p.attrelid OPERATOR(pg_catalog.=) a.attrelid "
			"AND p.attnum OPERATOR(pg_catalog.=) a.attnum AND NOT p.attisdropped "
			"WHERE a.name::pg_catalog.text OPERATOR(pg_catalog.=) $1 AND "
			"(a.ext_tbname::pg_catalog.text OPERATOR(pg_catalog.||) '.'::pg_catalog.text OPERATOR(pg_catalog.||) "
			"a.ext_attname::pg_catalog.text) COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.=) $2", attributes);
	values[0] = CStringGetTextDatum(connector);
	if (kind == SYNCHDB_MAPPING_DATATYPE && strchr(map->srcobj, '|'))
		values[1] = CStringGetTextDatum(pnstrdup(map->srcobj,
										strchr(map->srcobj, '|') - map->srcobj));
	else
		values[1] = CStringGetTextDatum(map->srcobj);
	if (SPI_execute_with_args(query, 2, types, values, NULL, true, 2) != SPI_OK_SELECT)
		elog(ERROR, "could not bind object mapping target");
	if (SPI_processed > 1)
		ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
				errmsg("object mapping source \"%s\" has multiple target bindings", map->srcobj)));
	if (SPI_processed == 0)
		return;
	tuple = SPI_tuptable->vals[0];
	desc = SPI_tuptable->tupdesc;
	schema = mapping_text(tuple, desc, 2, true);
	table = mapping_text(tuple, desc, 3, true);
	qualified = psprintf("%s.%s", schema, table);
	/* A byte-identical binding needs no legacy ALTER, even for quoted names. */
	if (kind == SYNCHDB_MAPPING_TABLE &&
		!strchr(schema, '.') && !strchr(table, '.') &&
		!strchr(schema, '"') && !strchr(table, '"') &&
		(strcmp(map->dstobj, qualified) == 0 ||
		 (strcmp(schema, "public") == 0 && strcmp(map->dstobj, table) == 0)))
		return;
	if (kind == SYNCHDB_MAPPING_COLUMN &&
		!strchr(map->dstobj, '"') &&
		strcmp(map->dstobj, mapping_text(tuple, desc, 5, true)) == 0)
		return;
	if (kind == SYNCHDB_MAPPING_TABLE || kind == SYNCHDB_MAPPING_COLUMN)
		mapping_check_target_name(kind, map->dstobj);
	mapping_legacy_identifier(schema);
	mapping_legacy_identifier(table);
	mapping_check_length(qualified, sizeof(map->curr_pg_tbname), "bound table");
	strlcpy(map->curr_pg_tbname, qualified, sizeof(map->curr_pg_tbname));
	if (kind != SYNCHDB_MAPPING_TABLE)
	{
		char *column = mapping_text(tuple, desc, 5, true);
		char *datatype = mapping_text(tuple, desc, 6, true);

		mapping_legacy_identifier(column);
		mapping_check_length(column, sizeof(map->curr_pg_attname), "bound column");
		mapping_check_length(datatype, sizeof(map->curr_pg_atttypename), "bound datatype");
		strlcpy(map->curr_pg_attname, column, sizeof(map->curr_pg_attname));
		strlcpy(map->curr_pg_atttypename, datatype, sizeof(map->curr_pg_atttypename));
	}
}

static void
mapping_store_list(const char *connector, ObjectMap **out, int *count,
				   MemoryContext result_context, bool bind_targets)
{
	Oid types[1] = {TEXTOID};
	Datum values[1];
	ObjectMap *checked;
	char *relation;
	char *attributes = NULL;
	char *query;
	int rows;
	int i;
	Size result_size;

	if (!connector || !connector[0] || !out || !count || !result_context)
		elog(ERROR, "invalid object mapping list arguments");
	*out = NULL;
	*count = 0;
	mapping_check_length(connector, SYNCHDB_CONNINFO_NAME_SIZE, "connector");
	synchdb_store_connect();
	relation = synchdb_store_relation(SYNCHDB_OBJECT_MAPPING_TABLE, AccessShareLock);
	if (bind_targets)
		attributes = synchdb_store_relation(SYNCHDB_ATTRIBUTE_TABLE, AccessShareLock);
	query = psprintf("SELECT objtype::pg_catalog.text,COALESCE(enabled,false),srcobj::pg_catalog.text,dstobj "
		"FROM %s WHERE name::pg_catalog.text OPERATOR(pg_catalog.=) $1 "
		"ORDER BY CASE WHEN pg_catalog.lower(objtype::pg_catalog.text) OPERATOR(pg_catalog.=) 'transform'::pg_catalog.text THEN 1 "
		"WHEN pg_catalog.lower(objtype::pg_catalog.text) OPERATOR(pg_catalog.=) 'datatype'::pg_catalog.text THEN 2 "
		"WHEN pg_catalog.lower(objtype::pg_catalog.text) OPERATOR(pg_catalog.=) 'column'::pg_catalog.text THEN 3 "
		"WHEN pg_catalog.lower(objtype::pg_catalog.text) OPERATOR(pg_catalog.=) 'table'::pg_catalog.text THEN 4 ELSE 5 END,"
		"srcobj COLLATE pg_catalog.\"C\"", relation);
	values[0] = CStringGetTextDatum(connector);
	if (SPI_execute_with_args(query, 1, types, values, NULL, true, 0) != SPI_OK_SELECT)
		elog(ERROR, "could not list object mappings");
	if (SPI_processed > INT_MAX || SPI_processed > MaxAllocSize / sizeof(ObjectMap))
		elog(ERROR, "too many object mappings");
	rows = (int) SPI_processed;
	checked = palloc0(rows * sizeof(ObjectMap));
	result_size = mul_size(rows, sizeof(ObjectMap));
	for (i = 0; i < rows; i++)
	{
		HeapTuple tuple = SPI_tuptable->vals[i];
		TupleDesc desc = SPI_tuptable->tupdesc;
		SynchdbMappingKind kind = synchdb_mapping_kind(mapping_text(tuple, desc, 1, true));
		char *source = mapping_text(tuple, desc, 3, true);
		char *destination;
		bool isnull;
		Datum enabled;

		strlcpy(checked[i].objtype, mapping_kind_name(kind), sizeof(checked[i].objtype));
		if (SPI_gettypeid(desc, 2) != BOOLOID)
			ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("invalid object mapping enabled type")));
		enabled = SPI_getbinval(tuple, desc, 2, &isnull);
		checked[i].enabled = !isnull && DatumGetBool(enabled);
		mapping_check_source(kind, source);
		checked[i].srcobj = source;
		result_size = add_size(result_size, strlen(source) + 1);
		if (result_size > MaxAllocSize)
			ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					errmsg("object mapping result exceeds the allocation limit")));
		if (!checked[i].enabled)
			continue;
		destination = mapping_text(tuple, desc, 4, true);
		mapping_check_destination(kind, source, destination);
		strlcpy(checked[i].dstobj, destination, sizeof(checked[i].dstobj));
	}
	if (bind_targets)
		for (i = 0; i < rows; i++)
			if (checked[i].enabled)
				mapping_bind(&checked[i], synchdb_mapping_kind(checked[i].objtype), attributes, connector);
	if (rows > 0)
	{
		char *strings;

		*out = MemoryContextAlloc(result_context, result_size);
		memcpy(*out, checked, rows * sizeof(ObjectMap));
		strings = (char *) *out + rows * sizeof(ObjectMap);
		for (i = 0; i < rows; i++)
		{
			Size source_size = strlen(checked[i].srcobj) + 1;

			memcpy(strings, checked[i].srcobj, source_size);
			(*out)[i].srcobj = strings;
			strings += source_size;
		}
	}
	synchdb_store_finish();
	*count = rows;
}

void
synchdb_mapping_store_list(const char *connector, ObjectMap **out, int *count,
						   MemoryContext result_context)
{
	mapping_store_list(connector, out, count, result_context, true);
}

void
synchdb_mapping_store_list_raw(const char *connector, ObjectMap **out, int *count,
							   MemoryContext result_context)
{
	mapping_store_list(connector, out, count, result_context, false);
}
