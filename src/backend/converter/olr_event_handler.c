/*
 * olr_event_handler.c
 *
 * contains routines to process change events originated from
 * openlog replicator.
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "access/sysattr.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/jsonfuncs.h"
#include "parser/parser.h"
#include "parser/scanner.h"
#include "mb/pg_wchar.h"
#include "nodes/parsenodes.h"
#include "synchdb/synchdb.h"
#include <time.h>
#include <sys/time.h>
#include <dlfcn.h>

#include "converter/olr_event_handler.h"
#include "converter/format_converter.h"
#include "converter/debezium_bind.h"
#include "executor/truncate.h"
#include "source/truncate_sql.h"
#include "olr/olr_client.h"
#include "runtime/config.h"
#include "olr/olr_handoff.h"

/* Oracle raw parser function prototype */
typedef List * (*oracle_raw_parser_fn)(const char *str, RawParseMode mode);
static oracle_raw_parser_fn synchdb_oracle_raw_parser = NULL;
static void * handle = NULL;

static bool isalllower(const char *s);
static bool isallupper(const char *s);
static OlrType getOlrTypeFromString(const char * typestring);
static bool is_whitelist_sql(StringInfoData * sql);
static bool is_whitelist_table(const char * table);
static SourceColumnCache * build_olr_schema_jsonpos_hash(Jsonb * jb);
static void destroyOLRDDL(OLR_DDL * ddlinfo);
static void destroyOLRDML(OLR_DML * dmlinfo);
static OLR_DDL * parseOLRDDL(Jsonb * jb, Jsonb * payload, orascn * scn,
		orascn * c_scn, orascn * c_idx, bool isfirst, bool islast, bool *ignored);
static OLR_DML * parseOLRDML(Jsonb * jb, char op, Jsonb * payload,
		orascn * scn, orascn * c_scn, orascn * c_idx,
		bool isfirst, bool islast);

static bool
isallupper(const char *s)
{
    const unsigned char *p = (const unsigned char *) s;
    for (; *p; p++)
    {
        if (*p >= 'a' && *p <= 'z')
            return false;
    }
    return true;
}

static bool
isalllower(const char *s)
{
    const unsigned char *p = (const unsigned char *) s;
    for (; *p; p++)
    {
        if (*p >= 'A' && *p <= 'Z')
            return false;
    }
    return true;
}

static OlrType
getOlrTypeFromString(const char * typestring)
{
	if (!typestring)
		return OLRTYPE_UNDEF;

	/* todo: perhaps a hash lookup table is more efficient */
	/* OLR types */
	if (!strcmp(typestring, "number") ||
		!strcmp(typestring, "binary_float") ||
		!strcmp(typestring, "binary_double") ||
		!strcmp(typestring, "date") ||
		!strcmp(typestring, "timestamp") ||
		!strcmp(typestring, "timestamp with local time zone"))
		return OLRTYPE_NUMBER;

	if (!strcmp(typestring, "char") ||
		!strcmp(typestring, "varchar2") ||
		!strcmp(typestring, "varchar") ||
		!strcmp(typestring, "nvarchar") ||
		!strcmp(typestring, "nvarchar2") ||
		!strcmp(typestring, "raw") ||
		!strcmp(typestring, "blob") ||
		!strcmp(typestring, "clob") ||
		!strcmp(typestring, "long") ||
		!strcmp(typestring, "urowid") ||
		!strcmp(typestring, "rowid") ||
		!strcmp(typestring, "unknown") ||
		!strcmp(typestring, "nclob") ||
		!strcmp(typestring, "interval day to second") ||
		!strcmp(typestring, "interval year to month") ||
		!strcmp(typestring, "timestamp with time zone"))
		return OLRTYPE_STRING;

	elog(DEBUG1, "unexpected dbz type %s - default to numeric type "
			"representation", typestring);
	return OLRTYPE_UNDEF;
}

static bool
is_whitelist_table(const char * table)
{
	/* special checking to exclude LOG_MINING_FLUSH table created by Debezium. */
	if (!strcasecmp(table, DBZ_LOG_MINING_FLUSH_TABLE))
	{
		return false;
	}
	return true;
}

/* Classify SQL tokens, not substrings inside object names. In particular,
 * CREATE INDEX on CREATE_TABLE_TEST is not a CREATE TABLE statement.
 * The core scanner also keeps quoted parentheses and AS inside identifiers
 * from corrupting Oracle storage-clause/recycle-bin normalization. */
static bool
is_whitelist_sql(StringInfoData *sql)
{
	core_yy_extra_type extra = {0};
	core_yyscan_t scanner;
	core_YYSTYPE value;
	YYLTYPE location;
	char *command;
	const char *token;
	bool create;
	bool drop;
	bool alter;
	bool allowed;
	int level = 0;
	int length = sql->len;

	scanner = scanner_init(sql->data, &extra, &ScanKeywords, ScanKeywordTokens);
	if (!core_yylex(&value, &location, scanner))
	{
		scanner_finish(scanner);
		return false;
	}
	command = pstrdup(extra.scanbuf + location);
	create = pg_strcasecmp(command, "CREATE") == 0;
	drop = pg_strcasecmp(command, "DROP") == 0;
	alter = pg_strcasecmp(command, "ALTER") == 0;
	allowed = create || drop || alter;
	if (!core_yylex(&value, &location, scanner))
		allowed = false;
	else
	{
		token = extra.scanbuf + location;
		while (create && (pg_strcasecmp(token, "GLOBAL") == 0 ||
			pg_strcasecmp(token, "PRIVATE") == 0 ||
			pg_strcasecmp(token, "TEMPORARY") == 0))
		{
			if (!core_yylex(&value, &location, scanner))
				break;
			token = extra.scanbuf + location;
		}
		allowed = allowed && pg_strcasecmp(token, "TABLE") == 0;
	}
	if (allowed && alter)
	{
		/* Read the qualified table name before inspecting its action.
		 * Supplemental logging changes capture metadata, not target columns. */
		if (core_yylex(&value, &location, scanner))
		{
			while (core_yylex(&value, &location, scanner))
			{
				token = extra.scanbuf + location;
				if (strcmp(token, ".") != 0)
					break;
				if (!core_yylex(&value, &location, scanner))
					break;
			}
			token = extra.scanbuf + location;
			if ((pg_strcasecmp(token, "ADD") == 0 || pg_strcasecmp(token, "DROP") == 0) &&
				core_yylex(&value, &location, scanner) &&
				pg_strcasecmp(extra.scanbuf + location, "SUPPLEMENTAL") == 0 &&
				core_yylex(&value, &location, scanner) &&
				pg_strcasecmp(extra.scanbuf + location, "LOG") == 0 &&
				core_yylex(&value, &location, scanner) &&
				(pg_strcasecmp(extra.scanbuf + location, "DATA") == 0 ||
				 pg_strcasecmp(extra.scanbuf + location, "GROUP") == 0))
				allowed = false;
		}
	}
	if (allowed && (create || drop))
	{
		while (core_yylex(&value, &location, scanner))
		{
			token = extra.scanbuf + location;
			if (drop && pg_strcasecmp(token, "AS") == 0)
			{
				length = location;
				break;
			}
			if (create && strcmp(token, "(") == 0)
				level++;
			else if (create && strcmp(token, ")") == 0 && --level == 0)
			{
				length = location + 1;
				break;
			}
		}
	}
	scanner_finish(scanner);
	pfree(command);
	sql->data[length] = '\0';
	sql->len = length;
	return allowed;
}

static SourceColumnCache *
build_olr_schema_jsonpos_hash(Jsonb *jb)
{
	SourceColumnCache *cache = fc_create_source_column_cache(TopMemoryContext);
	Datum path[] = {CStringGetTextDatum("columns")};
	Jsonb *schema = GET_JSONB_ELEM(jb, path, lengthof(path));
	unsigned int i;

	PG_TRY();
	{
		if (!schema || !JB_ROOT_IS_ARRAY(schema))
			elog(ERROR, "source column schema must be an array");
		for (i = 0; i < JsonContainerSize(&schema->root); i++)
		{
			JsonbValue *item = getIthJsonbValueFromContainer(&schema->root, i);
			JsonbValue buffer;
			JsonbValue *value;
			NameJsonposEntry record = {0};
			char *token;

			if (!item || item->type != jbvBinary || !JsonContainerIsObject(item->val.binary.data))
				elog(ERROR, "source column schema entry must be an object");
			value = getKeyJsonValueFromContainer(item->val.binary.data, "name", 4, &buffer);
			if (!value || value->type != jbvString)
				elog(ERROR, "source column schema requires a string name");
			record.name = pnstrdup(value->val.string.val, value->val.string.len);
			value = getKeyJsonValueFromContainer(item->val.binary.data, "type", 4, &buffer);
			if (!value || value->type != jbvString)
				elog(ERROR, "source column schema requires a string type");
			token = pnstrdup(value->val.string.val, value->val.string.len);
			record.dbztype = getOlrTypeFromString(token);
			pfree(token);

			value = getKeyJsonValueFromContainer(item->val.binary.data, "scale", 5, &buffer);
			if (value && value->type != jbvNull)
			{
				if (value->type == jbvString)
					record.scale = pg_strtoint32(pnstrdup(value->val.string.val, value->val.string.len));
				else if (value->type == jbvNumeric)
					record.scale = DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(value->val.numeric)));
				else
					elog(ERROR, "source column scale must be a string or number");
			}

			/* The source's spelling is exact; target casing is applied separately. */
			fc_source_column_insert(cache, record.name, i, record.dbztype,
									record.timerep, record.scale);
			pfree(record.name);
		}
	}
	PG_CATCH();
	{
		fc_destroy_source_column_cache(cache);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return cache;
}

/*
 * destroyOLRDDL
 *
 * Function to destroy OLR_DDL structure
 */
static void
destroyOLRDDL(OLR_DDL * ddlinfo)
{
	if (ddlinfo)
	{
		if (ddlinfo->id)
			pfree(ddlinfo->id);
		if (ddlinfo->source_database)
			pfree(ddlinfo->source_database);
		if (ddlinfo->source_schema)
			pfree(ddlinfo->source_schema);
		if (ddlinfo->source_table)
			pfree(ddlinfo->source_table);

		if (ddlinfo->primaryKeyColumnNames)
			pfree(ddlinfo->primaryKeyColumnNames);

		list_free_deep(ddlinfo->columns);

		pfree(ddlinfo);
	}
}

/*
 * destroyOLRDML
 *
 * Function to destroy OLR_DML structure
 */
static void
destroyOLRDML(OLR_DML * dmlinfo)
{
	if (dmlinfo)
	{
		if (dmlinfo->table)
			pfree(dmlinfo->table);

		if (dmlinfo->schema)
			pfree(dmlinfo->schema);

		if (dmlinfo->remoteObjectId)
			pfree(dmlinfo->remoteObjectId);

		if (dmlinfo->mappedObjectId)
			pfree(dmlinfo->mappedObjectId);

		if (dmlinfo->columnValuesBefore)
			list_free_deep(dmlinfo->columnValuesBefore);

		if (dmlinfo->columnValuesAfter)
			list_free_deep(dmlinfo->columnValuesAfter);

		pfree(dmlinfo);
	}
}

static void
load_oracle_parser(void)
{
	if (synchdb_oracle_raw_parser == NULL)
	{
		char * oralib_path = NULL, * error = NULL;

		oralib_path = psprintf("%s/%s", pkglib_path, ORACLE_RAW_PARSER_LIB);

		/*
		 * RTLD_LOCAL keeps the parser's internal ora_* symbols private to
		 * the library so they do not collide with IvorySQL's built-in
		 * Oracle parser (which exports the same names).  We only use the
		 * uniquely named synchdb_oracle_raw_parser entry via dlsym(), which
		 * works through the explicit handle regardless of LOCAL/GLOBAL.
		 */
		handle = dlopen(oralib_path, RTLD_NOW | RTLD_LOCAL);
		if (!handle)
		{
			set_shm_connector_errmsg(myConnectorId, "failed to load oracle_parser.so");
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("failed to load oracle_parser.so at %s", oralib_path),
					 errhint("%s",  dlerror())));

		}
		pfree(oralib_path);

		synchdb_oracle_raw_parser = (oracle_raw_parser_fn) dlsym(handle, "synchdb_oracle_raw_parser");
		if ((error = dlerror()) != NULL)
		{
			set_shm_connector_errmsg(myConnectorId, "failed to load synchdb_oracle_raw_parser symbol");
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("failed to load synchdb_oracle_raw_parser function symbol"),
					 errhint("make sure synchdb_oracle_raw_parser() function in libsynchdb_oracle_parser.so is "
							 "publicly accessible ")));
		}
	}
}

/*
 * parseOLRDDL
 */
static OLR_DDL *
parseOLRDDL(Jsonb * jb, Jsonb * payload, orascn * scn, orascn * c_scn, orascn * c_idx, bool isfirst, bool islast, bool *ignored)
{
	JsonbValue * v = NULL;
	JsonbValue vbuf;
	Jsonb * jbschema;
	OLR_DDL * olrddl = NULL;
	OLR_DDL_COLUMN * ddlcol = NULL;
	Datum datum_path_schema[1] = {CStringGetTextDatum("schema")};
	char * db = NULL, * schema = NULL, * table = NULL;
	StringInfoData sql;
	List * ptree = NULL;
	ListCell * cell;
	SynchdbSourceName truncate_name;

	/* scn - required */
	v = getKeyJsonValueFromContainer(&jb->root, "scn", strlen("scn"), &vbuf);
	if (!v || v->type != jbvNumeric)
	{
		elog(WARNING, "malformed change request - no scn value");
		goto end;
	}
	*scn = DatumGetUInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(v->val.numeric)));

	/* commit scn - required */
	v = getKeyJsonValueFromContainer(&jb->root, "c_scn", strlen("c_scn"), &vbuf);
	if (!v || v->type != jbvNumeric)
	{
		elog(WARNING, "malformed change request - no c_scn value");
		goto end;
	}
	*c_scn = DatumGetUInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(v->val.numeric)));

	/* commit index - required */
	v = getKeyJsonValueFromContainer(&jb->root, "c_idx", strlen("c_idx"), &vbuf);
	if (!v || v->type != jbvNumeric)
	{
		elog(WARNING, "malformed change request - no c_idx value");
		goto end;
	}
	* c_idx = DatumGetUInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(v->val.numeric)));

	/* db - required */
	v = getKeyJsonValueFromContainer(&jb->root, "db", strlen("db"), &vbuf);
	if (!v || v->type != jbvString || v->val.string.len == 0)
	{
		elog(WARNING, "malformed change request - no db value");
		goto end;
	}
	db = pnstrdup(v->val.string.val, v->val.string.len);

	/* fetch payload.0.schema */
	jbschema = GET_JSONB_ELEM(payload, &datum_path_schema[0], 1);
	if (!jbschema || !JB_ROOT_IS_OBJECT(jbschema))
	{
		elog(WARNING, "malformed change request - no payload.0.schema struct");
		goto end;
	}

	/* fetch owner -> considered schema - optional*/
	v = getKeyJsonValueFromContainer(&jbschema->root, "owner", strlen("owner"), &vbuf);
	if (v && v->type == jbvString && v->val.string.len > 0)
	{
		schema = pnstrdup(v->val.string.val, v->val.string.len);

		/* The handoff filter has verified the configured source schema.
		 * A privileged mining account need not own that schema. */
	}
	else
	{
		/*
		 * no schema: we will ignore all DDLs without a schema (normally the username) because
		 * OLR sends a lot of DDLs from system's internal maintainance so most of them are not
		 * intended for user tables
		 */
		elog(DEBUG1, "skip ddl with no schema...");
		goto end;
	}

	/* fetch payload.0.schema.table - required */
	v = getKeyJsonValueFromContainer(&jbschema->root, "table", strlen("table"), &vbuf);
	if (!v || v->type != jbvString || v->val.string.len == 0)
	{
		elog(WARNING, "malformed change request - no payload.0.schema.table value");
		goto end;
	}
	table = pnstrdup(v->val.string.val, v->val.string.len);

	if (!is_whitelist_table(table))
	{
		elog(DEBUG1, "table %s is not white-listed...", table);
		goto end;
	}

	/* fetch sql - required */
	initStringInfo(&sql);
	v = getKeyJsonValueFromContainer(&payload->root, "sql", strlen("sql"), &vbuf);
	if (!v || v->type != jbvString || v->val.string.len == 0)
	{
		elog(WARNING, "malformed change request - no payload.0.sql value");
		goto end;
	}

	appendBinaryStringInfo(&sql, v->val.string.val, v->val.string.len);

	/* A table-wide source event has no columns and needs no Oracle parser.
	 * Validate the whole statement and its exact source identity before binding
	 * any target. Quoted Oracle names are case sensitive. */
	if (synchdb_source_parse_truncate(sql.data, "oracle", &truncate_name))
	{
		synchdb_source_check_truncate_name(&truncate_name, "oracle", db, schema, table);
		if (synchdb_operation_skipped('t'))
		{
			elog(LOG, "skipping source TRUNCATE for %s.%s by configured operation policy",
				 db, table);
			*ignored = true;
			goto end;
		}
		olrddl = palloc0(sizeof(*olrddl));
		olrddl->type = DDL_TRUNCATE_TABLE;
		olrddl->source_database = pstrdup(db);
		olrddl->source_schema = pstrdup(schema);
		olrddl->source_table = pstrdup(table);
		if (isfirst || islast)
		{
			v = getKeyJsonValueFromContainer(&jb->root, "tm", strlen("tm"), &vbuf);
			if (v && v->type == jbvNumeric)
				olrddl->src_ts_ms = DatumGetUInt64(DirectFunctionCall1(numeric_int8,
					NumericGetDatum(v->val.numeric))) / 1000 / 1000;
		}
		return olrddl;
	}

	if (!is_whitelist_sql(&sql))
	{
		elog(DEBUG1, "auxiliary DDL is excluded from replication: %s", sql.data);
		*ignored = true;
		goto end;
	}

	/* construct the ddl struct */
	olrddl = (DBZ_DDL*) palloc0(sizeof(DBZ_DDL));
	olrddl->source_database = pstrdup(db);
	olrddl->source_schema = pstrdup(schema);
	olrddl->source_table = pstrdup(table);

	/* tm - only on first and last record within a batch */
	if (isfirst || islast)
	{
		v = getKeyJsonValueFromContainer(&jb->root, "tm", strlen("tm"), &vbuf);
		if (v)
		{
			olrddl->src_ts_ms = DatumGetUInt64(DirectFunctionCall1(numeric_int8,
					NumericGetDatum(v->val.numeric))) / 1000 / 1000;
		}
	}
	load_oracle_parser();
	/* Parse the Oracle SQL */
	PG_TRY();
	{
		ptree = synchdb_oracle_raw_parser(sql.data, RAW_PARSE_DEFAULT);
	}
	PG_CATCH();
	{
		MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);
		ErrorData  *errdata = CopyErrorData();

		elog(WARNING, "skipping bad DDL statement: '%s'", sql.data);
		if (errdata)
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "    reason: %s",
					errdata->message);
			elog(WARNING, "%s", msg);
			pfree(msg);
		}
		FreeErrorData(errdata);
		MemoryContextSwitchTo(oldctx);
		FlushErrorState();

		destroyOLRDDL(olrddl);
		olrddl = NULL;
		goto end;
	}
	PG_END_TRY();

	if (list_length(ptree) != 1)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("native OLR DDL requires one SQL statement per event")));

	foreach(cell, ptree)
	{
		RawStmt *raw = (RawStmt *) lfirst(cell);
		Node *stmt = NULL;

		if (!IsA(raw, RawStmt))
		{
			elog(WARNING, "not a raw stmt");
			continue;
		}

		stmt = raw->stmt;
		if (IsA(stmt, CreateStmt))
		{
			/* CREATE TABLE */
			ListCell *colCell;
			ListCell *constrCell;
			CreateStmt *createStmt = (CreateStmt *) stmt;
			RangeVar *relation = createStmt->relation;
			StringInfoData pklist;
			char *schemaName = relation->schemaname ? relation->schemaname : "public";
			char *tableName = relation->relname;

			elog(DEBUG1, "Creating table: %s.%s", schemaName, tableName);

			/* prepare to build primary key list */
			initStringInfo(&pklist);

			/* Columns and constraints */
			olrddl->type = DDL_CREATE_TABLE;
			appendStringInfo(&pklist, "[");
			foreach(colCell, createStmt->tableElts)
			{
				Node *elt = (Node *) lfirst(colCell);

				if (IsA(elt, ColumnDef))
				{
					ColumnDef *col = (ColumnDef *) elt;
					ddlcol = (OLR_DDL_COLUMN *) palloc0(sizeof(OLR_DDL_COLUMN));

					/* column name */
					elog(DEBUG1, "Column: %s", col->colname);
					ddlcol->name = pstrdup(col->colname);

					/*
					 * OLR delivers the name in reverse letter casing. (all cap to all lower or all lower
					 * to all cap), so for "as is" normalization strategy, we need to reverse it back to
					 * get the original object name. If the name contains a mixture of upper and lower
					 * case letters, it is delivered as is.
					 */
					if (synchdb_letter_casing_strategy == LCS_AS_IS)
					{
						if (isallupper(ddlcol->name))	/* need to normalize to lowercase */
							fc_normalize_name(LCS_NORMALIZE_LOWERCASE, ddlcol->name, strlen(ddlcol->name));
						else if (isalllower(ddlcol->name))
							fc_normalize_name(LCS_NORMALIZE_UPPERCASE, ddlcol->name, strlen(ddlcol->name));
						else
							fc_normalize_name(LCS_AS_IS, ddlcol->name, strlen(ddlcol->name));
					}
					else
						fc_normalize_name(synchdb_letter_casing_strategy, ddlcol->name, strlen(ddlcol->name));

					/* data type */
					if (col->typeName && col->typeName->names)
						elog(DEBUG1, "Type: %s", NameListToString(col->typeName->names));
					ddlcol->typeName = pstrdup(NameListToString(col->typeName->names));

					/* data type always normalized to lower case */
					fc_normalize_name(LCS_NORMALIZE_LOWERCASE, ddlcol->typeName, strlen(ddlcol->typeName));

					/* is optional? */
					if (col->is_not_null)
						ddlcol->optional = false;
					else
						ddlcol->optional = true;

					elog(DEBUG1, "Optional: %d", ddlcol->optional);

					/* auto increment? - assumed false todo */
					ddlcol->autoIncremented = false;

					/* length */
					if (list_length(col->typeName->typmods) > 0)
					{
						Node *lenNode = (Node *) linitial(col->typeName->typmods);
						if (IsA(lenNode, A_Const))
						{
					        A_Const *aconst = (A_Const *) lenNode;
					        if (IsA(&aconst->val, Integer))
					            ddlcol->length = intVal(&aconst->val);
					        else
					            elog(DEBUG1, "Expected integer for length value, got %d", nodeTag(&aconst->val));
						}
						else
						{
							elog(DEBUG1, "Expected a ConstNode, but got %d", nodeTag(lenNode));
							ddlcol->length = 0;
						}
					}
					elog(DEBUG1, "length = %d", ddlcol->length);

					/* scale */
					if (list_length(col->typeName->typmods) > 1)
					{
						Node *scaleNode = (Node *) lsecond(col->typeName->typmods);
						if (IsA(scaleNode, A_Const))
						{
					        A_Const *aconst = (A_Const *) scaleNode;
					        if (IsA(&aconst->val, Integer))
					            ddlcol->scale = intVal(&aconst->val);
					        else
					            elog(DEBUG1, "Expected integer for scale value, got %d", nodeTag(&aconst->val));
						}
						else
						{
							elog(DEBUG1, "Expected a ConstNode, but got %d", nodeTag(scaleNode));
							ddlcol->length = 0;
						}
					}
					elog(DEBUG1, "scale = %d", ddlcol->scale);

					/* inline constraint */
					if (col->constraints)
					{
						elog(DEBUG1, "col %s has inline constraint", col->colname);
						foreach(constrCell, col->constraints)
						{
							Constraint *constr = (Constraint *) lfirst(constrCell);

							if (constr->contype == CONSTR_PRIMARY)
							{
								/* primary key indication */
								elog(DEBUG1, "col %s is a pk", col->colname);
								appendStringInfo(&pklist, "\"%s\",", col->colname);
							}
							else if (constr->contype == CONSTR_DEFAULT)
					        {
					            /*
					             * default value expression - always defaults to null at later
					             * stage of processing so it does not matter what expression is
					             * put here. todo
					             */
					            ddlcol->defaultValueExpression = nodeToString(constr->raw_expr);
					            elog(DEBUG1, "default value %s", ddlcol->defaultValueExpression);
					        }
							else if (constr->contype == CONSTR_NOTNULL)
							{
								elog(DEBUG1, "col %s is not null", col->colname);
								ddlcol->optional = false;
							}
							else
								elog(WARNING, "unsupported constraint type %d", constr->contype);
						}
					}
					olrddl->columns = lappend(olrddl->columns, ddlcol);
				}
				else if (IsA(elt, Constraint))
				{
					/* separately defined constraint */
					Constraint *constr = (Constraint *) elt;

					if (constr->contype == CONSTR_PRIMARY)
					{
						ListCell * keys;

						elog(DEBUG1, "found a primary key constraint");
						foreach(keys, constr->keys)
						{
							char *keycol = strVal(lfirst(keys));
							elog(DEBUG1, "  -> PK Column: %s", keycol);
							appendStringInfo(&pklist, "\"%s\",", keycol);
						}
					}
				}
			}

			if (pklist.len > 1)	/* longer than '[' */
			{
				pklist.data[pklist.len - 1] = '\0';
				pklist.len = pklist.len - 1;
				appendStringInfo(&pklist, "]");
				elog(DEBUG1, "pks = %s", pklist.data);
				olrddl->primaryKeyColumnNames = pstrdup(pklist.data);

				/*
				 * OLR delivers the name in reverse letter casing. (all cap to all lower or all lower
				 * to all cap), so for "as is" normalization strategy, we need to reverse it back to
				 * get the original object name. If the name contains a mixture of upper and lower
				 * case letters, it is delivered as is.
				 */
				if (synchdb_letter_casing_strategy == LCS_AS_IS)
				{
					if (isallupper(olrddl->primaryKeyColumnNames))	/* need to normalize to lowercase */
						fc_normalize_name(LCS_NORMALIZE_LOWERCASE, olrddl->primaryKeyColumnNames,strlen(olrddl->primaryKeyColumnNames));
					else if (isalllower(olrddl->primaryKeyColumnNames))
						fc_normalize_name(LCS_NORMALIZE_UPPERCASE, olrddl->primaryKeyColumnNames, strlen(olrddl->primaryKeyColumnNames));
					else
						fc_normalize_name(LCS_AS_IS, olrddl->primaryKeyColumnNames, strlen(olrddl->primaryKeyColumnNames));
				}
				else
					fc_normalize_name(synchdb_letter_casing_strategy, olrddl->primaryKeyColumnNames, strlen(olrddl->primaryKeyColumnNames));
			}

			if (pklist.data)
				pfree(pklist.data);

			/*
			 * special checking on potential table name mismatch, always use the table name
			 * from the parser
			 */
			if (strcmp(tableName, table))
			{
				elog(WARNING, "table name mismatch, using %s instead of %s",
						tableName, table);
				pfree(table);
				table = pstrdup(tableName);
			}
		}
		else if (IsA(stmt, AlterTableStmt))
		{
			/* ALTER TABLE */
			ListCell *cmdCell;
			ListCell *constrCell;

			AlterTableStmt *alterStmt = (AlterTableStmt *) stmt;

			elog(DEBUG1, "ALTER TABLE: %s", alterStmt->relation->relname);

			olrddl->type = DDL_ALTER_TABLE;

			foreach(cmdCell, alterStmt->cmds)
			{
				AlterTableCmd *cmd = (AlterTableCmd *) lfirst(cmdCell);

				switch (cmd->subtype)
				{
					case AT_AddColumn:
					{
						StringInfoData pklist;
						ColumnDef *col = (ColumnDef *)cmd->def;
						ddlcol = (OLR_DDL_COLUMN *) palloc0(sizeof(OLR_DDL_COLUMN));

						/* prepare to build primary key list */
						initStringInfo(&pklist);

						/* column name */
						elog(DEBUG1, "  ADD COLUMN: %s", col->colname);
						ddlcol->name = pstrdup(col->colname);
						/*
						 * OLR delivers the name in reverse letter casing. (all cap to all lower or all lower
						 * to all cap), so for "as is" normalization strategy, we need to reverse it back to
						 * get the original object name. If the name contains a mixture of upper and lower
						 * case letters, it is delivered as is.
						 */
						if (synchdb_letter_casing_strategy == LCS_AS_IS)
						{
							if (isallupper(ddlcol->name))	/* need to normalize to lowercase */
								fc_normalize_name(LCS_NORMALIZE_LOWERCASE, ddlcol->name, strlen(ddlcol->name));
							else if (isalllower(ddlcol->name))
								fc_normalize_name(LCS_NORMALIZE_UPPERCASE, ddlcol->name, strlen(ddlcol->name));
							else
								fc_normalize_name(LCS_AS_IS, ddlcol->name, strlen(ddlcol->name));
						}
						else
							fc_normalize_name(synchdb_letter_casing_strategy, ddlcol->name, strlen(ddlcol->name));

						olrddl->subtype = SUBTYPE_ADD_COLUMN;

						/* data type */
						if (col->typeName && col->typeName->names)
							elog(DEBUG1, "Type: %s", NameListToString(col->typeName->names));
						ddlcol->typeName = pstrdup(NameListToString(col->typeName->names));

						/* data type always normalized to lower case */
						fc_normalize_name(LCS_NORMALIZE_LOWERCASE, ddlcol->typeName, strlen(ddlcol->typeName));

						/* is optional? */
						if (col->is_not_null)
							ddlcol->optional = false;
						else
							ddlcol->optional = true;

						elog(DEBUG1, "Optional: %d", ddlcol->optional);

						/* auto increment? - assumed false todo */
						ddlcol->autoIncremented = false;

						/* length */
						if (list_length(col->typeName->typmods) > 0)
						{
							Node *lenNode = (Node *) linitial(col->typeName->typmods);
							if (IsA(lenNode, A_Const))
							{
						        A_Const *aconst = (A_Const *) lenNode;
						        if (IsA(&aconst->val, Integer))
						            ddlcol->length = intVal(&aconst->val);
						        else
						            elog(DEBUG1, "Expected integer for length value, got %d", nodeTag(&aconst->val));
							}
							else
							{
								elog(DEBUG1, "Expected a ConstNode, but got %d", nodeTag(lenNode));
								ddlcol->length = 0;
							}
						}
						elog(DEBUG1, "length = %d", ddlcol->length);

						/* scale */
						if (list_length(col->typeName->typmods) > 1)
						{
							Node *scaleNode = (Node *) lsecond(col->typeName->typmods);
							if (IsA(scaleNode, A_Const))
							{
								A_Const *aconst = (A_Const *) scaleNode;
								if (IsA(&aconst->val, Integer))
									ddlcol->scale = intVal(&aconst->val);
								else
									elog(DEBUG1, "Expected integer for scale value, got %d", nodeTag(&aconst->val));
							}
							else
							{
								elog(DEBUG1, "Expected a ConstNode, but got %d", nodeTag(scaleNode));
								ddlcol->length = 0;
							}
						}
						elog(DEBUG1, "scale = %d", ddlcol->scale);

						/* inline constraint */
						if (col->constraints)
						{
							elog(DEBUG1, "col %s has inline constraint", col->colname);
							appendStringInfo(&pklist, "[");
							foreach(constrCell, col->constraints)
							{
								Constraint *constr = (Constraint *) lfirst(constrCell);

								if (constr->contype == CONSTR_PRIMARY)
								{
									/* primary key indication */
									elog(DEBUG1, "col %s is a pk", col->colname);
									appendStringInfo(&pklist, "\"%s\",", col->colname);
								}
								else if (constr->contype == CONSTR_DEFAULT)
						        {
						            /*
						             * default value expression - always defaults to null at later
						             * stage of processing so it does not matter what expression is
						             * put here. todo
						             */
						            ddlcol->defaultValueExpression = nodeToString(constr->raw_expr);
						            elog(DEBUG1, "default value %s", ddlcol->defaultValueExpression);
						        }
								else if (constr->contype == CONSTR_NOTNULL)
								{
									elog(DEBUG1, "col %s is not null", col->colname);
									ddlcol->optional = false;
								}
								else
									elog(WARNING, "unsupported constraint type %d", constr->contype);
							}

							if (pklist.len > 1)	/* longer than '[' */
							{
								pklist.data[pklist.len - 1] = '\0';
								pklist.len = pklist.len - 1;
								appendStringInfo(&pklist, "]");
								olrddl->primaryKeyColumnNames = pstrdup(pklist.data);

								/*
								 * OLR delivers the name in reverse letter casing. (all cap to all lower or all lower
								 * to all cap), so for "as is" normalization strategy, we need to reverse it back to
								 * get the original object name. If the name contains a mixture of upper and lower
								 * case letters, it is delivered as is.
								 */
								if (synchdb_letter_casing_strategy == LCS_AS_IS)
								{
									if (isallupper(olrddl->primaryKeyColumnNames))	/* need to normalize to lowercase */
										fc_normalize_name(LCS_NORMALIZE_LOWERCASE, olrddl->primaryKeyColumnNames,strlen(olrddl->primaryKeyColumnNames));
									else if (isalllower(olrddl->primaryKeyColumnNames))
										fc_normalize_name(LCS_NORMALIZE_UPPERCASE, olrddl->primaryKeyColumnNames, strlen(olrddl->primaryKeyColumnNames));
									else
										fc_normalize_name(LCS_AS_IS, olrddl->primaryKeyColumnNames, strlen(olrddl->primaryKeyColumnNames));
								}
								else
									fc_normalize_name(synchdb_letter_casing_strategy, olrddl->primaryKeyColumnNames, strlen(olrddl->primaryKeyColumnNames));
							}
						}
						if (pklist.data)
							pfree(pklist.data);

						break;
					}
					case AT_DropColumn:
					{
						ddlcol = (OLR_DDL_COLUMN *) palloc0(sizeof(OLR_DDL_COLUMN));

						elog(DEBUG1, "DROP COLUMN: %s", cmd->name);
						olrddl->subtype = SUBTYPE_DROP_COLUMN;
						ddlcol->name = pstrdup(cmd->name);

						/*
						 * OLR delivers the name in reverse letter casing. (all cap to all lower or all lower
						 * to all cap), so for "as is" normalization strategy, we need to reverse it back to
						 * get the original object name. If the name contains a mixture of upper and lower
						 * case letters, it is delivered as is.
						 */
						if (synchdb_letter_casing_strategy == LCS_AS_IS)
						{
							if (isallupper(ddlcol->name))	/* need to normalize to lowercase */
								fc_normalize_name(LCS_NORMALIZE_LOWERCASE, ddlcol->name, strlen(ddlcol->name));
							else if (isalllower(ddlcol->name))
								fc_normalize_name(LCS_NORMALIZE_UPPERCASE, ddlcol->name, strlen(ddlcol->name));
							else
								fc_normalize_name(LCS_AS_IS, ddlcol->name, strlen(ddlcol->name));
						}
						else
							fc_normalize_name(synchdb_letter_casing_strategy, ddlcol->name, strlen(ddlcol->name));
						break;
					}
					case AT_AlterColumnType:
					{
						StringInfoData pklist;
						ColumnDef *col = (ColumnDef *)cmd->def;
						ddlcol = (OLR_DDL_COLUMN *) palloc0(sizeof(OLR_DDL_COLUMN));

						/* prepare to build primary key list */
						initStringInfo(&pklist);

						/* column name */
						elog(DEBUG1, "MODIFY COLUMN: %s", cmd->name);
						ddlcol->name = pstrdup(cmd->name);

						/*
						 * OLR delivers the name in reverse letter casing. (all cap to all lower or all lower
						 * to all cap), so for "as is" normalization strategy, we need to reverse it back to
						 * get the original object name. If the name contains a mixture of upper and lower
						 * case letters, it is delivered as is.
						 */
						if (synchdb_letter_casing_strategy == LCS_AS_IS)
						{
							if (isallupper(ddlcol->name))	/* need to normalize to lowercase */
								fc_normalize_name(LCS_NORMALIZE_LOWERCASE, ddlcol->name, strlen(ddlcol->name));
							else if (isalllower(ddlcol->name))
								fc_normalize_name(LCS_NORMALIZE_UPPERCASE, ddlcol->name, strlen(ddlcol->name));
							else
								fc_normalize_name(LCS_AS_IS, ddlcol->name, strlen(ddlcol->name));
						}
						else
							fc_normalize_name(synchdb_letter_casing_strategy, ddlcol->name, strlen(ddlcol->name));

						olrddl->subtype = SUBTYPE_ALTER_COLUMN;

						/* data type */
						if (col->typeName && col->typeName->names)
							elog(DEBUG1, "Type: %s", NameListToString(col->typeName->names));

						ddlcol->typeName = pstrdup(NameListToString(col->typeName->names));

						/* data type always normalized to lower case */
						fc_normalize_name(LCS_NORMALIZE_LOWERCASE, ddlcol->typeName, strlen(ddlcol->typeName));

						/* is optional? */
						if (col->is_not_null)
							ddlcol->optional = false;
						else
							ddlcol->optional = true;

						elog(DEBUG1, "optional: %d", ddlcol->optional);

						/* length */
						if (list_length(col->typeName->typmods) > 0)
						{
							Node *lenNode = (Node *) linitial(col->typeName->typmods);
							if (IsA(lenNode, A_Const))
							{
						        A_Const *aconst = (A_Const *) lenNode;
						        if (IsA(&aconst->val, Integer))
						            ddlcol->length = intVal(&aconst->val);
						        else
						            elog(DEBUG1, "Expected integer for length value, got %d", nodeTag(&aconst->val));
							}
							else
							{
								elog(DEBUG1, "Expected a ConstNode, but got %d", nodeTag(lenNode));
								ddlcol->length = 0;
							}
						}
						elog(DEBUG1, "length = %d", ddlcol->length);

						/* scale */
						if (list_length(col->typeName->typmods) > 1)
						{
							Node *scaleNode = (Node *) lsecond(col->typeName->typmods);
							if (IsA(scaleNode, A_Const))
							{
								A_Const *aconst = (A_Const *) scaleNode;
								if (IsA(&aconst->val, Integer))
									ddlcol->scale = intVal(&aconst->val);
								else
									elog(DEBUG1, "Expected integer for scale value, got %d", nodeTag(&aconst->val));
							}
							else
							{
								elog(DEBUG1, "Expected a ConstNode, but got %d", nodeTag(scaleNode));
								ddlcol->length = 0;
							}
						}
						elog(DEBUG1, "scale = %d", ddlcol->scale);

						/* inline constraint */
						if (col->constraints)
						{
							elog(DEBUG1, "col %s has inline constraint",cmd->name);
							appendStringInfo(&pklist, "[");
							foreach(constrCell, col->constraints)
							{
								Constraint *constr = (Constraint *) lfirst(constrCell);

								if (constr->contype == CONSTR_PRIMARY)
								{
									/* primary key indication */
									elog(DEBUG1, "col %s is a pk", cmd->name);
									appendStringInfo(&pklist, "\"%s\",", cmd->name);
								}
								else if (constr->contype == CONSTR_DEFAULT)
						        {
						            /*
						             * default value expression - always defaults to null at later
						             * stage of processing so it does not matter what expression is
						             * put here. todo
						             */
						            ddlcol->defaultValueExpression = nodeToString(constr->raw_expr);
						            elog(DEBUG1, "default value %s", ddlcol->defaultValueExpression);
						        }
								else if (constr->contype == CONSTR_NOTNULL)
								{
									elog(DEBUG1, "col %s is a not null", cmd->name);
									ddlcol->optional = false;
								}
							}
							if (pklist.len > 1)	/* longer than '[' */
							{
								pklist.data[pklist.len - 1] = '\0';
								pklist.len = pklist.len - 1;
								appendStringInfo(&pklist, "]");
								olrddl->primaryKeyColumnNames = pstrdup(pklist.data);

								/*
								 * OLR delivers the name in reverse letter casing. (all cap to all lower or all lower
								 * to all cap), so for "as is" normalization strategy, we need to reverse it back to
								 * get the original object name. If the name contains a mixture of upper and lower
								 * case letters, it is delivered as is.
								 */
								if (synchdb_letter_casing_strategy == LCS_AS_IS)
								{
									if (isallupper(olrddl->primaryKeyColumnNames))	/* need to normalize to lowercase */
										fc_normalize_name(LCS_NORMALIZE_LOWERCASE, olrddl->primaryKeyColumnNames,strlen(olrddl->primaryKeyColumnNames));
									else if (isalllower(olrddl->primaryKeyColumnNames))
										fc_normalize_name(LCS_NORMALIZE_UPPERCASE, olrddl->primaryKeyColumnNames, strlen(olrddl->primaryKeyColumnNames));
									else
										fc_normalize_name(LCS_AS_IS, olrddl->primaryKeyColumnNames, strlen(olrddl->primaryKeyColumnNames));
								}
								else
									fc_normalize_name(synchdb_letter_casing_strategy, olrddl->primaryKeyColumnNames, strlen(olrddl->primaryKeyColumnNames));
							}
						}
						break;
					}
					case AT_AddConstraint:
					{
						StringInfoData pklist;
						Constraint *constr = (Constraint *)cmd->def;

						olrddl->subtype = SUBTYPE_ADD_CONSTRAINT;

						/* prepare to build primary key list */
						initStringInfo(&pklist);

						if (constr->contype == CONSTR_PRIMARY)
						{
							ListCell * keys;
							elog(DEBUG1, "adding primary key constraint %s", constr->conname);

							olrddl->constraintName = pstrdup(constr->conname);
							appendStringInfo(&pklist, "[");
							foreach(keys, constr->keys)
							{
								char *keycol = strVal(lfirst(keys));
								elog(DEBUG1, "  -> PK Column: %s", keycol);
								appendStringInfo(&pklist, "\"%s\",", keycol);
							}

							if (pklist.len > 1)	/* longer than '[' */
							{
								pklist.data[pklist.len - 1] = '\0';
								pklist.len = pklist.len - 1;
								appendStringInfo(&pklist, "]");
								olrddl->primaryKeyColumnNames = pstrdup(pklist.data);

								/*
								 * OLR delivers the name in reverse letter casing. (all cap to all lower or all lower
								 * to all cap), so for "as is" normalization strategy, we need to reverse it back to
								 * get the original object name. If the name contains a mixture of upper and lower
								 * case letters, it is delivered as is.
								 */
								if (synchdb_letter_casing_strategy == LCS_AS_IS)
								{
									if (isallupper(olrddl->primaryKeyColumnNames))	/* need to normalize to lowercase */
										fc_normalize_name(LCS_NORMALIZE_LOWERCASE, olrddl->primaryKeyColumnNames,strlen(olrddl->primaryKeyColumnNames));
									else if (isalllower(olrddl->primaryKeyColumnNames))
										fc_normalize_name(LCS_NORMALIZE_UPPERCASE, olrddl->primaryKeyColumnNames, strlen(olrddl->primaryKeyColumnNames));
									else
										fc_normalize_name(LCS_AS_IS, olrddl->primaryKeyColumnNames, strlen(olrddl->primaryKeyColumnNames));
								}
								else
									fc_normalize_name(synchdb_letter_casing_strategy, olrddl->primaryKeyColumnNames, strlen(olrddl->primaryKeyColumnNames));

							}
						}
						else
						{
							elog(WARNING, "unsupported constraint type %d", constr->contype);
						}
						break;
					}
					case AT_DropConstraint:
					{
						if (cmd->name)
						{
							elog(DEBUG1, "dropping constraint: %s", cmd->name);
							olrddl->constraintName = pstrdup(cmd->name);

							/*
							 * OLR delivers the name in reverse letter casing. (all cap to all lower or all lower
							 * to all cap), so for "as is" normalization strategy, we need to reverse it back to
							 * get the original object name. If the name contains a mixture of upper and lower
							 * case letters, it is delivered as is.
							 */
							if (synchdb_letter_casing_strategy == LCS_AS_IS)
							{
								if (isallupper(olrddl->constraintName))	/* need to normalize to lowercase */
									fc_normalize_name(LCS_NORMALIZE_LOWERCASE, olrddl->constraintName, strlen(olrddl->constraintName));
								else if (isalllower(olrddl->constraintName))
									fc_normalize_name(LCS_NORMALIZE_UPPERCASE, olrddl->constraintName, strlen(olrddl->constraintName));
								else
									fc_normalize_name(LCS_AS_IS, olrddl->constraintName, strlen(olrddl->constraintName));
							}
							else
								fc_normalize_name(synchdb_letter_casing_strategy, olrddl->constraintName, strlen(olrddl->constraintName));
						}
						olrddl->subtype = SUBTYPE_DROP_CONSTRAINT;
						break;
					}
					case AT_ColumnDefault:
					{
						elog(WARNING, "AT_ColumnDefault not supported yet");
						destroyOLRDDL(olrddl);
						olrddl = NULL;
						goto end;
						break;
					}
					case AT_DropNotNull:
					case AT_SetNotNull:
					{
						elog(WARNING, "AT_SetNotNull or AT_DropNotNull not supported yet");
						destroyOLRDDL(olrddl);
						olrddl = NULL;
						goto end;
						break;
					}
					default:
					{
						elog(WARNING, "Unhandled ALTER subtype: %d", cmd->subtype);
						destroyOLRDDL(olrddl);
						olrddl = NULL;
						goto end;
						break;
					}
				}

				if (ddlcol != NULL)
					olrddl->columns = lappend(olrddl->columns, ddlcol);
			}

			/*
			 * special checking on potential table name mismatch, always use the table name
			 * from the parser
			 */
			if (strcmp(alterStmt->relation->relname, table))
			{
				elog(WARNING, "table name mismatch, using %s instead of %s",
						alterStmt->relation->relname, table);
				pfree(table);
				table = pstrdup(alterStmt->relation->relname);
			}
		}
		else if (IsA(stmt, RenameStmt) &&
				 (((RenameStmt *) stmt)->renameType == OBJECT_TABCONSTRAINT ||
				  (((RenameStmt *) stmt)->renameType == OBJECT_TABLE &&
				   ((RenameStmt *) stmt)->newname &&
				   pg_strncasecmp(((RenameStmt *) stmt)->newname, "BIN$", 4) == 0)))
		{
			/* Recycle-bin renames precede the actual DROP TABLE event.
			 * Keep its original target name until that final event, and
			 * leave independently generated target constraint names alone. */
			*ignored = true;
			destroyOLRDDL(olrddl);
			olrddl = NULL;
			goto end;
		}
		else if (IsA(stmt, DropStmt))
		{
			/* DROP TABLE */
			DropStmt *dropStmt = (DropStmt *) stmt;
			olrddl->type = DDL_DROP_TABLE;

			if (dropStmt->removeType == OBJECT_TABLE)
			{
				ListCell *objCell;
				/* we only expect one entry from this list */
				if (list_length(dropStmt->objects) > 1)
				{
					elog(WARNING, "drop more than 1 table is not supported (%d)",
							list_length(dropStmt->objects));
					destroyOLRDDL(olrddl);
					olrddl = NULL;
					goto end;
				}

				foreach(objCell, dropStmt->objects)
				{
					List *names = (List *) lfirst(objCell);
					char *relname = strVal(llast(names));
					elog(DEBUG1, "DROP TABLE: %s", relname);

					/* replace table with the new value */
					if(table)
					{
						pfree(table);
						table = pstrdup(relname);
					}
				}
			}
			else
				elog(WARNING, "unsupported drop type %d", dropStmt->removeType);
		}
		else
		{
			elog(DEBUG1, "unsupported stmt type: %d ",  nodeTag(stmt));
			destroyOLRDDL(olrddl);
			olrddl = NULL;
			goto end;
		}
	}

	/*
	 * special case for OLR oracle parser. The names that come out of oracle parser
	 * will be reversed in letter casing if they are all upper or lower cases. If
	 * there is a mixed case, the oracle parser will leave it as is:
	 */
	if (table)
	{
		/*
		 * OLR delivers the name in reverse letter casing. (all cap to all lower or all lower
		 * to all cap), so for "as is" normalization strategy, we need to reverse it back to
		 * get the original object name. If the name contains a mixture of upper and lower
		 * case letters, it is delivered as is.
		 */
		if (synchdb_letter_casing_strategy == LCS_AS_IS)
		{
			if (isallupper(table))	/* need to normalize to lowercase */
				fc_normalize_name(LCS_NORMALIZE_LOWERCASE, table, strlen(table));
			else if (isalllower(table))
				fc_normalize_name(LCS_NORMALIZE_UPPERCASE, table, strlen(table));
			else
				fc_normalize_name(LCS_AS_IS, db, strlen(db));
		}
		else
		{
			fc_normalize_name(synchdb_letter_casing_strategy, table, strlen(table));
		}
	}

	fc_normalize_name(synchdb_letter_casing_strategy, db, strlen(db));
	fc_normalize_name(synchdb_letter_casing_strategy, schema, strlen(schema));

	if (db && schema && table)
		olrddl->id = psprintf("%s.%s.%s", db, schema, table);
	else
		olrddl->id = psprintf("%s.%s", db, table);

end:

	return olrddl;
}

/*
 * parseOLRDML
 */
static OLR_DML *
parseOLRDML(Jsonb * jb, char op, Jsonb * payload, orascn * scn, orascn * c_scn, orascn * c_idx, bool isfirst, bool islast)
{
	JsonbValue * v = NULL;
	JsonbValue vbuf;
	Jsonb * jbschema;
	OLR_DML * olrdml = NULL;
	StringInfoData strinfo, objid;
	bool found;
	HTAB * typeidhash;
	SourceColumnCache * namejsonposhash;
	HASHCTL hash_ctl;
	NameOidEntry * entry;
	NameJsonposEntry * entry2;
	Relation rel;
	TupleDesc tupdesc;
	int attnum;
	DataCacheKey cachekey = {0};
	DataCacheEntry * cacheentry;
	Bitmapset * pkattrs;
	char * db = NULL, * schema = NULL, * table = NULL;

	Datum datum_path_schema[1] = {CStringGetTextDatum("schema")};

	/* scn - required */
	v = getKeyJsonValueFromContainer(&jb->root, "scn", strlen("scn"), &vbuf);
	if (!v)
	{
		elog(WARNING, "malformed change request - no scn value");
		return NULL;
	}
	*scn = DatumGetUInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(v->val.numeric)));

	/* commit scn - required */
	v = getKeyJsonValueFromContainer(&jb->root, "c_scn", strlen("c_scn"), &vbuf);
	if (!v)
	{
		elog(WARNING, "malformed change request - no c_scn value");
		return NULL;
	}
	*c_scn = DatumGetUInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(v->val.numeric)));

	/* commit index - required */
	v = getKeyJsonValueFromContainer(&jb->root, "c_idx", strlen("c_idx"), &vbuf);
	if (!v)
	{
		elog(WARNING, "malformed change request - no c_idx value");
		return NULL;
	}
	*c_idx = DatumGetUInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(v->val.numeric)));

	/* db - required */
	v = getKeyJsonValueFromContainer(&jb->root, "db", strlen("db"), &vbuf);
	if (!v)
	{
		elog(WARNING, "malformed change request - no db value");
		return NULL;
	}
	db = pnstrdup(v->val.string.val, v->val.string.len);

	olrdml = palloc0(sizeof(DBZ_DML));
	olrdml->op = op;

	initStringInfo(&objid);
	initStringInfo(&strinfo);
	appendStringInfo(&objid, "%s.", db);

	/* tm - only at first and last record within a batch */
	if (isfirst || islast)
	{
		v = getKeyJsonValueFromContainer(&jb->root, "tm", strlen("tm"), &vbuf);
		if (v)
		{
			olrdml->src_ts_ms = DatumGetUInt64(DirectFunctionCall1(numeric_int8,
					NumericGetDatum(v->val.numeric))) / 1000 / 1000;
		}
	}
	elog(DEBUG1, "scn %llu c_scn %llu db %s op is %c", *scn, *c_scn, db, op);

	/* fetch payload.0.schema */
	jbschema = GET_JSONB_ELEM(payload, &datum_path_schema[0], 1);
	if (!jbschema)
	{
		elog(WARNING, "malformed change request - no payload.0.schema struct");
		destroyOLRDML(olrdml);
		olrdml = NULL;
		goto end;
	}

	/* fetch owner -> considered schema - optional*/
	v = getKeyJsonValueFromContainer(&jbschema->root, "owner", strlen("owner"), &vbuf);
	if (v)
	{
		schema = pnstrdup(v->val.string.val, v->val.string.len);
		appendStringInfo(&objid, "%s.", schema);
	}

	/* fetch payload.0.schema.table - required */
	v = getKeyJsonValueFromContainer(&jbschema->root, "table", strlen("table"), &vbuf);
	if (!v)
	{
		elog(WARNING, "malformed change request - no payload.0.schema.table value");
		destroyOLRDML(olrdml);
		olrdml = NULL;
		goto end;
	}
	table = pnstrdup(v->val.string.val, v->val.string.len);

	if (!is_whitelist_table(table))
	{
		elog(DEBUG1, "table %s is not white-listed", table);
		destroyOLRDML(olrdml);
		olrdml = NULL;
		goto end;
	}

	appendStringInfo(&objid, "%s", table);

	olrdml->remoteObjectId = pstrdup(objid.data);
	/* Resolve the same destination as TRUNCATE and Debezium rows. Splitting
	 * mapping SQL on dots leaves quotes in catalog names and also breaks dots
	 * inside quoted identifiers. The binder returns the actual locked table. */
	rel = synchdb_bind_source_table(db, schema, table, RowExclusiveLock);
	olrdml->tableoid = RelationGetRelid(rel);
	olrdml->schema = get_namespace_name(RelationGetNamespace(rel));
	olrdml->table = pstrdup(RelationGetRelationName(rel));
	olrdml->mappedObjectId = quote_qualified_identifier(olrdml->schema, olrdml->table);
	table_close(rel, NoLock);

	/* prepare cache key */
	strlcpy(cachekey.schema, olrdml->schema, sizeof(cachekey.schema));
	strlcpy(cachekey.table, olrdml->table, sizeof(cachekey.table));

	cacheentry = (DataCacheEntry *) hash_search(dataCacheHash, &cachekey, HASH_ENTER, &found);
	if (found && (cacheentry->tableoid != olrdml->tableoid ||
				  !cacheentry->typeidhash || !cacheentry->namejsonposhash))
	{
		/* The same name may now identify another table, or an earlier failed
		 * lookup may have left no usable schema. Never reuse either cache. */
		if (cacheentry->typeidhash)
			hash_destroy(cacheentry->typeidhash);
		fc_destroy_source_column_cache(cacheentry->namejsonposhash);
		found = false;
	}
	if (found)
	{
		/* use the cached data type hash for lookup later */
		typeidhash = cacheentry->typeidhash;
		olrdml->tableoid = cacheentry->tableoid;
		namejsonposhash = cacheentry->namejsonposhash;
		olrdml->natts = cacheentry->natts;
	}
	else
	{
		cacheentry->typeidhash = NULL;
		cacheentry->namejsonposhash = NULL;
		cacheentry->tableoid = InvalidOid;
		cacheentry->natts = 0;

		/* populate cached information */
		strlcpy(cacheentry->key.schema, olrdml->schema, sizeof(cachekey.schema));
		strlcpy(cacheentry->key.table, olrdml->table, sizeof(cachekey.table));
		cacheentry->tableoid = olrdml->tableoid;

		/* prepare a cached hash table for datatype look up with column name */
		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = NAMEDATALEN;
		hash_ctl.entrysize = sizeof(NameOidEntry);
		hash_ctl.hcxt = TopMemoryContext;

		cacheentry->typeidhash = hash_create("Name to OID Hash Table",
											 512,
											 &hash_ctl,
											 HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

		/* point to the cached datatype hash */
		typeidhash = cacheentry->typeidhash;

		/*
		 * get the column data type IDs for all columns from PostgreSQL catalog
		 * The type IDs are stored in typeidhash temporarily for the parser
		 * below to look up
		 */
		rel = table_open(olrdml->tableoid, AccessShareLock);
		tupdesc = RelationGetDescr(rel);

		/* get primary key bitmapset */
		pkattrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_PRIMARY_KEY);

		/* Only natts is retained; the relation owns its tuple descriptor. */
		olrdml->natts = tupdesc->natts;
		cacheentry->natts = olrdml->natts;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (attr->attisdropped)
				continue;
			entry = (NameOidEntry *) hash_search(typeidhash, NameStr(attr->attname), HASH_ENTER, &found);
			if (!found)
			{
				strlcpy(entry->name, NameStr(attr->attname), NAMEDATALEN);
				entry->oid = attr->atttypid;
				entry->position = attnum;
				entry->typemod = attr->atttypmod;
				if (pkattrs && bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber, pkattrs))
					entry->ispk =true;
				else
					entry->ispk = false;
				get_type_category_preferred(entry->oid, &entry->typcategory, &entry->typispreferred);
				strlcpy(entry->typname, format_type_be(attr->atttypid), NAMEDATALEN);
			}
		}
		bms_free(pkattrs);
		table_close(rel, AccessShareLock);

		/*
		 * build another hash to store json value's locations of schema data for correct additional param lookups
		 * todo: combine this hash with typeidhash above to save one hash
		 */
		PG_TRY();
		{
			cacheentry->namejsonposhash = build_olr_schema_jsonpos_hash(jbschema);
		}
		PG_CATCH();
		{
			/* Never reuse a half-built, session-lived schema cache after ERROR. */
			hash_destroy(typeidhash);
			(void) hash_search(dataCacheHash, &cachekey, HASH_REMOVE, NULL);
			PG_RE_THROW();
		}
		PG_END_TRY();
		namejsonposhash = cacheentry->namejsonposhash;
		if (!namejsonposhash)
		{
			/* dump the JSON change event as additional detail if available */
			if (synchdb_log_event_on_error && g_eventStr != NULL)
				elog(LOG, "%s", g_eventStr);

			elog(ERROR, "cannot parse columns section of OLR change event JSON. Abort");
		}
	}
	switch (olrdml->op)
	{
		case 'c':
		{
			/* parse after */
			Jsonb * dmldata = NULL;
			JsonbIterator *it;
			JsonbValue v;
			JsonbIteratorToken r;
			char * key = NULL;
			char * value = NULL;
			bool value_is_null = false;
			DBZ_DML_COLUMN_VALUE * colval = NULL;
			Datum datum_elems[1] = {CStringGetTextDatum("after")};

			dmldata = GET_JSONB_ELEM(payload, &datum_elems[0], 1);
			if (dmldata)
			{
				int pause = 0;
				it = JsonbIteratorInit(&dmldata->root);
				while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
				{
					switch (r)
					{
						case WJB_BEGIN_OBJECT:
							if (key != NULL)
							{
								pause = 1;
							}
							break;
						case WJB_END_OBJECT:
							if (pause)
							{
								pause = 0;
								if (key)
								{
									int pathsize = strlen("after.") + strlen(key) + 1;
									char * tmpPath = (char *) palloc0 (pathsize);
									snprintf(tmpPath, pathsize, "after.%s", key);
									if (getPathElementString(payload, tmpPath, &strinfo, false) == 0)
										value = pstrdup(strinfo.data);
									if(tmpPath)
										pfree(tmpPath);
								}
							}
							break;
						case WJB_BEGIN_ARRAY:
							if (key)
							{
								pfree(key);
								key = NULL;
							}
							break;
						case WJB_END_ARRAY:
							break;
						case WJB_KEY:
							if (pause)
								break;

							key = pnstrdup(v.val.string.val, v.val.string.len);
							value_is_null = false;
							break;
						case WJB_VALUE:
						case WJB_ELEM:
							if (pause)
								break;
							value_is_null = v.type == jbvNull;
							switch (v.type)
							{
								case jbvNull:
									value = pstrdup("NULL");
									break;
								case jbvString:
									value = pnstrdup(v.val.string.val, v.val.string.len);
									break;
								case jbvNumeric:
									value = DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v.val.numeric)));
									break;
								case jbvBool:
									if (v.val.boolean)
										value = pstrdup("true");
									else
										value = pstrdup("false");
									break;
								case jbvBinary:
									value = pstrdup("NULL");
									break;
								default:
									value = pstrdup("NULL");
									break;
							}
						break;
						default:
							break;
					}

					/* check if we have a key - value pair */
					if (key != NULL && value != NULL)
					{
						char * mappedColumnName = NULL;

						StringInfoData colNameObjId;

						colval = (DBZ_DML_COLUMN_VALUE *) palloc0(sizeof(DBZ_DML_COLUMN_VALUE));
						colval->name = pstrdup(key);

						/* a copy of original column name for expression rule lookup at later stage */
						colval->remoteColumnName = pstrdup(colval->name);

						fc_normalize_name(synchdb_letter_casing_strategy, colval->name, strlen(colval->name));
						colval->is_null = value_is_null;
						colval->value = value_is_null ? NULL : pstrdup(value);

						/* transform the column name if needed */
						initStringInfo(&colNameObjId);
						appendStringInfo(&colNameObjId, "%s.%s", objid.data, colval->remoteColumnName);
						mappedColumnName = transform_object_name(colNameObjId.data, "column");
						if (mappedColumnName)
						{
							/* replace the column name with looked up value here */
							pfree(colval->name);
							colval->name = pstrdup(mappedColumnName);
						}
						if (colNameObjId.data)
							pfree(colNameObjId.data);

						/* look up its data type */
						if (strlen(colval->name) >= NAMEDATALEN)
							ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG),
										errmsg("target column name exceeds PostgreSQL's identifier limit")));
						entry = (NameOidEntry *) hash_search(typeidhash, colval->name, HASH_FIND, &found);
						if (found)
						{
							colval->datatype = entry->oid;
							colval->position = entry->position;
							colval->typemod = entry->typemod;
							colval->ispk = entry->ispk;
							colval->typcategory = entry->typcategory;
							colval->typispreferred = entry->typispreferred;
							colval->typname = pstrdup(entry->typname);
						}
						else
							elog(ERROR, "cannot find data type for column %s. Non-existent column?", colval->name);

						entry2 = fc_source_column_lookup(namejsonposhash, colval->remoteColumnName);

						if (entry2)
						{
							colval->dbztype = entry2->dbztype;
							colval->timerep = entry2->timerep;
							colval->scale = entry2->scale;
						}
						else
						{
							fc_invalidate_data_cache(&cachekey);
							elog(ERROR, "cannot find olr json column schema data for column %s(%s). invalid json event?",
									colval->name, colval->remoteColumnName);
						}


						olrdml->columnValuesAfter = lappend(olrdml->columnValuesAfter, colval);
						pfree(key);
						pfree(value);
						key = NULL;
						value = NULL;
					}
				}
			}
			break;
		}
		case 'u':
		{
			/* parse before + after */
			Jsonb * dmldata = NULL;
			JsonbIterator *it;
			JsonbValue v;
			JsonbIteratorToken r;
			char * key = NULL;
			char * value = NULL;
			bool value_is_null = false;
			DBZ_DML_COLUMN_VALUE * colval = NULL;
			Datum datum_elems_before[1] = {CStringGetTextDatum("before")};
			Datum datum_elems_after[1] = {CStringGetTextDatum("after")};
			int i = 0;

			for (i = 0; i < 2; i++)
			{
				if (i == 0)
					dmldata = GET_JSONB_ELEM(payload, &datum_elems_before[0], 1);
				else
					dmldata = GET_JSONB_ELEM(payload, &datum_elems_after[0], 1);
				if (dmldata)
				{
					int pause = 0;
					it = JsonbIteratorInit(&dmldata->root);
					while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
					{
						switch (r)
						{
							case WJB_BEGIN_OBJECT:
								if (key != NULL)
								{
									pause = 1;
								}
								break;
							case WJB_END_OBJECT:
								if (pause)
								{
									pause = 0;
									if (key)
									{
										int pathsize = (i == 0 ? strlen("before.") + strlen(key) + 1 :
												strlen("after.") + strlen(key) + 1 );
										char * tmpPath = (char *) palloc0 (pathsize);
										if (i == 0)
											snprintf(tmpPath, pathsize, "before.%s", key);
										else
											snprintf(tmpPath, pathsize, "after.%s", key);
										if (getPathElementString(payload, tmpPath, &strinfo, false) == 0)
											value = pstrdup(strinfo.data);
										if(tmpPath)
											pfree(tmpPath);
									}
								}
								break;
							case WJB_BEGIN_ARRAY:
								if (key)
								{
									pfree(key);
									key = NULL;
								}
								break;
							case WJB_END_ARRAY:
								break;
							case WJB_KEY:
								if (pause)
									break;

								key = pnstrdup(v.val.string.val, v.val.string.len);
							value_is_null = false;
								break;
							case WJB_VALUE:
							case WJB_ELEM:
								if (pause)
									break;
								value_is_null = v.type == jbvNull;
							switch (v.type)
								{
									case jbvNull:
										value = pstrdup("NULL");
										break;
									case jbvString:
										value = pnstrdup(v.val.string.val, v.val.string.len);
										break;
									case jbvNumeric:
										value = DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v.val.numeric)));
										break;
									case jbvBool:
										if (v.val.boolean)
											value = pstrdup("true");
										else
											value = pstrdup("false");
										break;
									case jbvBinary:
										value = pstrdup("NULL");
										break;
									default:
										value = pstrdup("NULL");
										break;
								}
							break;
							default:
								break;
						}

						/* check if we have a key - value pair */
						if (key != NULL && value != NULL)
						{
							char * mappedColumnName = NULL;

							StringInfoData colNameObjId;

							colval = (DBZ_DML_COLUMN_VALUE *) palloc0(sizeof(DBZ_DML_COLUMN_VALUE));
							colval->name = pstrdup(key);

							/* a copy of original column name for expression rule lookup at later stage */
							colval->remoteColumnName = pstrdup(colval->name);

							fc_normalize_name(synchdb_letter_casing_strategy, colval->name, strlen(colval->name));
							colval->is_null = value_is_null;
							colval->value = value_is_null ? NULL : pstrdup(value);

							/* transform the column name if needed */
							initStringInfo(&colNameObjId);
							appendStringInfo(&colNameObjId, "%s.%s", objid.data, colval->remoteColumnName);
							mappedColumnName = transform_object_name(colNameObjId.data, "column");
							if (mappedColumnName)
							{
								/* replace the column name with looked up value here */
								pfree(colval->name);
								colval->name = pstrdup(mappedColumnName);
							}
							if (colNameObjId.data)
								pfree(colNameObjId.data);

							/* look up its data type */
							if (strlen(colval->name) >= NAMEDATALEN)
							ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG),
										errmsg("target column name exceeds PostgreSQL's identifier limit")));
						entry = (NameOidEntry *) hash_search(typeidhash, colval->name, HASH_FIND, &found);
							if (found)
							{
								colval->datatype = entry->oid;
								colval->position = entry->position;
								colval->typemod = entry->typemod;
								colval->ispk = entry->ispk;
								colval->typcategory = entry->typcategory;
								colval->typispreferred = entry->typispreferred;
								colval->typname = pstrdup(entry->typname);
							}
							else
								elog(ERROR, "cannot find data type for column %s. Non-existent column?", colval->name);

							entry2 = fc_source_column_lookup(namejsonposhash, colval->remoteColumnName);

							if (entry2)
							{
								colval->dbztype = entry2->dbztype;
								colval->timerep = entry2->timerep;
								colval->scale = entry2->scale;
							}
							else
							{
								fc_invalidate_data_cache(&cachekey);
								elog(ERROR, "cannot find olr json column schema data for column %s(%s). invalid json event?",
										colval->name, colval->remoteColumnName);
							}


							if (i == 0)
								olrdml->columnValuesBefore = lappend(olrdml->columnValuesBefore, colval);
							else
								olrdml->columnValuesAfter = lappend(olrdml->columnValuesAfter, colval);

							pfree(key);
							pfree(value);
							key = NULL;
							value = NULL;
						}
					}
				}
			}
			break;
		}
		case 'd':
		{
			/* parse after */
			Jsonb * dmldata = NULL;
			JsonbIterator *it;
			JsonbValue v;
			JsonbIteratorToken r;
			char * key = NULL;
			char * value = NULL;
			bool value_is_null = false;
			DBZ_DML_COLUMN_VALUE * colval = NULL;
			Datum datum_elems[1] = {CStringGetTextDatum("before")};

			dmldata = GET_JSONB_ELEM(payload, &datum_elems[0], 1);
			if (dmldata)
			{
				int pause = 0;
				it = JsonbIteratorInit(&dmldata->root);
				while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
				{
					switch (r)
					{
						case WJB_BEGIN_OBJECT:
							if (key != NULL)
							{
								pause = 1;
							}
							break;
						case WJB_END_OBJECT:
							if (pause)
							{
								pause = 0;
								if (key)
								{
									int pathsize = strlen("before.") + strlen(key) + 1;
									char * tmpPath = (char *) palloc0 (pathsize);
									snprintf(tmpPath, pathsize, "before.%s", key);
									if (getPathElementString(payload, tmpPath, &strinfo, false))
										value = pstrdup(strinfo.data);
									if(tmpPath)
										pfree(tmpPath);
								}
							}
							break;
						case WJB_BEGIN_ARRAY:
							if (key)
							{
								pfree(key);
								key = NULL;
							}
							break;
						case WJB_END_ARRAY:
							break;
						case WJB_KEY:
							if (pause)
								break;

							key = pnstrdup(v.val.string.val, v.val.string.len);
							value_is_null = false;
							break;
						case WJB_VALUE:
						case WJB_ELEM:
							if (pause)
								break;
							value_is_null = v.type == jbvNull;
							switch (v.type)
							{
								case jbvNull:
									value = pstrdup("NULL");
									break;
								case jbvString:
									value = pnstrdup(v.val.string.val, v.val.string.len);
									break;
								case jbvNumeric:
									value = DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v.val.numeric)));
									break;
								case jbvBool:
									if (v.val.boolean)
										value = pstrdup("true");
									else
										value = pstrdup("false");
									break;
								case jbvBinary:
									value = pstrdup("NULL");
									break;
								default:
									value = pstrdup("NULL");
									break;
							}
						break;
						default:
							break;
					}

					/* check if we have a key - value pair */
					if (key != NULL && value != NULL)
					{
						char * mappedColumnName = NULL;

						StringInfoData colNameObjId;

						colval = (DBZ_DML_COLUMN_VALUE *) palloc0(sizeof(DBZ_DML_COLUMN_VALUE));
						colval->name = pstrdup(key);

						/* a copy of original column name for expression rule lookup at later stage */
						colval->remoteColumnName = pstrdup(colval->name);

						fc_normalize_name(synchdb_letter_casing_strategy, colval->name, strlen(colval->name));
						colval->is_null = value_is_null;
						colval->value = value_is_null ? NULL : pstrdup(value);

						/* transform the column name if needed */
						initStringInfo(&colNameObjId);
						appendStringInfo(&colNameObjId, "%s.%s", objid.data, colval->remoteColumnName);
						mappedColumnName = transform_object_name(colNameObjId.data, "column");
						if (mappedColumnName)
						{
							/* replace the column name with looked up value here */
							pfree(colval->name);
							colval->name = pstrdup(mappedColumnName);
						}
						if (colNameObjId.data)
							pfree(colNameObjId.data);

						/* look up its data type */
						if (strlen(colval->name) >= NAMEDATALEN)
							ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG),
										errmsg("target column name exceeds PostgreSQL's identifier limit")));
						entry = (NameOidEntry *) hash_search(typeidhash, colval->name, HASH_FIND, &found);
						if (found)
						{
							colval->datatype = entry->oid;
							colval->position = entry->position;
							colval->typemod = entry->typemod;
							colval->ispk = entry->ispk;
							colval->typcategory = entry->typcategory;
							colval->typispreferred = entry->typispreferred;
							colval->typname = pstrdup(entry->typname);
						}
						else
							elog(ERROR, "cannot find data type for column %s. Non-existent column?", colval->name);

						entry2 = fc_source_column_lookup(namejsonposhash, colval->remoteColumnName);

						if (entry2)
						{
							colval->dbztype = entry2->dbztype;
							colval->timerep = entry2->timerep;
							colval->scale = entry2->scale;
						}
						else
						{
							fc_invalidate_data_cache(&cachekey);
							elog(ERROR, "cannot find olr json column schema data for column %s(%s). invalid json event?",
									colval->name, colval->remoteColumnName);
						}


						olrdml->columnValuesBefore = lappend(olrdml->columnValuesBefore, colval);
						pfree(key);
						pfree(value);
						key = NULL;
						value = NULL;
					}
				}
			}
			break;
		}
		default:
			break;
	}
	/*
	 * finally, we need to sort dbzdml->columnValuesBefore and dbzdml->columnValuesAfter
	 * based on position to align with PostgreSQL's attnum
	 */
	if (olrdml->columnValuesBefore != NULL)
		list_sort(olrdml->columnValuesBefore, list_sort_cmp);

	if (olrdml->columnValuesAfter != NULL)
		list_sort(olrdml->columnValuesAfter, list_sort_cmp);

end:
	if (strinfo.data)
		pfree(strinfo.data);

	if (objid.data)
		pfree(objid.data);

	return olrdml;
}

/*
 * fc_processOLRChangeEvent
 *
 * Main function to process Openlog Replicator change event
 */
int
fc_processOLRChangeEvent(void * event, SynchdbStatistics * myBatchStats,
		const char * name, bool * sendconfirm, bool isfirst, bool islast)
{
	Datum jsonb_datum;
	Jsonb * jb = NULL;
	Jsonb * payload = NULL;
	JsonbValue * v = NULL;
	JsonbValue vbuf;
	char * op = NULL;
	int ret = -1;
	MemoryContext tempContext, oldContext;
	struct timeval tv;

	Datum datum_path_payload[2] = {CStringGetTextDatum("payload"), CStringGetTextDatum("0")};

	/* Invalidation callbacks only mark an epoch. Rebuild borrowed metadata in
	 * this normal entry point, including after transaction/subtransaction abort. */
	fc_ensureDataCache();

	tempContext = AllocSetContextCreate(TopMemoryContext,
										"FORMAT_CONVERTER",
										ALLOCSET_DEFAULT_SIZES);

	oldContext = MemoryContextSwitchTo(tempContext);

    /* Convert event string to JSONB */
	PG_TRY();
	{
#if SYNCHDB_PG_MAJOR_VERSION >= 1700
		jsonb_datum = jsonb_from_text((text *) event, false);
#else
	    jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum((char *) event));
#endif
	    jb = DatumGetJsonbP(jsonb_datum);
	}
	PG_CATCH();
	{
		FlushErrorState();
#if SYNCHDB_PG_MAJOR_VERSION >= 1700
		elog(DEBUG1, "bad json message: %s", text_to_cstring(event));
#else
		elog(DEBUG1, "bad json message: %s", (char *) event);
#endif
		increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
		MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(tempContext);
		return -1;
	}
	PG_END_TRY();
	if (!olr_handoff_apply_record(jb))
	{
		MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(tempContext);
		return 0;
	}

#if SYNCHDB_PG_MAJOR_VERSION >= 1700
	elog(DEBUG1, "%s", text_to_cstring(event));
#else
	elog(DEBUG1, "%s", (char *) event);
#endif

	/* payload - required */
	payload = GET_JSONB_ELEM(jb, &datum_path_payload[0], 2);
	if (!payload)
	{
		elog(WARNING, "malformed change request - no payload struct");
		increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
		MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(tempContext);
		return -1;
	}

	/* payload.op - required */
	v = getKeyJsonValueFromContainer(&payload->root, "op", strlen("db"), &vbuf);
	if (!v)
	{
		elog(WARNING, "malformed change request - no payload.0.op value");
		increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
		MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(tempContext);
		return -1;
	}
	op = pnstrdup(v->val.string.val, v->val.string.len);

	/* update stage if needed */
	if (get_shm_connector_stage_enum(myConnectorId) != STAGE_CHANGE_DATA_CAPTURE)
		set_shm_connector_stage(myConnectorId, STAGE_CHANGE_DATA_CAPTURE);

	elog(DEBUG1, "op is %s", op);
	if (!strcasecmp(op, "begin") || !strcasecmp(op, "commit"))
	{
		orascn scn = 0, c_scn = 0, c_idx = 0;

		/* scn - required */
		v = getKeyJsonValueFromContainer(&jb->root, "scn", strlen("scn"), &vbuf);
		if (!v)
		{
			elog(WARNING, "malformed change request - no scn value");
			increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
			MemoryContextSwitchTo(oldContext);
			MemoryContextDelete(tempContext);
			return -1;
		}
		scn = DatumGetUInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(v->val.numeric)));

		/* commit scn - required */
		v = getKeyJsonValueFromContainer(&jb->root, "c_scn", strlen("c_scn"), &vbuf);
		if (!v)
		{
			elog(WARNING, "malformed change request - no c_scn value");
			increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
			MemoryContextSwitchTo(oldContext);
			MemoryContextDelete(tempContext);
			return -1;
		}
		c_scn = DatumGetUInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(v->val.numeric)));

		/* commit index - required */
		v = getKeyJsonValueFromContainer(&jb->root, "c_idx", strlen("c_idx"), &vbuf);
		if (!v)
		{
			elog(WARNING, "malformed change request - no c_idx value");
			increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
			MemoryContextSwitchTo(oldContext);
			MemoryContextDelete(tempContext);
			return -1;
		}
		c_idx = DatumGetUInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(v->val.numeric)));

		/* transaction boundary */
		if (!strcasecmp(op, "begin"))
		{
			/* todo */
			increment_connector_statistics(myBatchStats, STATS_TX, 1);
		}
		else if (!strcasecmp(op, "commit"))
		{
			/* todo */
			increment_connector_statistics(myBatchStats, STATS_TX, 1);
		}
		set_shm_connector_state(myConnectorId, STATE_SYNCING);

		/* tm - only at first and last record within a batch */
		/* update processing timestamps */
    	if (islast)
    	{
			v = getKeyJsonValueFromContainer(&jb->root, "tm", strlen("tm"), &vbuf);
			if (v)
			{
				myBatchStats->genstats.stats_last_src_ts = DatumGetUInt64(DirectFunctionCall1(numeric_int8,
						NumericGetDatum(v->val.numeric))) / 1000 / 1000;
			}
			gettimeofday(&tv, NULL);
			myBatchStats->genstats.stats_last_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

    	if (isfirst)
    	{
			v = getKeyJsonValueFromContainer(&jb->root, "tm", strlen("tm"), &vbuf);
			if (v)
			{
				myBatchStats->genstats.stats_first_src_ts = DatumGetUInt64(DirectFunctionCall1(numeric_int8,
						NumericGetDatum(v->val.numeric))) / 1000 / 1000;
			}
			gettimeofday(&tv, NULL);
			myBatchStats->genstats.stats_first_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

    	olr_client_set_scns(scn, c_scn, c_idx);
    	*sendconfirm = true;
	}
	else if (!strcasecmp(op, "c") || !strcasecmp(op, "u") || !strcasecmp(op, "d"))
	{
		/* DMLs */
		OLR_DML * olrdml = NULL;
		PG_DML * pgdml = NULL;
		orascn scn = 0, c_scn = 0, c_idx = 0;

		/* increment batch statistics */
		increment_connector_statistics(myBatchStats, STATS_DML, 1);

		if (!IsTransactionState())
		{
			elog(WARNING, "not in transaction state. Skip change events");
			MemoryContextSwitchTo(oldContext);
			MemoryContextDelete(tempContext);
			return -1;
		}

		/* (1) parse */
		set_shm_connector_state(myConnectorId, STATE_PARSING);
		olrdml = parseOLRDML(jb, op[0], payload, &scn, &c_scn, &c_idx, isfirst, islast);
    	if (!olrdml)
		{
			set_shm_connector_state(myConnectorId, STATE_SYNCING);
			increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
			MemoryContextSwitchTo(oldContext);
			MemoryContextDelete(tempContext);
			return -1;
		}

    	/* (2) convert */
    	set_shm_connector_state(myConnectorId, STATE_CONVERTING);
    	pgdml = convert2PGDML(olrdml, TYPE_OLR);
    	if (!pgdml)
    	{
    		set_shm_connector_state(myConnectorId, STATE_SYNCING);
    		increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
    		destroyOLRDML(olrdml);
    		MemoryContextSwitchTo(oldContext);
    		MemoryContextDelete(tempContext);
    		return -1;
    	}

    	/* (3) execute */
    	set_shm_connector_state(myConnectorId, STATE_EXECUTING);
    	ret = ra_executePGDML(pgdml, TYPE_OLR, myBatchStats, false);
    	if(ret)
    	{
    		set_shm_connector_state(myConnectorId, STATE_SYNCING);
    		increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
        	destroyOLRDML(olrdml);
        	destroyPGDML(pgdml);
        	MemoryContextSwitchTo(oldContext);
        	MemoryContextDelete(tempContext);
    		return -1;
    	}

    	/* (4) record scn, c_scn and processing timestamps */
    	olr_client_set_scns(scn, c_scn, c_idx);
    	* sendconfirm = true;

    	if (islast)
    	{
			myBatchStats->genstats.stats_last_src_ts = olrdml->src_ts_ms;
			gettimeofday(&tv, NULL);
			myBatchStats->genstats.stats_last_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

    	if (isfirst)
    	{
			myBatchStats->genstats.stats_first_src_ts = olrdml->src_ts_ms;
			gettimeofday(&tv, NULL);
			myBatchStats->genstats.stats_first_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

       	/* (5) clean up */
    	set_shm_connector_state(myConnectorId, STATE_SYNCING);
    	destroyOLRDML(olrdml);
    	destroyPGDML(pgdml);
	}
	else if (!strcasecmp(op, "ddl"))
	{
		/* DDLs */
		OLR_DDL * olrddl = NULL;
		PG_DDL * pgddl = NULL;
		bool ignored = false;
		orascn scn = 0, c_scn = 0, c_idx = 0;


		/* (2) parse */
		set_shm_connector_state(myConnectorId, STATE_PARSING);
		olrddl = parseOLRDDL(jb, payload, &scn, &c_scn, &c_idx, isfirst, islast, &ignored);
		if (!olrddl)
		{
			set_shm_connector_state(myConnectorId, STATE_SYNCING);
			if (!ignored)
				increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);

			/* Only explicitly excluded non-table DDL may advance without
			 * applying a target change. A table parser failure is fatal. */
			MemoryContextSwitchTo(oldContext);
			MemoryContextDelete(tempContext);
			return ignored ? 0 : -1;
		}

    	/* (3) convert */
    	set_shm_connector_state(myConnectorId, STATE_CONVERTING);
		if (olrddl->type == DDL_TRUNCATE_TABLE)
		{
			Relation relation = synchdb_bind_source_table(olrddl->source_database,
					olrddl->source_schema, olrddl->source_table, AccessExclusiveLock);

			set_shm_connector_state(myConnectorId, STATE_EXECUTING);
			synchdb_execute_truncate(relation);
			increment_connector_statistics(myBatchStats, STATS_TRUNCATE, 1);
			goto ddl_applied;
		}
		pgddl = convert2PGDDL(olrddl, TYPE_OLR);
    	if (!pgddl)
    	{
    		set_shm_connector_state(myConnectorId, STATE_SYNCING);
    		increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
    		destroyOLRDDL(olrddl);
    		MemoryContextSwitchTo(oldContext);
    		MemoryContextDelete(tempContext);
    		return -1;
    	}

    	/* (4) execute */
    	set_shm_connector_state(myConnectorId, STATE_EXECUTING);
    	ret = ra_executePGDDL(pgddl, TYPE_OLR);
    	if(ret)
    	{
    		set_shm_connector_state(myConnectorId, STATE_SYNCING);
    		increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
    		destroyOLRDDL(olrddl);
    		destroyPGDDL(pgddl);
    		MemoryContextSwitchTo(oldContext);
    		MemoryContextDelete(tempContext);
    		return -1;
    	}

	ddl_applied:
		increment_connector_statistics(myBatchStats, STATS_DDL, 1);
		/* (5) record scn, c_scn and processing timestamps */
    	olr_client_set_scns(scn, c_scn, c_idx);
    	* sendconfirm = true;

    	if (islast)
    	{
			myBatchStats->genstats.stats_last_src_ts = olrddl->src_ts_ms;
			gettimeofday(&tv, NULL);
			myBatchStats->genstats.stats_last_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

    	if (isfirst)
    	{
			myBatchStats->genstats.stats_first_src_ts = olrddl->src_ts_ms;
			gettimeofday(&tv, NULL);
			myBatchStats->genstats.stats_first_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

		/* (6) update attribute map table */
		if (pgddl != NULL)
			updateSynchdbAttribute(olrddl, pgddl, TYPE_OLR, name);

    	/* (7) clean up */
    	set_shm_connector_state(myConnectorId, STATE_SYNCING);
		destroyOLRDDL(olrddl);
		destroyPGDDL(pgddl);
	}
	else
	{
		elog(WARNING, "unsupported op %s", op);
	}

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(tempContext);
	return 0;
}

void
unload_oracle_parser(void)
{
	if (handle != NULL)
		dlclose(handle);
	handle = NULL;
	synchdb_oracle_raw_parser = NULL;
}
