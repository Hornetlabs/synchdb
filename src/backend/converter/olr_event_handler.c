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
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "parser/parser.h"
#include "mb/pg_wchar.h"
#include "nodes/parsenodes.h"
#include "synchdb/synchdb.h"
#include <time.h>
#include <sys/time.h>
#include <dlfcn.h>

#include "converter/olr_event_handler.h"
#include "converter/format_converter.h"
#include "olr/olr_client.h"

/* extern globals */
extern int myConnectorId;
extern bool synchdb_log_event_on_error;
extern char * g_eventStr;
extern HTAB * dataCacheHash;
extern char * dbz_skipped_operations;

/* Oracle raw parser function prototype */
typedef List * (*oracle_raw_parser_fn)(const char *str, RawParseMode mode);
static oracle_raw_parser_fn oracle_raw_parser = NULL;
static void * handle = NULL;

static char * strtoupper(const char *input);
static OlrType getOlrTypeFromString(const char * typestring);
static void strip_after_column_def(StringInfoData *sql);
static bool is_whitelist_sql(StringInfoData * sql);
static HTAB * build_olr_schema_jsonpos_hash(Jsonb * jb);
static void destroyOLRDDL(OLR_DDL * ddlinfo);
static void destroyOLRDML(OLR_DML * dmlinfo);
static OLR_DDL * parseOLRDDL(Jsonb * jb, Jsonb * payload, orascn * scn,
		orascn * c_scn, bool isfirst, bool islast);
static OLR_DML * parseOLRDML(Jsonb * jb, char op, Jsonb * payload,
		orascn * scn, orascn * c_scn, bool isfirst, bool islast);


static char *
strtoupper(const char *input)
{
    char *upper = pstrdup(input);
    for (char *p = upper; *p; ++p)
    	*p = (char) pg_toupper((unsigned char) *p);
    return upper;
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

void
strip_after_column_def(StringInfoData *sql)
{
	int paren_level = 0;
	int i;

	if (!sql || !sql->data)
		return;

	for (i = 0; i < sql->len; ++i)
	{
		if (sql->data[i] == '(')
		{
			paren_level++;
		}
		else if (sql->data[i] == ')')
		{
			paren_level--;
			if (paren_level == 0)
			{
				i++;  // include the closing ')'
				break;
			}
		}
	}
	/* Truncate after position i */
	sql->data[i] = '\0';
	sql->len = i;
}

/*
 * is_whitelist_sql
 *
 * this function does a simple filtering and normalization on the input DDL
 * statement and returns true if SQL is good to proceed, or false if otherwise.
 * This is kind of rough and may need to add more filtering as more tests are
 * conducted. This filter is needed because the oracle parser does not support
 * every single oracle syntax.
 */
static bool
is_whitelist_sql(StringInfoData * sql)
{
	char * sqlupper = strtoupper(sql->data);
    bool allowed = false;

    /* special case for DROP TABLE xxx AS yyy (internal oracle sql) */
    if (strstr(sqlupper, "DROP") && strstr(sqlupper, "TABLE") &&
		strstr(sqlupper, "AS"))
    {
    	/*
    	 * Oracle's recycling mode (if enabled) will send query like:
    	 * "DROP TABLE x AS y", which is internal Oracle mechanism and so we
    	 * need to truncate everything after 'AS' if this keyword exists.
    	 */
        const char *as_pos = strcasestr(sql->data, "AS");
		if (as_pos != NULL)
		{
			int new_len = as_pos - sql->data;
			sql->data[new_len] = '\0';
			sql->len = new_len;
		}
		allowed = true;
    }

    /* special case for CREATE TABLE xxx (...) INITRANS 4 PCTFREE 10 ... */
    if (strstr(sqlupper, "CREATE") && strstr(sqlupper, "TABLE"))
    {
    	/* we want to strip all of the post-column options */
    	strip_after_column_def(sql);
    	elog(DEBUG1, "sql after stripping = %s", sql->data);
    }
    /*
     * we are only interested in the following DDL statements though Some of
     * these may not be parsed successfully by oracle parser. This is okay as
     * we will just ignore them and ensure the ones we are interested can be
     * parsed
     */
    if (strstr(sqlupper, "CREATE") && strstr(sqlupper, "TABLE"))
    	allowed = true;

    if (strstr(sqlupper, "DROP") && strstr(sqlupper, "TABLE"))
    	allowed = true;

    if (strstr(sqlupper, "ALTER") && strstr(sqlupper, "TABLE"))
    	allowed = true;

    if (strstr(sqlupper, "TRUNCATE") && strstr(sqlupper, "TABLE"))
    	allowed = true;

    pfree(sqlupper);
    return allowed;
}

static HTAB *
build_olr_schema_jsonpos_hash(Jsonb * jb)
{
	HTAB * jsonposhash;
	HASHCTL hash_ctl;
	Jsonb * schemadata = NULL;
	int jsonpos = 0;
	NameJsonposEntry * entry;
	NameJsonposEntry tmprecord = {0};
	bool found = false;
	int i = 0, j = 0;
	unsigned int contsize = 0;
	Datum datum_elems[1] ={CStringGetTextDatum("columns")};
	bool isnull;

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = NAMEDATALEN;
	hash_ctl.entrysize = sizeof(NameJsonposEntry);
	hash_ctl.hcxt = TopMemoryContext;

	jsonposhash = hash_create("Name to jsonpos Hash Table",
							512,
							&hash_ctl,
							HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
	/* fixme - can crash if jsonb_get_element cannot find the element */
	schemadata = DatumGetJsonbP(jsonb_get_element(jb, &datum_elems[0], 1, &isnull, false));
	if (schemadata)
	{
		contsize = JsonContainerSize(&schemadata->root);
		for (i = 0; i < contsize; i++)
		{
			JsonbValue * v = NULL, * v2 = NULL;
			JsonbValue vbuf;
			char * tmpstr = NULL;

			memset(&tmprecord, 0, sizeof(NameJsonposEntry));
			v = getIthJsonbValueFromContainer(&schemadata->root, i);
			if (v->type == jbvBinary)
			{
				v2 = getKeyJsonValueFromContainer(v->val.binary.data, "name", strlen("name"), &vbuf);
				if (v2)
				{
					strncpy(tmprecord.name, v2->val.string.val, v2->val.string.len); /* todo check overflow */
					for (j = 0; j < strlen(tmprecord.name); j++)
						tmprecord.name[j] = (char) pg_tolower((unsigned char) tmprecord.name[j]);
				}
				else
				{
					elog(WARNING, "name is missing from olr column array...");
					continue;
				}
				v2 = getKeyJsonValueFromContainer(v->val.binary.data, "type", strlen("type"), &vbuf);
				if (v2)
				{
					tmpstr = pnstrdup(v2->val.string.val, v2->val.string.len);
					tmprecord.dbztype = getOlrTypeFromString(tmpstr);
					pfree(tmpstr);
				}
				else
				{
					elog(WARNING, "type is missing from olr column array...");
					continue;
				}
				v2 = getKeyJsonValueFromContainer(v->val.binary.data, "scale", strlen("scale"), &vbuf);
				if (v2)
				{
					tmpstr = pnstrdup(v2->val.string.val, v2->val.string.len);
					tmprecord.scale = atoi(tmpstr);
					pfree(tmpstr);
				}

				/* tmprecord.timerep not given from OLR - data processor needs to figure out itself */

				tmprecord.jsonpos = jsonpos;
				jsonpos++;
			}
			else
			{
				elog(WARNING, "unexpected container type %d", v->type);
				continue;
			}

			entry = (NameJsonposEntry *) hash_search(jsonposhash, tmprecord.name, HASH_ENTER, &found);
			if (!found)
			{
				strlcpy(entry->name, tmprecord.name, NAMEDATALEN);
				entry->jsonpos = tmprecord.jsonpos;
				entry->dbztype = tmprecord.dbztype;
				entry->timerep = tmprecord.timerep;
				entry->scale = tmprecord.scale;
				elog(DEBUG1, "new jsonpos entry name=%s pos=%d dbztype=%d timerep=%d scale=%d",
						entry->name, entry->jsonpos, entry->dbztype, entry->timerep, entry->scale);
			}
		}

	}
	return jsonposhash;
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

/*
 * parseOLRDDL
 */
static OLR_DDL *
parseOLRDDL(Jsonb * jb, Jsonb * payload, orascn * scn, orascn * c_scn, bool isfirst, bool islast)
{
	JsonbValue * v = NULL;
	JsonbValue vbuf;
	Jsonb * jbschema;
	OLR_DDL * olrddl = NULL;
	OLR_DDL_COLUMN * ddlcol = NULL;
	bool isnull = false;
	Datum datum_path_schema[1] = {CStringGetTextDatum("schema")};
	char * db = NULL, * schema = NULL, * table = NULL;
	StringInfoData sql;
	List * ptree = NULL;
	ListCell * cell;
	int j = 0;

	/* scn - required */
	v = getKeyJsonValueFromContainer(&jb->root, "scn", strlen("scn"), &vbuf);
	if (!v)
	{
		elog(WARNING, "malformed change request - no scn value");
		goto end;
	}
	*scn = DatumGetUInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(v->val.numeric)));

	/* commit scn - required */
	v = getKeyJsonValueFromContainer(&jb->root, "c_scn", strlen("c_scn"), &vbuf);
	if (!v)
	{
		elog(WARNING, "malformed change request - no c_scn value");
		goto end;
	}
	*c_scn = DatumGetUInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(v->val.numeric)));

	/* db - required */
	v = getKeyJsonValueFromContainer(&jb->root, "db", strlen("db"), &vbuf);
	if (!v)
	{
		elog(WARNING, "malformed change request - no db value");
		goto end;
	}
	db = pnstrdup(v->val.string.val, v->val.string.len);

	/* fetch payload.0.schema */
	jbschema = DatumGetJsonbP(jsonb_get_element(payload, &datum_path_schema[0], 1, &isnull, false));
	if (!jbschema)
	{
		elog(WARNING, "malformed change request - no payload.0.schema struct");
		goto end;
	}

	/* fetch owner -> considered schema - optional*/
	v = getKeyJsonValueFromContainer(&jbschema->root, "owner", strlen("owner"), &vbuf);
	if (v)
	{
		schema = pnstrdup(v->val.string.val, v->val.string.len);
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
	if (!v)
	{
		elog(WARNING, "malformed change request - no payload.0.schema.table value");
		goto end;
	}
	table = pnstrdup(v->val.string.val, v->val.string.len);

	/* fetch sql - required */
	initStringInfo(&sql);
	v = getKeyJsonValueFromContainer(&payload->root, "sql", strlen("sql"), &vbuf);
	if (!v)
	{
		elog(WARNING, "malformed change request - no payload.0.sql value");
		goto end;
	}

	appendBinaryStringInfo(&sql, v->val.string.val, v->val.string.len);
	remove_double_quotes(&sql);

	if (!is_whitelist_sql(&sql))
	{
		elog(DEBUG1, "unsupported DDL -----> %s", sql.data);
		goto end;
	}

	/* construct the ddl struct */
	olrddl = (DBZ_DDL*) palloc0(sizeof(DBZ_DDL));

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
	/* Parse the Oracle SQL */
	PG_TRY();
	{
		ptree = oracle_raw_parser(sql.data, RAW_PARSE_DEFAULT);
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

					/* data type */
					if (col->typeName && col->typeName->names)
						elog(DEBUG1, "Type: %s", NameListToString(col->typeName->names));
					ddlcol->typeName = pstrdup(NameListToString(col->typeName->names));

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
								elog(DEBUG1, "unsupported constraint type %d", constr->contype);
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
			}

			if (pklist.data)
				pfree(pklist.data);
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

						olrddl->subtype = SUBTYPE_ADD_COLUMN;

						/* data type */
						if (col->typeName && col->typeName->names)
							elog(DEBUG1, "Type: %s", NameListToString(col->typeName->names));
						ddlcol->typeName = pstrdup(NameListToString(col->typeName->names));

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
									elog(DEBUG1, "unsupported constraint type %d", constr->contype);
							}

							if (pklist.len > 1)	/* longer than '[' */
							{
								pklist.data[pklist.len - 1] = '\0';
								pklist.len = pklist.len - 1;
								appendStringInfo(&pklist, "]");
								olrddl->primaryKeyColumnNames = pstrdup(pklist.data);
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
						olrddl->subtype = SUBTYPE_ALTER_COLUMN;

						/* data type */
						if (col->typeName && col->typeName->names)
							elog(DEBUG1, "Type: %s", NameListToString(col->typeName->names));
						ddlcol->typeName = pstrdup(NameListToString(col->typeName->names));

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
							}
						}
						else
						{
							elog(DEBUG1, "unsupported constraint type %d", constr->contype);
						}
						break;
					}
					case AT_DropConstraint:
					{
						if (cmd->name)
						{
							elog(DEBUG1, "dropping constraint: %s", cmd->name);
							olrddl->constraintName = pstrdup(cmd->name);
						}
						olrddl->subtype = SUBTYPE_DROP_CONSTRAINT;
						break;
					}
					default:
					{
						elog(WARNING, "  Unhandled ALTER subtype: %d", cmd->subtype);
						destroyOLRDDL(olrddl);
						olrddl = NULL;
						goto end;
						break;
					}
				}

				if (ddlcol != NULL)
					olrddl->columns = lappend(olrddl->columns, ddlcol);
			}
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
				elog(DEBUG1, "unsupported drop type %d", dropStmt->removeType);
		}
		else if (IsA(stmt, TruncateStmt))
		{
			TruncateStmt *truncateStmt = (TruncateStmt *)stmt;
			ListCell *objCell;
			olrddl->type = DDL_TRUNCATE_TABLE;

			if (strstr(dbz_skipped_operations, "t"))
			{
				elog(WARNING, "truncate support is not enabled. Remove 't' from "
						"dbz_skipped_operations GUC to support it");
				destroyOLRDDL(olrddl);
				olrddl = NULL;
				goto end;
			}

			/* we only expect one entry from this list */
			if (list_length(truncateStmt->relations) > 1)
			{
				elog(WARNING, "truncate more than 1 relations is not supported (%d)",
						list_length(truncateStmt->relations));
				destroyOLRDDL(olrddl);
				olrddl = NULL;
				goto end;
			}

			foreach (objCell, truncateStmt->relations)
			{
				RangeVar *rv = lfirst(objCell);
				elog(WARNING, "truncate rv.relname %s", rv->relname);

				/* replace table with the new value to be truncated */
				if(table)
				{
					pfree(table);
					table = pstrdup(rv->relname);
				}
			}
		}
		else
		{
			elog(WARNING, "unsupported stmt type: %d ",  nodeTag(stmt));
			destroyOLRDDL(olrddl);
			olrddl = NULL;
			goto end;
		}
	}

	if (db && schema && table)
		olrddl->id = psprintf("%s.%s.%s", db, schema, table);
	else
		olrddl->id = psprintf("%s.%s", db, table);

	for (j = 0; j < strlen(olrddl->id); j++)
		olrddl->id[j] = (char) pg_tolower((unsigned char) olrddl->id[j]);
end:

	return olrddl;
}

/*
 * parseOLRDML
 */
static OLR_DML *
parseOLRDML(Jsonb * jb, char op, Jsonb * payload, orascn * scn, orascn * c_scn, bool isfirst, bool islast)
{
	JsonbValue * v = NULL;
	JsonbValue vbuf;
	Jsonb * jbschema;
	bool isnull = false;
	OLR_DML * olrdml = NULL;
	StringInfoData strinfo, objid;
	bool found;
	HTAB * typeidhash;
	HTAB * namejsonposhash;
	HASHCTL hash_ctl;
	NameOidEntry * entry;
	NameJsonposEntry * entry2;
	Oid schemaoid;
	Relation rel;
	TupleDesc tupdesc;
	int attnum, j = 0;
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
	jbschema = DatumGetJsonbP(jsonb_get_element(payload, &datum_path_schema[0], 1, &isnull, false));
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

	/* special checking to exclude LOG_MINING_FLUSH table created by Debezium. */
	if (!strcasecmp(table, DBZ_LOG_MINING_FLUSH_TABLE))
	{
		elog(WARNING, "debezium log mining flush table %s ignored...", DBZ_LOG_MINING_FLUSH_TABLE);
		destroyOLRDML(olrdml);
		olrdml = NULL;
		goto end;
	}
	appendStringInfo(&objid, "%s", table);

	/* table name transformation and normalized objectid to lower case */
	for (j = 0; j < objid.len; j++)
		objid.data[j] = (char) pg_tolower((unsigned char) objid.data[j]);

	olrdml->remoteObjectId = pstrdup(objid.data);
	olrdml->mappedObjectId = transform_object_name(olrdml->remoteObjectId, "table");
	if (olrdml->mappedObjectId)
	{
		char * objectIdCopy = pstrdup(olrdml->mappedObjectId);
		char * db2 = NULL, * table2 = NULL, * schema2 = NULL;

		splitIdString(objectIdCopy, &db2, &schema2, &table2, false);
		if (!table2)
		{
			/* save the error */
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "transformed object ID is invalid: %s",
					olrdml->mappedObjectId);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}
		else
			olrdml->table = pstrdup(table2);

		if (schema2)
			olrdml->schema = pstrdup(schema2);
		else
			olrdml->schema = pstrdup("public");
	}
	else
	{
		/* by default, remote's db is mapped to schema in pg */
		olrdml->schema = pstrdup(db);
		olrdml->table = pstrdup(table);

		resetStringInfo(&strinfo);
		appendStringInfo(&strinfo, "%s.%s", olrdml->schema, olrdml->table);
		olrdml->mappedObjectId = pstrdup(strinfo.data);
	}

	/*
	 * before parsing, we need to make sure the target namespace and table
	 * do exist in PostgreSQL, and also fetch their attribute type IDs. PG
	 * automatically converts upper case letters to lower when they are
	 * created. However, catalog lookups are case sensitive so here we must
	 * convert db and table to all lower case letters.
	 */
	for (j = 0; j < strlen(olrdml->schema); j++)
		olrdml->schema[j] = (char) pg_tolower((unsigned char) olrdml->schema[j]);

	for (j = 0; j < strlen(olrdml->table); j++)
		olrdml->table[j] = (char) pg_tolower((unsigned char) olrdml->table[j]);

	/* prepare cache key */
	strlcpy(cachekey.schema, olrdml->schema, sizeof(cachekey.schema));
	strlcpy(cachekey.table, olrdml->table, sizeof(cachekey.table));

	cacheentry = (DataCacheEntry *) hash_search(dataCacheHash, &cachekey, HASH_ENTER, &found);
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
		schemaoid = get_namespace_oid(olrdml->schema, false);
		if (!OidIsValid(schemaoid))
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for schema '%s'", olrdml->schema);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

		olrdml->tableoid = get_relname_relid(olrdml->table, schemaoid);
		if (!OidIsValid(olrdml->tableoid))
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for table '%s'", olrdml->table);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

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

		/* cache tupdesc and save natts for later use */
		cacheentry->tupdesc = CreateTupleDescCopy(tupdesc);
		olrdml->natts = tupdesc->natts;
		cacheentry->natts = olrdml->natts;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
			entry = (NameOidEntry *) hash_search(typeidhash, NameStr(attr->attname), HASH_ENTER, &found);
			if (!found)
			{
				strlcpy(entry->name, NameStr(attr->attname), NAMEDATALEN);
				entry->oid = attr->atttypid;
				entry->position = attnum;
				entry->typemod = attr->atttypmod;
				if (pkattrs && bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber, pkattrs))
					entry->ispk =true;
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
		cacheentry->namejsonposhash = build_olr_schema_jsonpos_hash(jbschema);
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
			DBZ_DML_COLUMN_VALUE * colval = NULL;
			Datum datum_elems[1] = {CStringGetTextDatum("after")};

			dmldata = DatumGetJsonbP(jsonb_get_element(payload, &datum_elems[0], 1, &isnull, false));
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
									getPathElementString(payload, tmpPath, &strinfo, false);
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
							break;
						case WJB_VALUE:
						case WJB_ELEM:
							if (pause)
								break;
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

						/* convert to lower case column name */
						for (j = 0; j < strlen(colval->name); j++)
							colval->name[j] = (char) pg_tolower((unsigned char) colval->name[j]);

						colval->value = pstrdup(value);
						/* a copy of original column name for expression rule lookup at later stage */
						colval->remoteColumnName = pstrdup(colval->name);

						/* transform the column name if needed */
						initStringInfo(&colNameObjId);
						appendStringInfo(&colNameObjId, "%s.%s", objid.data, colval->name);
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
							elog(ERROR, "cannot find data type for column %s. None-existent column?", colval->name);

						entry2 = (NameJsonposEntry *) hash_search(namejsonposhash, colval->remoteColumnName, HASH_FIND, &found);
						if (found)
						{
							colval->dbztype = entry2->dbztype;
							colval->timerep = entry2->timerep;
							colval->scale = entry2->scale;
						}
						else
							elog(ERROR, "cannot find olr json column schema data for column %s(%s). invalid json event?",
									colval->name, colval->remoteColumnName);

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
			DBZ_DML_COLUMN_VALUE * colval = NULL;
			Datum datum_elems_before[1] = {CStringGetTextDatum("before")};
			Datum datum_elems_after[1] = {CStringGetTextDatum("after")};
			int i = 0;

			for (i = 0; i < 2; i++)
			{
				if (i == 0)
					dmldata = DatumGetJsonbP(jsonb_get_element(payload, &datum_elems_before[0], 1, &isnull, false));
				else
					dmldata = DatumGetJsonbP(jsonb_get_element(payload, &datum_elems_after[0], 1, &isnull, false));
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
										getPathElementString(payload, tmpPath, &strinfo, false);
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
								break;
							case WJB_VALUE:
							case WJB_ELEM:
								if (pause)
									break;
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

							/* convert to lower case column name */
							for (j = 0; j < strlen(colval->name); j++)
								colval->name[j] = (char) pg_tolower((unsigned char) colval->name[j]);

							colval->value = pstrdup(value);
							/* a copy of original column name for expression rule lookup at later stage */
							colval->remoteColumnName = pstrdup(colval->name);

							/* transform the column name if needed */
							initStringInfo(&colNameObjId);
							appendStringInfo(&colNameObjId, "%s.%s", objid.data, colval->name);
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
								elog(ERROR, "cannot find data type for column %s. None-existent column?", colval->name);

							entry2 = (NameJsonposEntry *) hash_search(namejsonposhash, colval->remoteColumnName, HASH_FIND, &found);
							if (found)
							{
								colval->dbztype = entry2->dbztype;
								colval->timerep = entry2->timerep;
								colval->scale = entry2->scale;
							}
							else
								elog(ERROR, "cannot find olr json column schema data for column %s(%s). invalid json event?",
										colval->name, colval->remoteColumnName);

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
			DBZ_DML_COLUMN_VALUE * colval = NULL;
			Datum datum_elems[1] = {CStringGetTextDatum("before")};

			dmldata = DatumGetJsonbP(jsonb_get_element(payload, &datum_elems[0], 1, &isnull, false));
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
									getPathElementString(payload, tmpPath, &strinfo, false);
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
							break;
						case WJB_VALUE:
						case WJB_ELEM:
							if (pause)
								break;
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

						/* convert to lower case column name */
						for (j = 0; j < strlen(colval->name); j++)
							colval->name[j] = (char) pg_tolower((unsigned char) colval->name[j]);

						colval->value = pstrdup(value);
						/* a copy of original column name for expression rule lookup at later stage */
						colval->remoteColumnName = pstrdup(colval->name);

						/* transform the column name if needed */
						initStringInfo(&colNameObjId);
						appendStringInfo(&colNameObjId, "%s.%s", objid.data, colval->name);
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
							elog(ERROR, "cannot find data type for column %s. None-existent column?", colval->name);

						entry2 = (NameJsonposEntry *) hash_search(namejsonposhash, colval->remoteColumnName, HASH_FIND, &found);
						if (found)
						{
							colval->dbztype = entry2->dbztype;
							colval->timerep = entry2->timerep;
							colval->scale = entry2->scale;
						}
						else
							elog(ERROR, "cannot find olr json column schema data for column %s(%s). invalid json event?",
									colval->name, colval->remoteColumnName);

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
fc_processOLRChangeEvent(const char * event, SynchdbStatistics * myBatchStats,
		const char * name, bool * sendconfirm, bool isfirst, bool islast)
{
	Datum jsonb_datum;
	Jsonb * jb = NULL;
	Jsonb * payload = NULL;
	JsonbValue * v = NULL;
	JsonbValue vbuf;
	char * op = NULL;
	int ret = -1;
	bool isnull = false;
	MemoryContext tempContext, oldContext;
	struct timeval tv;

	Datum datum_path_payload[2] = {CStringGetTextDatum("payload"), CStringGetTextDatum("0")};

	tempContext = AllocSetContextCreate(TopMemoryContext,
										"FORMAT_CONVERTER",
										ALLOCSET_DEFAULT_SIZES);

	oldContext = MemoryContextSwitchTo(tempContext);

    /* Convert event string to JSONB */
	PG_TRY();
	{
	    jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(event));
	    jb = DatumGetJsonbP(jsonb_datum);
	}
	PG_CATCH();
	{
		FlushErrorState();
		elog(WARNING, "bad json message: %s", event);
		increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
		MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(tempContext);
		return -1;
	}
	PG_END_TRY();

	elog(DEBUG1, "%s", event);

	/* payload - required */
	payload = DatumGetJsonbP(jsonb_get_element(jb, &datum_path_payload[0], 2, &isnull, false));
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
		orascn scn = 0, c_scn = 0;

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

		/* transaction boundary */
		if (!strcasecmp(op, "begin"))
		{
			/* todo */
			increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
		}
		else if (!strcasecmp(op, "commit"))
		{
			/* todo */
			increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
		}
		set_shm_connector_state(myConnectorId, STATE_SYNCING);

		/* tm - only at first and last record within a batch */
		/* update processing timestamps */
    	if (islast)
    	{
			v = getKeyJsonValueFromContainer(&jb->root, "tm", strlen("tm"), &vbuf);
			if (v)
			{
				myBatchStats->stats_last_src_ts = DatumGetUInt64(DirectFunctionCall1(numeric_int8,
						NumericGetDatum(v->val.numeric))) / 1000 / 1000;
			}
			gettimeofday(&tv, NULL);
			myBatchStats->stats_last_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

    	if (isfirst)
    	{
			v = getKeyJsonValueFromContainer(&jb->root, "tm", strlen("tm"), &vbuf);
			if (v)
			{
				myBatchStats->stats_first_src_ts = DatumGetUInt64(DirectFunctionCall1(numeric_int8,
						NumericGetDatum(v->val.numeric))) / 1000 / 1000;
			}
			gettimeofday(&tv, NULL);
			myBatchStats->stats_first_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

    	olr_client_set_scns(scn, c_scn);
    	*sendconfirm = true;
	}
	else if (!strcasecmp(op, "c") || !strcasecmp(op, "u") || !strcasecmp(op, "d"))
	{
		/* DMLs */
		OLR_DML * olrdml = NULL;
		PG_DML * pgdml = NULL;
		orascn scn = 0, c_scn = 0;

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
		olrdml = parseOLRDML(jb, op[0], payload, &scn, &c_scn, isfirst, islast);
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
    	ret = ra_executePGDML(pgdml, TYPE_OLR, myBatchStats);
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
    	olr_client_set_scns(scn, c_scn);
    	* sendconfirm = true;

    	if (islast)
    	{
			myBatchStats->stats_last_src_ts = olrdml->src_ts_ms;
			myBatchStats->stats_last_dbz_ts = olrdml->dbz_ts_ms;
			gettimeofday(&tv, NULL);
			myBatchStats->stats_last_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

    	if (isfirst)
    	{
			myBatchStats->stats_first_src_ts = olrdml->src_ts_ms;
			myBatchStats->stats_first_dbz_ts = olrdml->dbz_ts_ms;
			gettimeofday(&tv, NULL);
			myBatchStats->stats_first_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
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
		orascn scn = 0, c_scn = 0;

		/* increment batch statistics */
		increment_connector_statistics(myBatchStats, STATS_DDL, 1);

		/* (1) make sure oracle parser is ready to use - todo move earlier */
		if (oracle_raw_parser == NULL)
		{
			char * oralib_path = NULL, * error = NULL;

			oralib_path = psprintf("%s/%s", pkglib_path, ORACLE_RAW_PARSER_LIB);

			/* Load the shared library */
			handle = dlopen(oralib_path, RTLD_NOW | RTLD_GLOBAL);
			if (!handle)
			{
				set_shm_connector_errmsg(myConnectorId, "failed to load oracle_parser.so");
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("failed to load oracle_parser.so at %s", oralib_path),
						 errhint("%s",  dlerror())));

			}
			pfree(oralib_path);

			oracle_raw_parser = (oracle_raw_parser_fn) dlsym(handle, "oracle_raw_parser");
			if ((error = dlerror()) != NULL)
			{
				set_shm_connector_errmsg(myConnectorId, "failed to load oracle_raw_parser symbol");
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("failed to load oracle_raw_parser function symbol"),
						 errhint("make sure oracle_raw_parser() function in oracle_parser.so is "
								 "publicly accessible ")));
			}
		}

		/* (2) parse */
		set_shm_connector_state(myConnectorId, STATE_PARSING);
		olrddl = parseOLRDDL(jb, payload, &scn, &c_scn, isfirst, islast);
    	if (!olrddl)
		{
			set_shm_connector_state(myConnectorId, STATE_SYNCING);
			increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);

			/*
			 * openlog replicator delivers a lot of DDLs issued internally in oracle that
			 * we still receive and try to process. Most of them do not parse but we still
			 * have to move forward with scn and c_scn so we do not receive these again
			 * in case of connector restart.
			 */
	    	olr_client_set_scns(scn, c_scn);
	    	* sendconfirm = true;

			MemoryContextSwitchTo(oldContext);
			MemoryContextDelete(tempContext);
			return -1;
		}

    	/* (3) convert */
    	set_shm_connector_state(myConnectorId, STATE_CONVERTING);
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

    	/* (5) record scn, c_scn and processing timestmaps */
    	olr_client_set_scns(scn, c_scn);
    	* sendconfirm = true;

    	if (islast)
    	{
			myBatchStats->stats_last_src_ts = olrddl->src_ts_ms;
			myBatchStats->stats_last_dbz_ts = olrddl->dbz_ts_ms;
			gettimeofday(&tv, NULL);
			myBatchStats->stats_last_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

    	if (isfirst)
    	{
			myBatchStats->stats_first_src_ts = olrddl->src_ts_ms;
			myBatchStats->stats_first_dbz_ts = olrddl->dbz_ts_ms;
			gettimeofday(&tv, NULL);
			myBatchStats->stats_first_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

		/* (6) update attribute map table */
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
}
