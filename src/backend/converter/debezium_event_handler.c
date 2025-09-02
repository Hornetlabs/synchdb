/*
 * debezium_event_handler.c
 *
 * contains routines to process change events originated from
 * debezium connectors.
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "catalog/namespace.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include <time.h>
#include <sys/time.h>
#include <dlfcn.h>

#include "access/table.h"
#include "synchdb/synchdb.h"
#include "converter/debezium_event_handler.h"
#include "converter/format_converter.h"
#ifdef WITH_OLR
#include "olr/olr_client.h"
#endif

/* extern globals */
extern int myConnectorId;
extern bool synchdb_log_event_on_error;
extern char * g_eventStr;
extern HTAB * dataCacheHash;

static DdlType name_to_ddltype(const char * name);
static DbzType getDbzTypeFromString(const char * typestring);
static TimeRep getTimerepFromString(const char * typestring);
static HTAB * build_schema_jsonpos_hash(Jsonb * jb);
static void destroyDBZDDL(DBZ_DDL * ddlinfo);
static void destroyDBZDML(DBZ_DML * dmlinfo);
static DBZ_DDL * parseDBZDDL(Jsonb * jb, bool isfirst, bool islast);
static DBZ_DML * parseDBZDML(Jsonb * jb, char op, ConnectorType type,
		Jsonb * source, bool isfirst, bool islast);

static DdlType
name_to_ddltype(const char * name)
{
	if (!strcasecmp(name, "CREATE"))
		return DDL_CREATE_TABLE;
	else if (!strcasecmp(name, "ALTER"))
		return DDL_ALTER_TABLE;
	else if (!strcasecmp(name, "DROP"))
		return DDL_DROP_TABLE;
	else
		return DDL_UNDEF;
}

static DbzType
getDbzTypeFromString(const char * typestring)
{
	if (!typestring)
		return DBZTYPE_UNDEF;

	/* todo: perhaps a hash lookup table is more efficient */
	/* DBZ types */
	if (!strcmp(typestring, "float32"))
		return DBZTYPE_FLOAT32;
	if (!strcmp(typestring, "float64"))
		return DBZTYPE_FLOAT64;
	if (!strcmp(typestring, "float"))
		return DBZTYPE_FLOAT;
	if (!strcmp(typestring, "double"))
		return DBZTYPE_DOUBLE;
	if (!strcmp(typestring, "bytes"))
		return DBZTYPE_BYTES;
	if (!strcmp(typestring, "int8"))
		return DBZTYPE_INT8;
	if (!strcmp(typestring, "int16"))
		return DBZTYPE_INT16;
	if (!strcmp(typestring, "int32"))
		return DBZTYPE_INT32;
	if (!strcmp(typestring, "int64"))
		return DBZTYPE_INT64;
	if (!strcmp(typestring, "struct"))
		return DBZTYPE_STRUCT;
	if (!strcmp(typestring, "string"))
		return DBZTYPE_STRING;

	return DBZTYPE_UNDEF;
}

static TimeRep
getTimerepFromString(const char * typestring)
{
	if (!typestring)
		return TIME_UNDEF;

	if (find_exact_string_match(typestring, "io.debezium.time.Date"))
		return TIME_DATE;
	else if (find_exact_string_match(typestring, "io.debezium.time.Time"))
		return TIME_TIME;
	else if (find_exact_string_match(typestring, "io.debezium.time.MicroTime"))
		return TIME_MICROTIME;
	else if (find_exact_string_match(typestring, "io.debezium.time.NanoTime"))
		return TIME_NANOTIME;
	else if (find_exact_string_match(typestring, "io.debezium.time.Timestamp"))
		return TIME_TIMESTAMP;
	else if (find_exact_string_match(typestring, "io.debezium.time.MicroTimestamp"))
		return TIME_MICROTIMESTAMP;
	else if (find_exact_string_match(typestring, "io.debezium.time.NanoTimestamp"))
		return TIME_NANOTIMESTAMP;
	else if (find_exact_string_match(typestring, "io.debezium.time.ZonedTimestamp"))
		return TIME_ZONEDTIMESTAMP;
	else if (find_exact_string_match(typestring, "io.debezium.time.MicroDuration"))
		return TIME_MICRODURATION;
	else if (find_exact_string_match(typestring, "io.debezium.data.VariableScaleDecimal"))
		return DATA_VARIABLE_SCALE;
	else if (find_exact_string_match(typestring, "io.debezium.data.geometry.Geometry"))
		return DATA_VARIABLE_SCALE;
	else if (find_exact_string_match(typestring, "io.debezium.data.Enum"))
		return DATA_ENUM;

	elog(DEBUG1, "unhandled dbz type %s", typestring);
	return TIME_UNDEF;
}

static HTAB *
build_schema_jsonpos_hash(Jsonb * jb)
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
	Datum datum_elems[4] ={CStringGetTextDatum("schema"), CStringGetTextDatum("fields"),
			CStringGetTextDatum("0"), CStringGetTextDatum("fields")};
	bool isnull;

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = NAMEDATALEN;
	hash_ctl.entrysize = sizeof(NameJsonposEntry);
	hash_ctl.hcxt = TopMemoryContext;

	jsonposhash = hash_create("Name to jsonpos Hash Table",
							512,
							&hash_ctl,
							HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

	schemadata = DatumGetJsonbP(jsonb_get_element(jb, &datum_elems[0], 4, &isnull, false));
	if (schemadata)
	{
		contsize = JsonContainerSize(&schemadata->root);
		for (i = 0; i < contsize; i++)
		{
			JsonbValue * v = NULL, * v2 = NULL, *v3 = NULL;
			JsonbValue vbuf;
			char * tmpstr = NULL;

			memset(&tmprecord, 0, sizeof(NameJsonposEntry));
			v = getIthJsonbValueFromContainer(&schemadata->root, i);
			if (v->type == jbvBinary)
			{
				v2 = getKeyJsonValueFromContainer(v->val.binary.data, "field", strlen("field"), &vbuf);
				if (v2)
				{
					strncpy(tmprecord.name, v2->val.string.val, v2->val.string.len); /* todo check overflow */
					for (j = 0; j < strlen(tmprecord.name); j++)
						tmprecord.name[j] = (char) pg_tolower((unsigned char) tmprecord.name[j]);
				}
				else
				{
					elog(WARNING, "field is missing from dbz schema...");
					continue;
				}
				v2 = getKeyJsonValueFromContainer(v->val.binary.data, "type", strlen("type"), &vbuf);
				if (v2)
				{
					tmpstr = pnstrdup(v2->val.string.val, v2->val.string.len);
					tmprecord.dbztype = getDbzTypeFromString(tmpstr);
					pfree(tmpstr);
				}
				else
				{
					elog(WARNING, "type is missing from dbz schema...");
					continue;
				}
				v2 = getKeyJsonValueFromContainer(v->val.binary.data, "name", strlen("name"), &vbuf);
				if (v2)
				{
					tmpstr = pnstrdup(v2->val.string.val, v2->val.string.len);
					tmprecord.timerep = getTimerepFromString(tmpstr);
					pfree(tmpstr);
				}

				/* check if parameters group exists */
				v2 = getKeyJsonValueFromContainer(v->val.binary.data, "parameters", strlen("parameters"), &vbuf);
				if (v2)
				{
					if (v->type == jbvBinary)
					{
						v3 = getKeyJsonValueFromContainer(v2->val.binary.data, "scale", strlen("scale"), &vbuf);
						if (v3)
						{
							tmpstr = pnstrdup(v3->val.string.val, v3->val.string.len);
							tmprecord.scale = atoi(tmpstr);
							pfree(tmpstr);
						}
					}
				}
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
 * destroyDBZDDL
 *
 * Function to destroy DBZ_DDL structure
 */
static void
destroyDBZDDL(DBZ_DDL * ddlinfo)
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
 * destroyDBZDML
 *
 * Function to destroy DBZ_DML structure
 */
static void
destroyDBZDML(DBZ_DML * dmlinfo)
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
 * parseDBZDDL
 *
 * Function to parse Debezium DDL expressed in Jsonb
 *
 * @return DBZ_DDL structure
 */
static DBZ_DDL *
parseDBZDDL(Jsonb * jb, bool isfirst, bool islast)
{
	Jsonb * ddlpayload = NULL;
	JsonbIterator *it;
	JsonbValue v;
	JsonbIteratorToken r;
	char * key = NULL;
	char * value = NULL;
	int j = 0;

	DBZ_DDL * ddlinfo = (DBZ_DDL*) palloc0(sizeof(DBZ_DDL));
	DBZ_DDL_COLUMN * ddlcol = NULL;

	/* get table name and action type */
	StringInfoData strinfo;
	initStringInfo(&strinfo);

	/*
	 * payload.ts_ms and payload.source.ts_ms- read only on the first or last
	 * change event of a batch for statistic display purpose
	 */
	if (isfirst || islast)
	{
		getPathElementString(jb, "payload.ts_ms", &strinfo, true);
		if (!strcasecmp(strinfo.data, "NULL"))
			ddlinfo->dbz_ts_ms = 0;
		else
			ddlinfo->dbz_ts_ms = strtoull(strinfo.data, NULL, 10);

		getPathElementString(jb, "payload.source.ts_ms", &strinfo, true);
		if (!strcasecmp(strinfo.data, "NULL"))
			ddlinfo->src_ts_ms = 0;
		else
			ddlinfo->src_ts_ms = strtoull(strinfo.data, NULL, 10);
	}

    getPathElementString(jb, "payload.tableChanges.0.id", &strinfo, true);
    ddlinfo->id = pstrdup(strinfo.data);

    getPathElementString(jb, "payload.tableChanges.0.table.primaryKeyColumnNames", &strinfo, false);
    ddlinfo->primaryKeyColumnNames = pstrdup(strinfo.data);

    getPathElementString(jb, "payload.tableChanges.0.type", &strinfo, true);
    ddlinfo->type = name_to_ddltype(strinfo.data);

    /* free the data inside strinfo as we no longer needs it */
    pfree(strinfo.data);

    if (!strcmp(ddlinfo->id, "NULL") || ddlinfo->type == DDL_UNDEF)
    {
    	elog(DEBUG1, "no table change data or unknown DDL type. Stop parsing...");
    	destroyDBZDDL(ddlinfo);
    	return NULL;
    }

    /* once we are done checking ddlinfo->id, we turn it to lowercase */
	for (j = 0; j < strlen(ddlinfo->id); j++)
		ddlinfo->id[j] = (char) pg_tolower((unsigned char) ddlinfo->id[j]);

    if (ddlinfo->type == DDL_CREATE_TABLE || ddlinfo->type == DDL_ALTER_TABLE)
    {
		/* fetch payload.tableChanges.0.table.columns as jsonb */
    	Datum datum_elems[5] ={CStringGetTextDatum("payload"), CStringGetTextDatum("tableChanges"),
    			CStringGetTextDatum("0"), CStringGetTextDatum("table"), CStringGetTextDatum("columns")};
    	bool isnull;

    	ddlpayload = DatumGetJsonbP(jsonb_get_element(jb, &datum_elems[0], 5, &isnull, false));
		/*
		 * following parser expects this json array named 'columns' from DBZ embedded:
		 * "columns": [
		 *   {
		 *     "name": "a",
		 *     "scale": null,
		 *     "length": null,
		 *     "comment": null,
		 *     "jdbcType": 4,
		 *     "optional": true,
		 *     "position": 1,
		 *     "typeName": "INT",
		 *     "generated": false,
		 *     "enumValues": null,
		 *     "nativeType": null,
		 *     "charsetName": null,
		 *     "typeExpression": "INT",
		 *     "autoIncremented": false,
		 *     "defaultValueExpression": null
		 *   },
		 *   ...... rest of array elements
		 *
		 * columns array may contains another array of enumValues, this is ignored
		 * for now as enums are to be mapped to text as of now
		 *
		 *	   "enumValues":
		 *     [
         *         "'fish'",
         *         "'mammal'",
         *         "'bird'"
         *     ]
		 */
		if (ddlpayload)
		{
			int pause = 0;
			/* iterate this payload jsonb */
			it = JsonbIteratorInit(&ddlpayload->root);
			while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
			{
				switch (r)
				{
					case WJB_BEGIN_OBJECT:
						elog(DEBUG1, "parsing column --------------------");
						ddlcol = (DBZ_DDL_COLUMN *) palloc0(sizeof(DBZ_DDL_COLUMN));

						if (key)
						{
							pfree(key);
							key = NULL;
						}
						break;
					case WJB_END_OBJECT:
						/* append ddlcol to ddlinfo->columns list for further processing */
						ddlinfo->columns = lappend(ddlinfo->columns, ddlcol);

						break;
					case WJB_BEGIN_ARRAY:
						elog(DEBUG1, "Begin array under %s", key ? key : "NULL");
						if (key)
						{
							elog(DEBUG1, "sub array detected, skip it");
							pause = 1;
							pfree(key);
							key = NULL;
						}
						break;
					case WJB_END_ARRAY:
						elog(DEBUG1, "End array");
						if (pause)
						{
							elog(DEBUG1, "sub array ended, resume parsing operation");
							pause = 0;
						}
						break;
					case WJB_KEY:
						if (pause)
							break;
						key = pnstrdup(v.val.string.val, v.val.string.len);
						elog(DEBUG2, "Key: %s", key);

						break;
					case WJB_VALUE:
					case WJB_ELEM:
						if (pause)
							break;
						switch (v.type)
						{
							case jbvNull:
								elog(DEBUG2, "Value: NULL");
								value = pnstrdup("NULL", strlen("NULL"));
								break;
							case jbvString:
								value = pnstrdup(v.val.string.val, v.val.string.len);
								elog(DEBUG2, "String Value: %s", value);
								break;
							case jbvNumeric:
							{
								value = DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v.val.numeric)));
								elog(DEBUG2, "Numeric Value: %s", value);
								break;
							}
							case jbvBool:
								elog(DEBUG2, "Boolean Value: %s", v.val.boolean ? "true" : "false");
								if (v.val.boolean)
									value = pnstrdup("true", strlen("true"));
								else
									value = pnstrdup("false", strlen("false"));
								break;
							case jbvBinary:
								elog(DEBUG2, "Binary Value: [binary data]");
								break;
							default:
								elog(DEBUG2, "Unknown value type: %d", v.type);
								break;
						}
					break;
					default:
						elog(DEBUG1, "Unknown token: %d", r);
						break;
				}

				/* check if we have a key - value pair */
				if (key != NULL && value != NULL)
				{
					if (!strcmp(key, "name"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->name = pstrdup(value);

						/* convert typeName to lowercase for consistency */
						for (j = 0; j < strlen(ddlcol->name); j++)
							ddlcol->name[j] = (char) pg_tolower(ddlcol->name[j]);
					}
					if (!strcmp(key, "length"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->length = strcmp(value, "NULL") == 0 ? 0 : atoi(value);
					}
					if (!strcmp(key, "optional"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->optional = strcmp(value, "true") == 0 ? true : false;
					}
					if (!strcmp(key, "position"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->position = atoi(value);
					}
					if (!strcmp(key, "typeName"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->typeName = pstrdup(value);

						/* convert typeName to lowercase for consistency */
						for (j = 0; j < strlen(ddlcol->typeName); j++)
							ddlcol->typeName[j] = (char) pg_tolower(ddlcol->typeName[j]);
					}
					if (!strcmp(key, "enumValues"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->enumValues = pstrdup(value);
					}
					if (!strcmp(key, "charsetName"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->charsetName = pstrdup(value);
					}
					if (!strcmp(key, "autoIncremented"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->autoIncremented = strcmp(value, "true") == 0 ? true : false;
					}
					if (!strcmp(key, "defaultValueExpression"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->defaultValueExpression = pstrdup(value);
					}
					if (!strcmp(key, "scale"))
					{
						elog(DEBUG1, "consuming %s = %s", key, value);
						ddlcol->scale = strcmp(value, "NULL") == 0 ? 0 : atoi(value);
					}

					/* note: other key - value pairs ignored for now */
					pfree(key);
					pfree(value);
					key = NULL;
					value = NULL;
				}
			}
		}
		else
		{
			elog(WARNING, "failed to get payload.tableChanges.0.table.columns as jsonb");
			destroyDBZDDL(ddlinfo);
			return NULL;
		}
    }
    else if (ddlinfo->type == DDL_DROP_TABLE)
    {
    	/* no further parsing needed for DROP, just return ddlinfo */
    	return ddlinfo;
    }
    else
    {
		elog(WARNING, "unsupported ddl type %d", ddlinfo->type);
		destroyDBZDDL(ddlinfo);
		return NULL;
    }
	return ddlinfo;
}

/*
 * parseDBZDML
 *
 * this function parses a Jsonb that represents DML operation and produce a DBZ_DML structure
 */
static DBZ_DML *
parseDBZDML(Jsonb * jb, char op, ConnectorType type, Jsonb * source, bool isfirst, bool islast)
{
	StringInfoData strinfo, objid;
	Jsonb * dmlpayload = NULL;
	JsonbIterator *it;
	JsonbValue v;
	JsonbIteratorToken r;
	char * key = NULL;
	char * value = NULL;
	DBZ_DML * dbzdml = NULL;
	DBZ_DML_COLUMN_VALUE * colval = NULL;
	Oid schemaoid;
	Relation rel;
	TupleDesc tupdesc;
	int attnum, j = 0;
	bool isnull = false;
	HTAB * typeidhash;
	HTAB * namejsonposhash;
	HASHCTL hash_ctl;
	NameOidEntry * entry;
	NameJsonposEntry * entry2;
	bool found;
	DataCacheKey cachekey = {0};
	DataCacheEntry * cacheentry;
	Bitmapset * pkattrs;

	/* these are the components that compose of an object ID before transformation */
	char * db = NULL, * schema = NULL, * table = NULL;

	initStringInfo(&strinfo);
	initStringInfo(&objid);
	dbzdml = (DBZ_DML *) palloc0(sizeof(DBZ_DML));

	if (source)
	{
		JsonbValue * v = NULL;
		JsonbValue vbuf;

		/* payload.source.db - required */
		v = getKeyJsonValueFromContainer(&source->root, "db", strlen("db"), &vbuf);
		if (!v)
		{
			elog(WARNING, "malformed DML change request - no database attribute specified");
			destroyDBZDML(dbzdml);
			dbzdml = NULL;
			goto end;
		}
		db = pnstrdup(v->val.string.val, v->val.string.len);
		appendStringInfo(&objid, "%s.", db);
		memset(&vbuf, 0, sizeof(JsonbValue));

		/*
		 * payload.source.ts_ms - read only on the first or last change event of a batch
		 * for statistic display purpose
		 */
		if (isfirst || islast)
		{
			v = getKeyJsonValueFromContainer(&source->root, "ts_ms", strlen("ts_ms"), &vbuf);
			if (!v)
				dbzdml->src_ts_ms = 0;
			else
				dbzdml->src_ts_ms = DatumGetUInt64(DirectFunctionCall1(numeric_int8, PointerGetDatum(v->val.numeric)));
		}

		/* payload.source.schema - optional */
		v = getKeyJsonValueFromContainer(&source->root, "schema", strlen("schema"), &vbuf);
		if (v)
		{
			schema = pnstrdup(v->val.string.val, v->val.string.len);
			appendStringInfo(&objid, "%s.", schema);
		}

		/* payload.source.table - required */
		v = getKeyJsonValueFromContainer(&source->root, "table", strlen("table"), &vbuf);
		if (!v)
		{
			elog(WARNING, "malformed DML change request - no table attribute specified");
			destroyDBZDML(dbzdml);
			dbzdml = NULL;
			goto end;
		}
		table = pnstrdup(v->val.string.val, v->val.string.len);
		appendStringInfo(&objid, "%s", table);
	}
	else
	{
		elog(WARNING, "malformed DML change request - no source element");
		destroyDBZDML(dbzdml);
		dbzdml = NULL;
		goto end;
	}

	/*
	 * payload.ts_ms - read only on the first or last change event of a batch
	 * for statistic display purpose
	 */
	if (isfirst || islast)
	{
		getPathElementString(jb, "payload.ts_ms", &strinfo, true);
		if (!strcasecmp(strinfo.data, "NULL"))
			dbzdml->dbz_ts_ms = 0;
		else
			dbzdml->dbz_ts_ms = strtoull(strinfo.data, NULL, 10);
	}

	/* table name transformation and normalized objectid to lower case */
	for (j = 0; j < objid.len; j++)
		objid.data[j] = (char) pg_tolower((unsigned char) objid.data[j]);

	dbzdml->remoteObjectId = pstrdup(objid.data);
	dbzdml->mappedObjectId = transform_object_name(dbzdml->remoteObjectId, "table");
	if (dbzdml->mappedObjectId)
	{
		char * objectIdCopy = pstrdup(dbzdml->mappedObjectId);
		char * db2 = NULL, * table2 = NULL, * schema2 = NULL;

		splitIdString(objectIdCopy, &db2, &schema2, &table2, false);
		if (!table2)
		{
			/* save the error */
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "transformed object ID is invalid: %s",
					dbzdml->mappedObjectId);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}
		else
			dbzdml->table = pstrdup(table2);

		if (schema2)
			dbzdml->schema = pstrdup(schema2);
		else
			dbzdml->schema = pstrdup("public");
	}
	else
	{
		/* by default, remote's db is mapped to schema in pg */
		dbzdml->schema = pstrdup(db);
		dbzdml->table = pstrdup(table);

		resetStringInfo(&strinfo);
		appendStringInfo(&strinfo, "%s.%s", dbzdml->schema, dbzdml->table);
		dbzdml->mappedObjectId = pstrdup(strinfo.data);
	}
	/* free the temporary pointers */
	if (db)
	{
		pfree(db);
		db = NULL;
	}
	if (schema)
	{
		pfree(schema);
		schema = NULL;
	}
	if (table)
	{
		pfree(table);
		table = NULL;
	}

	dbzdml->op = op;

	/*
	 * before parsing, we need to make sure the target namespace and table
	 * do exist in PostgreSQL, and also fetch their attribute type IDs. PG
	 * automatically converts upper case letters to lower when they are
	 * created. However, catalog lookups are case sensitive so here we must
	 * convert db and table to all lower case letters.
	 */
	for (j = 0; j < strlen(dbzdml->schema); j++)
		dbzdml->schema[j] = (char) pg_tolower((unsigned char) dbzdml->schema[j]);

	for (j = 0; j < strlen(dbzdml->table); j++)
		dbzdml->table[j] = (char) pg_tolower((unsigned char) dbzdml->table[j]);

	/* prepare cache key */
	strlcpy(cachekey.schema, dbzdml->schema, sizeof(cachekey.schema));
	strlcpy(cachekey.table, dbzdml->table, sizeof(cachekey.table));

	cacheentry = (DataCacheEntry *) hash_search(dataCacheHash, &cachekey, HASH_ENTER, &found);
	if (found)
	{
		/* use the cached data type hash for lookup later */
		typeidhash = cacheentry->typeidhash;
		dbzdml->tableoid = cacheentry->tableoid;
		namejsonposhash = cacheentry->namejsonposhash;
		dbzdml->natts = cacheentry->natts;
	}
	else
	{
		schemaoid = get_namespace_oid(dbzdml->schema, false);
		if (!OidIsValid(schemaoid))
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for schema '%s'", dbzdml->schema);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

		dbzdml->tableoid = get_relname_relid(dbzdml->table, schemaoid);
		if (!OidIsValid(dbzdml->tableoid))
		{
			char * msg = palloc0(SYNCHDB_ERRMSG_SIZE);
			snprintf(msg, SYNCHDB_ERRMSG_SIZE, "no valid OID found for table '%s'", dbzdml->table);
			set_shm_connector_errmsg(myConnectorId, msg);

			/* trigger pg's error shutdown routine */
			elog(ERROR, "%s", msg);
		}

		/* populate cached information */
		strlcpy(cacheentry->key.schema, dbzdml->schema, sizeof(cachekey.schema));
		strlcpy(cacheentry->key.table, dbzdml->table, sizeof(cachekey.table));
		cacheentry->tableoid = dbzdml->tableoid;

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
		rel = table_open(dbzdml->tableoid, AccessShareLock);
		tupdesc = RelationGetDescr(rel);

		/* get primary key bitmapset */
		pkattrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_PRIMARY_KEY);

		/* cache tupdesc and save natts for later use */
		cacheentry->tupdesc = CreateTupleDescCopy(tupdesc);
		dbzdml->natts = tupdesc->natts;
		cacheentry->natts = dbzdml->natts;

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
		cacheentry->namejsonposhash = build_schema_jsonpos_hash(jb);
		namejsonposhash = cacheentry->namejsonposhash;
		if (!namejsonposhash)
		{
			/* dump the JSON change event as additional detail if available */
			if (synchdb_log_event_on_error && g_eventStr != NULL)
				elog(LOG, "%s", g_eventStr);

			elog(ERROR, "cannot parse schema section of change event JSON. Abort");
		}
	}

	switch(op)
	{
		case 'c':	/* create: data created after initial sync (INSERT) */
		case 'r':	/* read: initial data read */
		{
			/* sample payload:
			 * "payload": {
			 * 		"before": null,
			 * 		"after" : {
			 * 			"order_number": 10001,
			 * 			"order_date": 16816,
			 * 			"purchaser": 1001,
			 * 			"quantity": 1,
			 * 			"product_id": 102
			 * 		}
			 * 	}
			 *
			 * 	This parser expects the payload to contain only scalar values. In some special
			 * 	cases like geometry or oracle's number column type, the payload could contain
			 * 	sub element like:
			 * 	"after" : {
			 * 		"id"; 1,
			 * 		"g": {
			 * 			"wkb": "AQIAAAACAAAAAAAAAAAAAEAAAAAAAADwPwAAAAAAABhAAAAAAAAAGEA=",
			 * 			"srid": null
			 * 		},
			 * 		"h": null
			 * 		"i": {
             *          "scale": 0,
             *          "value": "AQ=="
             *      }
			 * 	}
			 * 	in this case, the parser will parse the entire sub element as string under the key "g"
			 * 	in the above example.
			 */
			Datum datum_elems[2] ={CStringGetTextDatum("payload"), CStringGetTextDatum("after")};
			dmlpayload = DatumGetJsonbP(jsonb_get_element(jb, &datum_elems[0], 2, &isnull, false));
			if (dmlpayload)
			{
				int pause = 0;
				it = JsonbIteratorInit(&dmlpayload->root);
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
									int pathsize = strlen("payload.after.") + strlen(key) + 1;
									char * tmpPath = (char *) palloc0 (pathsize);
									snprintf(tmpPath, pathsize, "payload.after.%s", key);
									getPathElementString(jb, tmpPath, &strinfo, false);
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
							elog(ERROR, "cannot find json schema data for column %s(%s). invalid json event?",
									colval->name, colval->remoteColumnName);

						dbzdml->columnValuesAfter = lappend(dbzdml->columnValuesAfter, colval);
						pfree(key);
						pfree(value);
						key = NULL;
						value = NULL;
					}
				}
			}
			break;
		}
		case 'd':	/* delete: data deleted after initial sync (DELETE) */
		{
			/* sample payload:
			 * "payload": {
			 * 		"before" : {
			 * 			"id": 1015,
			 * 			"first_name": "first",
			 * 			"last_name": "last",
			 * 			"email": "abc@mail.com"
			 * 		},
			 * 		"after": null
			 * 	}
			 */
			Datum datum_elems[2] = { CStringGetTextDatum("payload"), CStringGetTextDatum("before")};
			dmlpayload = DatumGetJsonbP(jsonb_get_element(jb, &datum_elems[0], 2, &isnull, false));
			if (dmlpayload)
			{
				int pause = 0;
				it = JsonbIteratorInit(&dmlpayload->root);
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
									int pathsize = strlen("payload.before.") + strlen(key) + 1;
									char * tmpPath = (char *) palloc0 (pathsize);
									snprintf(tmpPath, pathsize, "payload.before.%s", key);
									getPathElementString(jb, tmpPath, &strinfo, false);
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
									value = pnstrdup("NULL", strlen("NULL"));
									break;
								case jbvString:
									value = pnstrdup(v.val.string.val, v.val.string.len);
									break;
								case jbvNumeric:
									value = DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v.val.numeric)));
									break;
								case jbvBool:
									if (v.val.boolean)
										value = pnstrdup("true", strlen("true"));
									else
										value = pnstrdup("false", strlen("false"));
									break;
								case jbvBinary:
									value = pnstrdup("NULL", strlen("NULL"));
									break;
								default:
									value = pnstrdup("NULL", strlen("NULL"));
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
							elog(ERROR, "cannot find json schema data for column %s(%s). invalid json event?",
									colval->name, colval->remoteColumnName);

						dbzdml->columnValuesBefore = lappend(dbzdml->columnValuesBefore, colval);
						pfree(key);
						pfree(value);
						key = NULL;
						value = NULL;
					}
				}
			}
			break;
		}
		case 'u':	/* update: data updated after initial sync (UPDATE) */
		{
			/* sample payload:
			 * "payload": {
			 * 		"before" : {
			 * 			"order_number": 10006,
			 * 			"order_date": 17449,
			 * 			"purchaser": 1003,
			 * 			"quantity": 5,
			 * 			"product_id": 107
			 * 		},
			 * 		"after": {
			 * 			"order_number": 10006,
			 * 			"order_date": 17449,
			 * 			"purchaser": 1004,
			 * 			"quantity": 5,
			 * 			"product_id": 107
			 * 		}
			 * 	}
			 */
			int i = 0;
			for (i = 0; i < 2; i++)
			{
				/* need to parse before and after */
				if (i == 0)
				{
					Datum datum_elems[2] = { CStringGetTextDatum("payload"), CStringGetTextDatum("before")};
					dmlpayload = DatumGetJsonbP(jsonb_get_element(jb, &datum_elems[0], 2, &isnull, false));
				}
				else
				{
					Datum datum_elems[2] = { CStringGetTextDatum("payload"), CStringGetTextDatum("after")};
					dmlpayload = DatumGetJsonbP(jsonb_get_element(jb, &datum_elems[0], 2, &isnull, false));
				}
				if (dmlpayload)
				{
					int pause = 0;
					it = JsonbIteratorInit(&dmlpayload->root);
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
										int pathsize = (i == 0 ? strlen("payload.before.") + strlen(key) + 1 :
												strlen("payload.after.") + strlen(key) + 1);
										char * tmpPath = (char *) palloc0 (pathsize);
										if (i == 0)
											snprintf(tmpPath, pathsize, "payload.before.%s", key);
										else
											snprintf(tmpPath, pathsize, "payload.after.%s", key);
										getPathElementString(jb, tmpPath, &strinfo, false);
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
										value = pnstrdup("NULL", strlen("NULL"));
										break;
									case jbvString:
										value = pnstrdup(v.val.string.val, v.val.string.len);
										break;
									case jbvNumeric:
										value = DatumGetCString(DirectFunctionCall1(numeric_out, PointerGetDatum(v.val.numeric)));
										break;
									case jbvBool:
										if (v.val.boolean)
											value = pnstrdup("true", strlen("true"));
										else
											value = pnstrdup("false", strlen("false"));
										break;
									case jbvBinary:
										value = pnstrdup("NULL", strlen("NULL"));
										break;
									default:
										value = pnstrdup("NULL", strlen("NULL"));
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
								elog(ERROR, "cannot find json schema data for column %s(%s). invalid json event?",
										colval->name, colval->remoteColumnName);

							if (i == 0)
								dbzdml->columnValuesBefore = lappend(dbzdml->columnValuesBefore, colval);
							else
								dbzdml->columnValuesAfter = lappend(dbzdml->columnValuesAfter, colval);

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
		default:
		{
			elog(WARNING, "op %c not supported", op);
			if(strinfo.data)
				pfree(strinfo.data);

			destroyDBZDML(dbzdml);
			return NULL;
		}
	}

	/*
	 * finally, we need to sort dbzdml->columnValuesBefore and dbzdml->columnValuesAfter
	 * based on position to align with PostgreSQL's attnum
	 */
	if (dbzdml->columnValuesBefore != NULL)
		list_sort(dbzdml->columnValuesBefore, list_sort_cmp);

	if (dbzdml->columnValuesAfter != NULL)
		list_sort(dbzdml->columnValuesAfter, list_sort_cmp);

end:
	if (strinfo.data)
		pfree(strinfo.data);

	if (objid.data)
		pfree(objid.data);

	return dbzdml;
}

/*
 * fc_processDBZChangeEvent
 *
 * Main function to process Debezium change event
 */
int
fc_processDBZChangeEvent(const char * event, SynchdbStatistics * myBatchStats,
		int flag, const char * name, bool isfirst, bool islast)
{
	Datum jsonb_datum;
	Jsonb * jb;
	Jsonb * source = NULL;
	StringInfoData strinfo;
	ConnectorType type;
	MemoryContext tempContext, oldContext;
	bool islastsnapshot = false;
	int ret = -1;
	struct timeval tv;
	Datum datum_elems[2] = {CStringGetTextDatum("payload"), CStringGetTextDatum("source")};
	bool isnull;

	tempContext = AllocSetContextCreate(TopMemoryContext,
										"FORMAT_CONVERTER",
										ALLOCSET_DEFAULT_SIZES);

	oldContext = MemoryContextSwitchTo(tempContext);

	initStringInfo(&strinfo);

    /* Convert event string to JSONB */
    jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(event));
    jb = DatumGetJsonbP(jsonb_datum);

    /* Obtain source element - required */
	source = DatumGetJsonbP(jsonb_get_element(jb, &datum_elems[0], 2, &isnull, false));
    if (source)
    {
		JsonbValue * v = NULL;
		JsonbValue vbuf;
		char * tmp = NULL;

		/* payload.source.connector - required */
		v = getKeyJsonValueFromContainer(&source->root, "connector", strlen("connector"), &vbuf);
		if (!v)
		{
			elog(WARNING, "malformed change request - no connector attribute specified");
	    	increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
	    	MemoryContextSwitchTo(oldContext);
	    	MemoryContextDelete(tempContext);
			return -1;
		}
		tmp = pnstrdup(v->val.string.val, v->val.string.len);
		type = fc_get_connector_type(tmp);
		pfree(tmp);

		/* payload.source.snapshot - required */
		v = getKeyJsonValueFromContainer(&source->root, "snapshot", strlen("snapshot"), &vbuf);
		if (!v)
		{
			elog(WARNING, "malformed DML change request - no snapshot attribute specified");
			increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
	    	MemoryContextSwitchTo(oldContext);
	    	MemoryContextDelete(tempContext);
			return -1;
		}
		tmp = pnstrdup(v->val.string.val, v->val.string.len);
	    if (!strcmp(tmp, "true") || !strcmp(tmp, "last"))
	    {
	    	if (type == TYPE_OLR)
	    	{
	    		/* native openlog repliciator does not support schema synch mode yet- todo */
				if (get_shm_connector_stage_enum(myConnectorId) != STAGE_INITIAL_SNAPSHOT)
					set_shm_connector_stage(myConnectorId, STAGE_INITIAL_SNAPSHOT);
	    	}
	    	else
	    	{
				if (flag & CONNFLAG_SCHEMA_SYNC_MODE)
				{
					if (get_shm_connector_stage_enum(myConnectorId) != STAGE_SCHEMA_SYNC)
						set_shm_connector_stage(myConnectorId, STAGE_SCHEMA_SYNC);
				}
				else
				{
					if (get_shm_connector_stage_enum(myConnectorId) != STAGE_INITIAL_SNAPSHOT)
						set_shm_connector_stage(myConnectorId, STAGE_INITIAL_SNAPSHOT);
				}
	    	}
	    	if (!strcmp(tmp, "last"))
	    		islastsnapshot = true;
	    }
	    else
	    {
	    	if (get_shm_connector_stage_enum(myConnectorId) != STAGE_CHANGE_DATA_CAPTURE)
	    		set_shm_connector_stage(myConnectorId, STAGE_CHANGE_DATA_CAPTURE);
	    }
	    pfree(tmp);

#ifdef WITH_OLR
		elog(DEBUG1, "islastsnapshot %d, type %d", islastsnapshot, get_shm_connector_type_enum(myConnectorId));
	    if (islastsnapshot && get_shm_connector_type_enum(myConnectorId) == TYPE_OLR)
	    {
			orascn scn = 0, c_scn = 0;
			/*
			 * OLR connector only - get this event's scn and c_scn and set to
			 * OLR client so that it would start CDC from beyond this last
			 * snapshot event
			 */
			v = getKeyJsonValueFromContainer(&source->root, "scn", strlen("scn"), &vbuf);
			if (v && v->type != jbvNull)
			{
				if (v->type == jbvString)
				{
					tmp = pnstrdup(v->val.string.val, v->val.string.len);
					elog(WARNING, "scn %s", tmp);
					scn = strtoull(tmp, NULL, 10);
					pfree(tmp);
				}
				else
					elog(WARNING, "scn not a string...");
			}

			v = getKeyJsonValueFromContainer(&source->root, "commit_scn", strlen("commit_scn"), &vbuf);
			if (v && v->type != jbvNull)
			{
				if (v->type == jbvString)
				{
					tmp = pnstrdup(v->val.string.val, v->val.string.len);
					elog(WARNING, "commit_scn %s", tmp);
					c_scn = strtoull(tmp, NULL, 10);
					pfree(tmp);
				}
				else
					elog(WARNING, "commit scn not a string...");
			}
			elog(WARNING, "last snapshot event is at: scn=%llu c_scn=%llu", scn, c_scn);

			olr_client_set_scns(scn, c_scn > 0 ? c_scn : scn);
	    }
#endif
    }
    else
    {
    	elog(WARNING, "malformed change request - no source element");
    	increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
    	MemoryContextSwitchTo(oldContext);
    	MemoryContextDelete(tempContext);
		return -1;
    }

    /* Check if it's a DDL or DML event */
    getPathElementString(jb, "payload.op", &strinfo, true);
    if (!strcmp(strinfo.data, "NULL"))
    {
        /* Process DDL event */
    	DBZ_DDL * dbzddl = NULL;
    	PG_DDL * pgddl = NULL;

    	/* increment batch statistics */
    	increment_connector_statistics(myBatchStats, STATS_DDL, 1);

    	/* (1) parse */
    	set_shm_connector_state(myConnectorId, STATE_PARSING);
    	dbzddl = parseDBZDDL(jb, isfirst, islast);
    	if (!dbzddl)
    	{
    		set_shm_connector_state(myConnectorId, STATE_SYNCING);
    		increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
    		MemoryContextSwitchTo(oldContext);
    		MemoryContextDelete(tempContext);
    		return -1;
    	}

    	/* (2) convert */
    	set_shm_connector_state(myConnectorId, STATE_CONVERTING);
    	pgddl = convert2PGDDL(dbzddl, type);
    	if (!pgddl)
    	{
    		set_shm_connector_state(myConnectorId, STATE_SYNCING);
    		increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
    		destroyDBZDDL(dbzddl);
    		MemoryContextSwitchTo(oldContext);
    		MemoryContextDelete(tempContext);
    		return -1;
    	}

    	/* (3) execute */
    	set_shm_connector_state(myConnectorId, STATE_EXECUTING);
    	ret = ra_executePGDDL(pgddl, type);
    	if(ret)
    	{
    		set_shm_connector_state(myConnectorId, STATE_SYNCING);
    		increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
    		destroyDBZDDL(dbzddl);
    		destroyPGDDL(pgddl);
    		MemoryContextSwitchTo(oldContext);
    		MemoryContextDelete(tempContext);
    		return -1;
    	}

		/* (4) update attribute map table */
    	updateSynchdbAttribute(dbzddl, pgddl, get_shm_connector_type_enum(myConnectorId), name);

    	/* (5) record only the first and last change event's processing timestamps only */
    	if (islast)
    	{
			myBatchStats->stats_last_src_ts = dbzddl->src_ts_ms;
			myBatchStats->stats_last_dbz_ts = dbzddl->dbz_ts_ms;
			gettimeofday(&tv, NULL);
			myBatchStats->stats_last_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

    	if (isfirst)
    	{
			myBatchStats->stats_first_src_ts = dbzddl->src_ts_ms;
			myBatchStats->stats_first_dbz_ts = dbzddl->dbz_ts_ms;
			gettimeofday(&tv, NULL);
			myBatchStats->stats_first_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

		/* (6) clean up */
    	set_shm_connector_state(myConnectorId, (islastsnapshot &&
    			((flag & CONNFLAG_SCHEMA_SYNC_MODE) || (flag & CONNFLAG_EXIT_ON_SNAPSHOT_DONE)) ?
    			STATE_SCHEMA_SYNC_DONE : STATE_SYNCING));
    	destroyDBZDDL(dbzddl);
    	destroyPGDDL(pgddl);
    }
    else
    {
        /* Process DML event */
    	DBZ_DML * dbzdml = NULL;
    	PG_DML * pgdml = NULL;

    	/* increment batch statistics */
    	increment_connector_statistics(myBatchStats, STATS_DML, 1);

    	/* (1) parse */
    	set_shm_connector_state(myConnectorId, STATE_PARSING);
    	dbzdml = parseDBZDML(jb, strinfo.data[0], type, source, isfirst, islast);
    	if (!dbzdml)
		{
			set_shm_connector_state(myConnectorId, STATE_SYNCING);
			increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
			MemoryContextSwitchTo(oldContext);
			MemoryContextDelete(tempContext);
			return -1;
		}

    	/* (2) convert */
    	set_shm_connector_state(myConnectorId, STATE_CONVERTING);
    	pgdml = convert2PGDML(dbzdml, type);
    	if (!pgdml)
    	{
    		set_shm_connector_state(myConnectorId, STATE_SYNCING);
    		increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
    		destroyDBZDML(dbzdml);
    		MemoryContextSwitchTo(oldContext);
    		MemoryContextDelete(tempContext);
    		return -1;
    	}

    	/* (3) execute */
    	set_shm_connector_state(myConnectorId, STATE_EXECUTING);
    	ret = ra_executePGDML(pgdml, type, myBatchStats);
    	if(ret)
    	{
    		set_shm_connector_state(myConnectorId, STATE_SYNCING);
    		increment_connector_statistics(myBatchStats, STATS_BAD_CHANGE_EVENT, 1);
        	destroyDBZDML(dbzdml);
        	destroyPGDML(pgdml);
        	MemoryContextSwitchTo(oldContext);
        	MemoryContextDelete(tempContext);
    		return -1;
    	}

    	/* (4) record only the first and last change event's processing timestamps only */
    	if (islast)
    	{
			myBatchStats->stats_last_src_ts = dbzdml->src_ts_ms;
			myBatchStats->stats_last_dbz_ts = dbzdml->dbz_ts_ms;
			gettimeofday(&tv, NULL);
			myBatchStats->stats_last_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

    	if (isfirst)
    	{
			myBatchStats->stats_first_src_ts = dbzdml->src_ts_ms;
			myBatchStats->stats_first_dbz_ts = dbzdml->dbz_ts_ms;
			gettimeofday(&tv, NULL);
			myBatchStats->stats_first_pg_ts = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    	}

       	/* (5) clean up */
    	set_shm_connector_state(myConnectorId, (islastsnapshot &&
    			((flag & CONNFLAG_SCHEMA_SYNC_MODE) || (flag & CONNFLAG_EXIT_ON_SNAPSHOT_DONE)) ?
    			STATE_SCHEMA_SYNC_DONE : STATE_SYNCING));
    	destroyDBZDML(dbzdml);
    	destroyPGDML(pgdml);
    }

	if(strinfo.data)
		pfree(strinfo.data);

	if (jb)
		pfree(jb);

	MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(tempContext);
	return 0;
}
