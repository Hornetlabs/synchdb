/* Mapping reload migrates known physical objects before publishing new rules. */
#include "postgres.h"

#include "access/table.h"
#include "access/xact.h"
#include "catalog/attribute_store.h"
#include "catalog/namespace.h"
#include "catalog/pg_type_d.h"
#include "commands/dbcommands.h"
#include "converter/mapping_reload.h"
#include "executor/spi.h"
#include "executor/target_validate.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_type.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

typedef struct ReloadColumn
{
	AttrNumber attnum;
	char *name;
	char *type_sql;
	Oid type;
	int32 typmod;
	bool inherit_typmod;
} ReloadColumn;

typedef struct ReloadTable
{
	Oid relation;
	const char *source;
	char *schema;
	char *name;
	List *columns;
	List *commands;
} ReloadTable;

static const char *
reload_connector_type(ConnectorType type)
{
	switch (type)
	{
		case TYPE_MYSQL: return "mysql";
		case TYPE_ORACLE: return "oracle";
		case TYPE_OLR: return "olr";
		case TYPE_SQLSERVER: return "sqlserver";
		case TYPE_POSTGRES: return "postgres";
		default: elog(ERROR, "unsupported mapping reload connector type");
	}
	pg_unreachable();
}

static ReloadTable *
reload_table(List **tables, const SynchdbAttributeBinding *binding)
{
	ListCell *cell;
	ReloadTable *table;

	foreach(cell, *tables)
	{
		table = lfirst(cell);
		if (table->relation == binding->target_relation)
		{
			if (strcmp(table->source, binding->source_table) != 0)
				ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
								errmsg("mapping reload target has bindings for multiple source tables")));
			return table;
		}
	}
	table = palloc0(sizeof(*table));
	table->relation = binding->target_relation;
	table->source = binding->source_table;
	*tables = lappend(*tables, table);
	return table;
}

static ReloadColumn *
reload_column(ReloadTable *table, AttrNumber attnum)
{
	ListCell *cell;
	ReloadColumn *column;

	foreach(cell, table->columns)
	{
		column = lfirst(cell);
		if (column->attnum == attnum)
			return column;
	}
	column = palloc0(sizeof(*column));
	column->attnum = attnum;
	table->columns = lappend(table->columns, column);
	return column;
}

/* Snapshot compilation already validated the rule syntax. Resolve its type in
 * the same transaction as DDL; never interpolate an unchecked SQL expression. */
static void
reload_type(ReloadColumn *column, const char *destination)
{
	char *name = pstrdup(destination);
	char *separator = strchr(name, '|');
	int length = -1;
	Oid type;
	int32 typmod;
	TypeName *parsed;

	if (separator)
	{
		*separator++ = '\0';
		if (*separator)
			length = pg_strtoint32(separator);
	}
	/* SERIAL is a CREATE shortcut. ALTER preserves the column's existing
	 * sequence/default and changes only its physical integer type. */
	if (pg_strcasecmp(name, "serial") == 0 || pg_strcasecmp(name, "serial4") == 0)
		name = pstrdup("integer");
	else if (pg_strcasecmp(name, "bigserial") == 0 || pg_strcasecmp(name, "serial8") == 0)
		name = pstrdup("bigint");
	else if (pg_strcasecmp(name, "smallserial") == 0 || pg_strcasecmp(name, "serial2") == 0)
		name = pstrdup("smallint");
	parsed = typeStringToTypeName(name, NULL);
	typenameTypeIdAndMod(NULL, parsed, &type, &typmod);
	if (length != 0 && (type == BITOID || type == BPCHAROID) &&
		list_length(parsed->typmods) == 1 && exprLocation(linitial(parsed->typmods)) < 0)
		typmod = -1;
	if (get_typtype(type) == TYPTYPE_PSEUDO)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("mapping reload cannot use a pseudo type for a target column")));
	if (length > 0 && typmod == -1)
	{
		if (type == TIMESTAMPOID || type == TIMESTAMPTZOID ||
			type == TIMEOID || type == TIMETZOID)
		{
			if (length > 6)
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("mapping reload timestamp precision must be between 0 and 6")));
			typmod = length;
		}
		else
		{
			char *modified = psprintf("%s(%d)", name, length);

			(void) parseTypeString(modified, &type, &typmod, NULL);
		}
	}
	if (OidIsValid(column->type) && (column->type != type || column->typmod != typmod))
		ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
						errmsg("mapping reload has conflicting type rules for one target column")));
	column->type = type;
	column->typmod = typmod;
	column->inherit_typmod = length < 0 && typmod == -1;
	column->type_sql = format_type_extended(type, typmod, FORMAT_TYPE_TYPEMOD_GIVEN | FORMAT_TYPE_FORCE_QUALIFY);
}

static List *
reload_collect(const char *connector, ConnectorType type, const ObjectMap *rules, int count)
{
	List *bindings = synchdb_attribute_store_list_connector(connector,
			reload_connector_type(type), CurrentMemoryContext);
	List *tables = NIL;
	int i;

	for (i = 0; i < count; i++)
	{
		const ObjectMap *rule = &rules[i];
		SynchdbMappingKind kind;
		char *source;
		Oid matched_relation = InvalidOid;
		AttrNumber matched_attnum = InvalidAttrNumber;
		ListCell *cell;

		if (!rule->enabled)
			continue;
		kind = synchdb_mapping_kind(rule->objtype);
		if (kind == SYNCHDB_MAPPING_TRANSFORM)
			continue;
		source = pstrdup(rule->srcobj);
		/* The old catalog joined the complete source key to its physical
		 * binding. A |true/|false suffix therefore changed only the rules.
		 * Bindings do not store that source flag, so do not guess which branch
		 * applies and ALTER an existing column after stripping the suffix. */
		if (kind == SYNCHDB_MAPPING_DATATYPE && strchr(source, '|') != NULL)
			continue;
		/* Match legacy behavior: global datatype overrides affect subsequent
		 * schema planning; automatic ALTER is for a qualified source column. */
		if (kind == SYNCHDB_MAPPING_DATATYPE && strchr(source, '.') == NULL)
			continue;
		foreach(cell, bindings)
		{
			const SynchdbAttributeBinding *binding = lfirst(cell);
			char *key;
			ReloadTable *table;

			/* A dropped binding is a tombstone, never permission to reuse attnum. */
			if (!binding->source_table || !binding->source_column || !binding->source_type)
				continue;
			key = kind == SYNCHDB_MAPPING_TABLE ? (char *) binding->source_table :
				psprintf("%s.%s", binding->source_table, binding->source_column);
			if (strcmp(key, source) != 0)
				continue;
			if (OidIsValid(matched_relation) &&
				(matched_relation != binding->target_relation ||
				 (kind != SYNCHDB_MAPPING_TABLE && matched_attnum != binding->target_attnum)))
				ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
								errmsg("mapping source \"%s\" has multiple physical target bindings", source)));
			matched_relation = binding->target_relation;
			matched_attnum = binding->target_attnum;
			table = reload_table(&tables, binding);
			if (kind == SYNCHDB_MAPPING_TABLE)
			{
				char *database;

				synchdb_rule_parse_target_name(rule->dstobj, &database, &table->schema, &table->name);
				if (database && strcmp(database, get_database_name(MyDatabaseId)) != 0)
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("mapping reload cannot move a target to another database")));
			}
			else
			{
				ReloadColumn *column = reload_column(table, binding->target_attnum);

				if (kind == SYNCHDB_MAPPING_COLUMN)
					column->name = pstrdup(rule->dstobj);
				else
					reload_type(column, rule->dstobj);
			}
		}
	}
	/* A physical relation must not combine two distinct source-table bindings
	 * of this connector, even when only one of them has an enabled override. */
	{
		ListCell *table_cell;

		foreach(table_cell, tables)
		{
			ReloadTable *table = lfirst(table_cell);
			ListCell *binding_cell;

			foreach(binding_cell, bindings)
			{
				const SynchdbAttributeBinding *binding = lfirst(binding_cell);

				if (binding->source_type && binding->target_relation == table->relation &&
					(!binding->source_table || strcmp(binding->source_table, table->source) != 0))
					ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
									errmsg("mapping reload target has bindings for multiple source tables")));
			}
		}
	}
	return tables;
}

static int
reload_oid_compare(const void *left, const void *right)
{
	Oid a = *(const Oid *) left;
	Oid b = *(const Oid *) right;

	return a == b ? 0 : (a < b ? -1 : 1);
}

static void
reload_prepare(ReloadTable *table)
{
	Relation relation = table_open(table->relation, NoLock);
	TupleDesc descriptor = RelationGetDescr(relation);
	char *old_schema = get_namespace_name(RelationGetNamespace(relation));
	char *old_name = pstrdup(RelationGetRelationName(relation));
	char *qualified = quote_qualified_identifier(old_schema, old_name);
	ListCell *cell;

	synchdb_target_validate_relation(relation);
	foreach(cell, table->columns)
	{
		ReloadColumn *column = lfirst(cell);
		Form_pg_attribute attribute;
		char *name;

		if (column->attnum <= 0 || column->attnum > descriptor->natts ||
			TupleDescAttr(descriptor, column->attnum - 1)->attisdropped)
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
							errmsg("mapping reload target column binding is no longer valid")));
		attribute = TupleDescAttr(descriptor, column->attnum - 1);
		if (column->type_sql && column->inherit_typmod && attribute->atttypid == column->type)
		{
			column->typmod = attribute->atttypmod;
			column->type_sql = format_type_extended(column->type, column->typmod,
				FORMAT_TYPE_TYPEMOD_GIVEN | FORMAT_TYPE_FORCE_QUALIFY);
		}
		name = column->name ? column->name : NameStr(attribute->attname);
		if (column->name && strcmp(column->name, NameStr(attribute->attname)) != 0)
			table->commands = lappend(table->commands,
				psprintf("ALTER TABLE ONLY %s RENAME COLUMN %s TO %s", qualified,
					quote_identifier(NameStr(attribute->attname)), quote_identifier(column->name)));
		if (column->type_sql &&
			(attribute->atttypid != column->type || attribute->atttypmod != column->typmod))
			table->commands = lappend(table->commands,
				psprintf("ALTER TABLE ONLY %s ALTER COLUMN %s TYPE %s", qualified,
					quote_identifier(name), column->type_sql));
	}
	/* Column commands retain the old qualified name until all are applied.
	 * RENAME and SET SCHEMA preserve the OID/attnum-based attribute catalog. */
	if (table->name && (strcmp(table->name, old_name) != 0 || strcmp(table->schema, old_schema) != 0))
	{
		Oid namespace_oid = get_namespace_oid(table->schema, true);
		Oid collision = OidIsValid(namespace_oid) ? get_relname_relid(table->name, namespace_oid) : InvalidOid;

		if (OidIsValid(collision) && collision != table->relation)
			ereport(ERROR, (errcode(ERRCODE_DUPLICATE_TABLE),
							errmsg("mapping reload destination table already exists: %s.%s", table->schema, table->name)));
		if (!OidIsValid(namespace_oid))
			table->commands = lappend(table->commands,
				psprintf("CREATE SCHEMA IF NOT EXISTS %s", quote_identifier(table->schema)));
		if (strcmp(table->name, old_name) != 0 && strcmp(table->schema, old_schema) != 0 &&
			OidIsValid(get_relname_relid(table->name, RelationGetNamespace(relation))))
		{
			/* The final destination can be free even when an intermediate
			 * old_schema.new_name is occupied. Move first when possible; if
			 * both intermediate names are occupied, use a private temporary
			 * name for this transaction without touching either other table. */
			const char *move_name = old_name;
			char temporary[NAMEDATALEN];

			if (OidIsValid(namespace_oid) && OidIsValid(get_relname_relid(old_name, namespace_oid)))
			{
				int attempt;

				for (attempt = 0; attempt < 1000; attempt++)
				{
					snprintf(temporary, sizeof(temporary), "__synchdb_reload_%u_%d", table->relation, attempt);
					if (!OidIsValid(get_relname_relid(temporary, RelationGetNamespace(relation))) &&
						!OidIsValid(get_relname_relid(temporary, namespace_oid)))
						break;
				}
				if (attempt == 1000)
					ereport(ERROR, (errcode(ERRCODE_DUPLICATE_TABLE),
									errmsg("mapping reload could not reserve an intermediate table name")));
				table->commands = lappend(table->commands,
					psprintf("ALTER TABLE ONLY %s RENAME TO %s", qualified, quote_identifier(temporary)));
				move_name = temporary;
			}
			table->commands = lappend(table->commands,
				psprintf("ALTER TABLE ONLY %s SET SCHEMA %s",
					quote_qualified_identifier(old_schema, move_name), quote_identifier(table->schema)));
			table->commands = lappend(table->commands,
				psprintf("ALTER TABLE ONLY %s RENAME TO %s",
					quote_qualified_identifier(table->schema, move_name), quote_identifier(table->name)));
		}
		else
		{
			if (strcmp(table->name, old_name) != 0)
				table->commands = lappend(table->commands,
					psprintf("ALTER TABLE ONLY %s RENAME TO %s", qualified, quote_identifier(table->name)));
			if (strcmp(table->schema, old_schema) != 0)
				table->commands = lappend(table->commands,
					psprintf("ALTER TABLE ONLY %s SET SCHEMA %s",
						quote_qualified_identifier(old_schema, table->name), quote_identifier(table->schema)));
		}
	}
	table_close(relation, NoLock);
}

void
synchdb_mapping_reload_targets(const char *connector, ConnectorType type,
							   const ObjectMap *rules, int count)
{
	List *tables;
	ListCell *cell;
	Oid *locks;
	int i = 0;

	if (!IsTransactionState() || !ActiveSnapshotSet())
		elog(ERROR, "mapping target migration requires a transaction and snapshot");
	tables = reload_collect(connector, type, rules, count);
	if (tables == NIL)
		return;
	locks = palloc(mul_size(list_length(tables), sizeof(*locks)));
	foreach(cell, tables)
		locks[i++] = ((ReloadTable *) lfirst(cell))->relation;
	qsort(locks, i, sizeof(*locks), reload_oid_compare);
	for (i = 0; i < list_length(tables); i++)
		LockRelationOid(locks[i], AccessExclusiveLock);
	foreach(cell, tables)
		reload_prepare(lfirst(cell));
	foreach(cell, tables)
	{
		ReloadTable *table = lfirst(cell);
		ListCell *command;

		foreach(command, table->commands)
		{
			if (SPI_connect() != SPI_OK_CONNECT)
				elog(ERROR, "could not connect to SPI for mapping reload");
			if (SPI_execute(lfirst(command), false, 0) != SPI_OK_UTILITY)
				elog(ERROR, "mapping reload did not execute as a utility statement");
			if (SPI_finish() != SPI_OK_FINISH)
				elog(ERROR, "could not finish SPI for mapping reload");
			CommandCounterIncrement();
		}
	}
}
