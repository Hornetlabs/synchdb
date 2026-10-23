/* Full source images are matched by source identity, never by column counts. */
#include "postgres.h"

#include <ctype.h>

#include "access/genam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/attribute_store.h"
#include "catalog/namespace.h"
#include "catalog/dependency.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_type_d.h"
#include "commands/dbcommands.h"
#include "converter/ddl_plan.h"
#include "executor/pg_compat.h"
#include "executor/spi.h"
#include "executor/target_validate.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "runtime/config.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "parser/parse_type.h"

typedef struct DdlColumn
{
	const SynchdbDbzSchemaColumn *source;
	char *name;
	char *create_type;
	char *alter_type;
	Oid type;
	int32 typmod;
	AttrNumber old_attnum;
} DdlColumn;

typedef struct DdlTable
{
	const SynchdbDbzTableChange *source;
	char *schema;
	char *name;
	char *old_schema;
	char *old_name;
	char *qualified;
	Oid relation_oid;
	Oid old_namespace_oid;
	Oid namespace_oid;
	List *columns;
	List *old_bindings;
	List *commands;
	bool create_schema;
	bool changed;
} DdlTable;

struct SynchdbDdlPlan
{
	MemoryContext context;
	const SynchdbRuleSnapshot *rules;
	uint64 generation;
	char *connector;
	char *connector_type;
	List *tables;
	bool applied;
};

static void
ddl_require_transaction(void)
{
	if (!IsTransactionState() || !ActiveSnapshotSet())
		elog(ERROR, "schema planning requires a transaction and active snapshot");
}

static void
ddl_unsupported(const char *table, const char *reason)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot apply source schema image for \"%s\"", table),
			 errdetail_internal("%s", reason)));
}

static void
ddl_identifier(const char *value, const char *field)
{
	if (value == NULL || value[0] == '\0')
		ereport(ERROR, (errcode(ERRCODE_INVALID_NAME),
						errmsg("target %s must not be empty", field)));
	if (strlen(value) >= NAMEDATALEN)
		ereport(ERROR,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("target %s exceeds PostgreSQL identifier capacity", field),
				 errdetail("The identifier supports at most %d bytes.", NAMEDATALEN - 1)));
}

static char *
ddl_normalized(const char *source)
{
	char *name = pstrdup(source);
	char *at;

	for (at = name; *at; at++)
		if (synchdb_letter_casing_strategy == LCS_NORMALIZE_LOWERCASE)
			*at = pg_tolower((unsigned char) *at);
		else if (synchdb_letter_casing_strategy == LCS_NORMALIZE_UPPERCASE)
			*at = pg_toupper((unsigned char) *at);
	return name;
}

static void
ddl_target_name(const SynchdbRuleSnapshot *rules, const SynchdbDbzTableId *source,
				char **schema, char **table)
{
	const char *mapped = synchdb_rule_object(rules, SYNCHDB_MAPPING_TABLE, source->key);

	if (mapped)
	{
		char *database;

		synchdb_rule_parse_target_name(mapped, &database, schema, table);
		if (database)
		{
			char *current_database = get_database_name(MyDatabaseId);

			if (!current_database || strcmp(database, current_database) != 0)
				ddl_unsupported(source->key, "cross-database target table mappings are not supported");
		}
	}
	else
	{
		*schema = ddl_normalized(source->database);
		*table = ddl_normalized(source->table);
	}
	ddl_identifier(*schema, "schema");
	ddl_identifier(*table, "table");
}

static Oid
ddl_lookup_relation(const char *schema, const char *table)
{
	Oid namespace_oid = get_namespace_oid(schema, true);

	return OidIsValid(namespace_oid) ? get_relname_relid(table, namespace_oid) : InvalidOid;
}

static const char *
ddl_connector_type(ConnectorType type)
{
	switch (type)
	{
		case TYPE_MYSQL: return "mysql";
		case TYPE_ORACLE: return "oracle";
		case TYPE_OLR: return "olr";
		case TYPE_SQLSERVER: return "sqlserver";
		case TYPE_POSTGRES: return "postgres";
		default: elog(ERROR, "full schema planning does not support connector type %d", type);
	}
	pg_unreachable();
}

static bool
ddl_has_payload(const JsonbValue *value)
{
	if (value == NULL || value->type == jbvNull)
		return false;
	if (value->type == jbvBinary)
		return JsonContainerSize(value->val.binary.data) != 0;
	return true;
}

static void
ddl_validate_source(const SynchdbDbzTableChange *source)
{
	static const char *const unsupported[] = {
		"constraints", "uniqueConstraints", "uniqueKeys", "foreignKeys",
		"checkConstraints", "indexes"
	};
	ListCell *cell;
	size_t i;

	if (source->operation == SYNCHDB_DBZ_SCHEMA_DROP)
		return;
	if (!source->primary_key_known)
		ddl_unsupported(source->id.key, "primaryKeyColumnNames must be present; missing metadata cannot mean an empty primary key");
	if (list_length(source->columns) > MaxHeapAttributeNumber)
		ddl_unsupported(source->id.key, "source image exceeds the supported target column count");
	for (i = 0; i < lengthof(unsupported); i++)
	{
		JsonbValue value;
		JsonbValue *member = getKeyJsonValueFromContainer(&source->table_document->root,
				unsupported[i], strlen(unsupported[i]), &value);

		if (ddl_has_payload(member))
			ddl_unsupported(source->id.key, "source constraints other than primary keys are not supported by the schema planner");
	}
	foreach(cell, source->columns)
	{
		const SynchdbDbzSchemaColumn *column = lfirst(cell);
		ListCell *key;

		/* Debezium also marks AUTO_INCREMENT/identity columns as generated.
		 * Those use the existing auto_incremented type mapping; only computed
		 * columns require a generation expression absent from this image. */
		if (column->generated && !column->auto_incremented)
			ddl_unsupported(source->id.key, "source generated-column expressions cannot be reconstructed from this schema image");
		if (column->optional)
			foreach(key, source->primary_key)
				if (strcmp(lfirst(key), column->name) == 0)
					ddl_unsupported(source->id.key, "source primary-key columns must be declared nonnullable");
		for (i = 0; i < lengthof(unsupported); i++)
		{
			JsonbValue value;
			JsonbValue *member = column->document ?
				getKeyJsonValueFromContainer(&column->document->root,
					unsupported[i], strlen(unsupported[i]), &value) : NULL;

			if (ddl_has_payload(member))
				ddl_unsupported(source->id.key,
					"explicit source column constraints are not supported by the schema planner");
		}
	}
}

static char *
ddl_source_type(const SynchdbDbzSchemaColumn *column, ConnectorType type)
{
	char *name = pstrdup(column->type_name);
	char *read;
	char *write;
	int depth = 0;

	if (type != TYPE_ORACLE && type != TYPE_OLR)
		return name;
	/* Oracle typeName can contain precision as well as separate length/scale. */
	for (read = write = name; *read; read++)
	{
		if (*read == '(')
			depth++;
		else if (*read == ')')
		{
			if (depth == 0)
				ddl_unsupported(column->name, "unbalanced Oracle type precision");
			depth--;
		}
		else if (depth == 0)
			*write++ = *read;
	}
	if (depth != 0)
		ddl_unsupported(column->name, "unbalanced Oracle type precision");
	*write = '\0';
	return name;
}

static DdlColumn *
ddl_column(const SynchdbDbzTableChange *table, const SynchdbDbzSchemaColumn *source,
		   ConnectorType type, const SynchdbRuleSnapshot *rules)
{
	DdlColumn *column = palloc0(sizeof(*column));
	char *key = synchdb_source_column_key(table->id.key, source->name);
	const char *mapped = synchdb_rule_object(rules, SYNCHDB_MAPPING_COLUMN, key);
	const SynchdbRuleTypeMapping *mapping;
	char *source_type = ddl_source_type(source, type);
	char *lookup = source_type;
	char *name;
	int length = source->has_length ? source->length : -1;
	int scale = source->has_scale ? source->scale : 0;
	bool use_source_modifiers;
	Oid serial_type = InvalidOid;

	column->source = source;
	/* A column rule is a physical column name, not SQL syntax. */
	column->name = mapped ? pstrdup(mapped) : ddl_normalized(source->name);
	ddl_identifier(column->name, "column");
	if ((type == TYPE_MYSQL || type == TYPE_SQLSERVER) && length == 1 &&
		pg_strcasecmp(source_type, "bit") == 0)
		lookup = psprintf("%s(1)", source_type);
	else if ((type == TYPE_ORACLE || type == TYPE_OLR) && scale == 0 && pg_strcasecmp(source_type, "number") == 0)
		lookup = psprintf("%s(%d,0)", source_type, length);
	mapping = synchdb_rule_type(rules, type, key, lookup, source->auto_incremented);
	if (mapping == NULL && lookup != source_type)
		mapping = synchdb_rule_type(rules, type, key, source_type, source->auto_incremented);
	name = pstrdup(mapping ? mapping->name : source_type);
	use_source_modifiers = mapping == NULL || mapping->length == -1;
	if (mapping && mapping->length >= 0)
		length = mapping->length;
	if (pg_strcasecmp(name, "serial") == 0 || pg_strcasecmp(name, "serial4") == 0)
		serial_type = INT4OID;
	else if (pg_strcasecmp(name, "bigserial") == 0 || pg_strcasecmp(name, "serial8") == 0)
		serial_type = INT8OID;
	else if (pg_strcasecmp(name, "smallserial") == 0 || pg_strcasecmp(name, "serial2") == 0)
		serial_type = INT2OID;
	if (OidIsValid(serial_type))
	{
		column->type = serial_type;
		column->typmod = -1;
		column->create_type = name;
	}
	else
	{
		TypeName *parsed = typeStringToTypeName(name, NULL);

		typenameTypeIdAndMod(NULL, parsed, &column->type, &column->typmod);
		/* SQL grammar supplies an implicit (1) for bare BIT and CHAR. It
		 * must not hide an inherited source width; explicit modifiers have
		 * a source location and retain their authored meaning. */
		if ((use_source_modifiers || (mapping && mapping->length > 0)) &&
			(column->type == BITOID || column->type == BPCHAROID) &&
			list_length(parsed->typmods) == 1 &&
			exprLocation(linitial(parsed->typmods)) < 0)
			column->typmod = -1;
		if (get_typtype(column->type) == TYPTYPE_PSEUDO)
			ddl_unsupported(table->id.key, "pseudo types are not supported target columns");
		if (column->typmod == -1 && (use_source_modifiers || length > 0 ||
			(type == TYPE_SQLSERVER && source->has_scale)))
		{
			char *modified = NULL;

			if ((column->type == VARCHAROID || column->type == BPCHAROID ||
				 column->type == BITOID || column->type == VARBITOID) && length > 0)
				modified = psprintf("%s(%d)", name, length);
			else if (column->type == NUMERICOID && length > 0)
				modified = psprintf("%s(%d,%d)", name, length, scale);
			else if (column->type == TIMESTAMPOID || column->type == TIMESTAMPTZOID ||
					 column->type == TIMEOID || column->type == TIMETZOID)
			{
				int precision = type == TYPE_SQLSERVER ?
					(source->has_scale ? scale : -1) : length;

				/* Oracle JDBC reports the byte width of zoned timestamps in
				 * length and their fractional precision in scale. Streamed DDL
				 * can instead carry that precision in length without a scale. */
				if ((type == TYPE_ORACLE || type == TYPE_OLR) &&
					use_source_modifiers && source->has_scale &&
					(pg_strncasecmp(source_type, "timestamp", 9) == 0 ||
					 pg_strcasecmp(source_type, "date") == 0))
					precision = scale;

				/* SQL Server defaults TIME/DATETIME2/DATETIMEOFFSET to
				 * seven digits. Keep the established microsecond mapping;
				 * the value codec and target input round to that resolution. */
				if (type == TYPE_SQLSERVER && precision == 7)
					precision = 6;
				if (precision >= 0 && precision <= 6)
					/* Builtin datetime typmods are exactly fractional precision.
					 * Formatting below puts it before WITH/WITHOUT TIME ZONE. */
					column->typmod = precision;
				else if (precision > 6)
					ddl_unsupported(table->id.key, "source temporal precision exceeds PostgreSQL's six fractional digits");
			}
			if (modified)
				(void) parseTypeString(modified, &column->type, &column->typmod, NULL);
		}
		column->create_type = format_type_extended(column->type, column->typmod,
				FORMAT_TYPE_TYPEMOD_GIVEN | FORMAT_TYPE_FORCE_QUALIFY);
	}
	column->alter_type = format_type_extended(column->type, column->typmod,
			FORMAT_TYPE_TYPEMOD_GIVEN | FORMAT_TYPE_FORCE_QUALIFY);
	return column;
}

char *
synchdb_ddl_render_column_type(const char *source_table,
							const SynchdbDbzSchemaColumn *source,
							ConnectorType type, const SynchdbRuleSnapshot *rules)
{
	SynchdbDbzTableChange table = {0};
	DdlColumn *column;

	ddl_require_transaction();
	if (!source_table || !source)
		elog(ERROR, "type rendering requires a source table and column");
	table.id.key = (char *) source_table;
	column = ddl_column(&table, source, type, rules);
	return column->create_type;
}

static int
ddl_compare_oids(const void *left, const void *right)
{
	Oid a = *(const Oid *) left;
	Oid b = *(const Oid *) right;

	return a < b ? -1 : a > b ? 1 : 0;
}

static DdlColumn *
ddl_find_column(const DdlTable *table, const char *source_name)
{
	ListCell *cell;

	foreach(cell, table->columns)
	{
		DdlColumn *column = lfirst(cell);

		if (strcmp(column->source->name, source_name) == 0)
			return column;
	}
	return NULL;
}

static bool
ddl_has_old_attribute(const DdlTable *table, AttrNumber attnum)
{
	ListCell *cell;

	foreach(cell, table->columns)
		if (((DdlColumn *) lfirst(cell))->old_attnum == attnum)
			return true;
	return false;
}

static char *
ddl_primary_key_sql(const DdlTable *table)
{
	StringInfoData sql;
	ListCell *cell;
	bool first = true;

	initStringInfo(&sql);
	foreach(cell, table->source->primary_key)
	{
		DdlColumn *column = ddl_find_column(table, lfirst(cell));

		if (!column)
			elog(ERROR, "source primary key references an absent planned column");
		appendStringInfo(&sql, "%s%s", first ? "" : ",", quote_identifier(column->name));
		first = false;
	}
	return sql.data;
}

static void
ddl_command(DdlTable *table, char *command)
{
	table->commands = lappend(table->commands, command);
	table->changed = true;
}

static void
ddl_create(DdlTable *table)
{
	StringInfoData sql;
	ListCell *cell;
	bool first = true;

	initStringInfo(&sql);
	appendStringInfo(&sql, "CREATE TABLE %s (", table->qualified);
	foreach(cell, table->columns)
	{
		DdlColumn *column = lfirst(cell);

		appendStringInfo(&sql, "%s%s %s%s", first ? "" : ",",
				quote_identifier(column->name), column->create_type,
				column->source->optional ? "" : " NOT NULL");
		first = false;
	}
	if (table->source->primary_key != NIL)
		appendStringInfo(&sql, ",PRIMARY KEY (%s)", ddl_primary_key_sql(table));
	appendStringInfoChar(&sql, ')');
	ddl_command(table, sql.data);
}

static List *
ddl_current_primary_key(Relation relation, char **constraint_name)
{
	Oid index_oid = synchdb_pg_primary_key_index(relation, true);
	List *columns = NIL;

	*constraint_name = NULL;
	if (OidIsValid(index_oid))
	{
		Relation index = index_open(index_oid, AccessShareLock);
		Oid constraint = get_index_constraint(index_oid);
		int i;

		if (!index->rd_index->indisvalid || !OidIsValid(constraint))
			ddl_unsupported(RelationGetRelationName(relation), "target primary key is not a valid constraint");
		*constraint_name = get_constraint_name(constraint);
		for (i = 0; i < IndexRelationGetNumberOfKeyAttributes(index); i++)
			columns = lappend_int(columns, index->rd_index->indkey.values[i]);
		index_close(index, NoLock);
	}
	return columns;
}

static bool
ddl_same_primary_key(const DdlTable *table, List *old_key)
{
	ListCell *old;
	ListCell *desired;

	if (list_length(old_key) != list_length(table->source->primary_key))
		return false;
	forboth(old, old_key, desired, table->source->primary_key)
		if (lfirst_int(old) != ddl_find_column(table, lfirst(desired))->old_attnum)
			return false;
	return true;
}

static bool
ddl_table_empty(Relation relation)
{
	bool empty;
	TableScanDesc scan;
	TupleTableSlot *slot;

	/* AccessExclusiveLock has drained concurrent writers. SnapshotSelf sees
	 * their committed rows and our own rows even under an older batch/RR
	 * snapshot; ordinary readonly SPI would incorrectly call that table empty. */
	slot = table_slot_create(relation, NULL);
	scan = table_beginscan(relation, SnapshotSelf, 0, NULL);
	empty = !table_scan_getnextslot(scan, ForwardScanDirection, slot);
	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);
	return empty;
}

static void
ddl_bind_existing(DdlTable *table, Relation relation)
{
	ListCell *cell;
	ListCell *other;
	TupleDesc descriptor = RelationGetDescr(relation);
	int new_sources = 0;
	int dropped = 0;

	foreach(cell, table->columns)
	{
		DdlColumn *column = lfirst(cell);

		foreach(other, table->old_bindings)
		{
			const SynchdbAttributeBinding *binding = lfirst(other);

			if (binding->source_column == NULL || binding->source_type == NULL ||
				strcmp(binding->source_column, column->source->name) != 0)
				continue;
			if (binding->target_relation != table->relation_oid || binding->target_attnum <= 0 ||
				binding->target_attnum > descriptor->natts ||
				TupleDescAttr(descriptor, binding->target_attnum - 1)->attisdropped)
				ddl_unsupported(table->source->id.key, "persisted source binding no longer names a live attribute of this target");
			if (column->old_attnum != 0)
				ddl_unsupported(table->source->id.key, "source column has more than one persisted physical binding");
			column->old_attnum = binding->target_attnum;
		}
		if (column->old_attnum == 0)
		{
			new_sources++;
			column->old_attnum = get_attnum(table->relation_oid, column->name);
		}
		if (column->old_attnum < 0)
			ddl_unsupported(table->source->id.key, "source columns cannot bind target system columns");
		if (column->old_attnum > 0)
		{
			Form_pg_attribute attribute = TupleDescAttr(descriptor, column->old_attnum - 1);

			if (attribute->attgenerated != '\0')
				ddl_unsupported(table->source->id.key, "source columns cannot bind target generated columns");
		}
		foreach(other, table->columns)
		{
			DdlColumn *earlier = lfirst(other);

			if (earlier == column)
				break;
			if (strcmp(earlier->name, column->name) == 0 ||
				(column->old_attnum && earlier->old_attnum == column->old_attnum))
				ddl_unsupported(table->source->id.key, "multiple source columns resolve to one target column");
		}
	}
	foreach(cell, table->old_bindings)
	{
		const SynchdbAttributeBinding *binding = lfirst(cell);

		if (binding->source_type == NULL)
			continue;
		if (binding->target_relation != table->relation_oid)
			ddl_unsupported(table->source->id.key, "source table metadata spans more than one target relation");
		if (binding->target_attnum > 0 && binding->target_attnum <= descriptor->natts &&
			!TupleDescAttr(descriptor, binding->target_attnum - 1)->attisdropped &&
			!ddl_has_old_attribute(table, binding->target_attnum))
			dropped++;
	}
	if (new_sources && dropped)
		ddl_unsupported(table->source->id.key,
			"a removed and an added source column are ambiguous with a source column rename; an explicit physical mapping is required");
}

static void
ddl_existing(DdlTable *table, Relation relation)
{
	TupleDesc descriptor = RelationGetDescr(relation);
	char *constraint;
	List *old_key;
	bool same_key;
	ListCell *cell;

	ddl_bind_existing(table, relation);
	old_key = ddl_current_primary_key(relation, &constraint);
	same_key = ddl_same_primary_key(table, old_key);
	if (table->source->operation == SYNCHDB_DBZ_SCHEMA_CREATE)
	{
		foreach(cell, table->columns)
		{
			DdlColumn *column = lfirst(cell);
			Form_pg_attribute attribute;

			if (!column->old_attnum)
				ddl_unsupported(table->source->id.key, "existing target does not contain every source column");
			attribute = TupleDescAttr(descriptor, column->old_attnum - 1);
			if (strcmp(NameStr(attribute->attname), column->name) != 0 ||
				attribute->atttypid != column->type || attribute->atttypmod != column->typmod ||
				attribute->attnotnull != !column->source->optional)
				ddl_unsupported(table->source->id.key, "existing target column is incompatible with the CREATE image");
		}
		if (!same_key)
			ddl_unsupported(table->source->id.key, "existing target primary key is incompatible with the CREATE image");
		return;
	}
	if (!same_key && constraint)
		ddl_command(table, psprintf("ALTER TABLE ONLY %s DROP CONSTRAINT %s RESTRICT",
				table->qualified, quote_identifier(constraint)));
	foreach(cell, table->old_bindings)
	{
		const SynchdbAttributeBinding *binding = lfirst(cell);

		if (binding->source_type && binding->target_relation == table->relation_oid &&
			binding->target_attnum > 0 && binding->target_attnum <= descriptor->natts &&
			!TupleDescAttr(descriptor, binding->target_attnum - 1)->attisdropped &&
			!ddl_has_old_attribute(table, binding->target_attnum))
			ddl_command(table, psprintf("ALTER TABLE ONLY %s DROP COLUMN %s RESTRICT",
					table->qualified,
					quote_identifier(NameStr(TupleDescAttr(descriptor, binding->target_attnum - 1)->attname))));
	}
	foreach(cell, table->columns)
	{
		DdlColumn *column = lfirst(cell);
		Form_pg_attribute attribute;

		if (column->old_attnum == 0)
		{
			if ((column->source->has_default || column->source->auto_incremented ||
				 !column->source->optional) && !ddl_table_empty(relation))
				ddl_unsupported(table->source->id.key,
					"adding this column to a nonempty target requires an explicit target backfill/default policy");
			ddl_command(table, psprintf("ALTER TABLE ONLY %s ADD COLUMN %s %s%s",
					table->qualified, quote_identifier(column->name), column->create_type,
					column->source->optional ? "" : " NOT NULL"));
			continue;
		}
		attribute = TupleDescAttr(descriptor, column->old_attnum - 1);
		if (strcmp(NameStr(attribute->attname), column->name) != 0)
		{
			AttrNumber collision = get_attnum(table->relation_oid, column->name);

			if (collision != InvalidAttrNumber && collision != column->old_attnum)
				ddl_unsupported(table->source->id.key, "target column rename collides with an existing physical column");
			ddl_command(table, psprintf("ALTER TABLE ONLY %s RENAME COLUMN %s TO %s",
					table->qualified, quote_identifier(NameStr(attribute->attname)),
					quote_identifier(column->name)));
		}
		if (attribute->atttypid != column->type || attribute->atttypmod != column->typmod)
			ddl_command(table, psprintf("ALTER TABLE ONLY %s ALTER COLUMN %s TYPE %s",
					table->qualified, quote_identifier(column->name), column->alter_type));
		if (attribute->attnotnull != !column->source->optional)
			ddl_command(table, psprintf("ALTER TABLE ONLY %s ALTER COLUMN %s %s NOT NULL",
					table->qualified, quote_identifier(column->name),
					column->source->optional ? "DROP" : "SET"));
	}
	if (!same_key && table->source->primary_key != NIL)
		ddl_command(table, psprintf("ALTER TABLE ONLY %s ADD PRIMARY KEY (%s)",
				table->qualified, ddl_primary_key_sql(table)));
}

static void
ddl_prepare_table(SynchdbDdlPlan *plan, DdlTable *table)
{
	Relation relation;
	ListCell *cell;
	const char *source_key = table->source->previous_id ?
		table->source->previous_id->key : table->source->id.key;

	table->old_bindings = synchdb_attribute_store_list_source_table(plan->connector,
			plan->connector_type, source_key, plan->context);
	/* A reload changes routing, not ownership of an existing target. In
	 * particular, DROP must not delete the newly mapped table while forgetting
	 * a source binding that still belongs to another physical relation. */
	foreach(cell, table->old_bindings)
	{
		const SynchdbAttributeBinding *binding = lfirst(cell);

		if (binding->source_type && binding->target_relation != table->relation_oid)
			ddl_unsupported(table->source->id.key,
				"persisted source bindings belong to a different physical target; rule reload does not migrate target structure");
	}
	if (OidIsValid(table->relation_oid))
	{
		List *owners = synchdb_attribute_store_list_target_relation(plan->connector,
				plan->connector_type, table->relation_oid, plan->context);

		foreach(cell, owners)
		{
			const SynchdbAttributeBinding *binding = lfirst(cell);

			if (binding->source_type &&
				(binding->source_table == NULL ||
				 (strcmp(binding->source_table, table->source->id.key) != 0 &&
				  strcmp(binding->source_table, source_key) != 0)))
				ddl_unsupported(table->source->id.key,
					"target relation already has bindings for another source table of this connector");
		}
	}
	if (!OidIsValid(table->relation_oid))
	{
		if (table->source->operation == SYNCHDB_DBZ_SCHEMA_ALTER)
			ddl_unsupported(table->source->id.key, "ALTER target relation is missing");
		if (table->source->operation == SYNCHDB_DBZ_SCHEMA_CREATE)
		{
			table->create_schema = !OidIsValid(get_namespace_oid(table->schema, true));
			ddl_create(table);
		}
		return;
	}
	relation = table_open(table->relation_oid, NoLock);
	if (strcmp(RelationGetRelationName(relation), table->old_name) != 0 ||
		RelationGetNamespace(relation) != table->old_namespace_oid ||
		strcmp(get_namespace_name(RelationGetNamespace(relation)), table->old_schema) != 0)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("target relation changed while building schema plan")));
	synchdb_target_validate_relation(relation);
	if (table->source->operation == SYNCHDB_DBZ_SCHEMA_DROP)
		ddl_command(table, psprintf("DROP TABLE %s RESTRICT", table->qualified));
	else
		ddl_existing(table, relation);
	/* Column commands refer to the old table name. Perform a proven table rename
	 * last; the physical OID and all bindings remain the same. */
	if (table->source->previous_id &&
		(strcmp(table->old_name, table->name) != 0 || strcmp(table->old_schema, table->schema) != 0))
	{
		Oid collision = ddl_lookup_relation(table->schema, table->name);
		char *old_qualified = quote_qualified_identifier(table->old_schema, table->old_name);

		if (OidIsValid(collision) && collision != table->relation_oid)
			ddl_unsupported(table->source->id.key, "table rename destination already exists");
		if (!OidIsValid(get_namespace_oid(table->schema, true)))
			table->create_schema = true;
		if (strcmp(table->old_name, table->name) != 0)
			ddl_command(table, psprintf("ALTER TABLE ONLY %s RENAME TO %s",
					old_qualified, quote_identifier(table->name)));
		if (strcmp(table->old_schema, table->schema) != 0)
			ddl_command(table, psprintf("ALTER TABLE ONLY %s SET SCHEMA %s",
					quote_qualified_identifier(table->old_schema, table->name),
					quote_identifier(table->schema)));
	}
	table_close(relation, NoLock);
}

SynchdbDdlPlan *
synchdb_ddl_plan_build(const char *connector, ConnectorType type,
					   const SynchdbRuleSnapshot *rules,
					   const SynchdbDbzSchemaChange *source, MemoryContext parent)
{
	MemoryContext context;
	MemoryContext previous;
	SynchdbDdlPlan *plan;
	ListCell *cell;
	Oid *locks;
	int lock_count = 0;
	int i;

	ddl_require_transaction();
	if (!connector || !source || !parent ||
		(rules && (strcmp(connector, synchdb_rule_snapshot_connector(rules)) != 0 ||
				   synchdb_rule_snapshot_type(rules) != type)))
		elog(ERROR, "invalid schema planner inputs");
	context = AllocSetContextCreate(parent, "SynchDB schema plan", ALLOCSET_DEFAULT_SIZES);
	previous = MemoryContextSwitchTo(context);
	plan = palloc0(sizeof(*plan));
	plan->context = context;
	plan->rules = rules;
	plan->generation = synchdb_rule_snapshot_generation(rules);
	plan->connector = pstrdup(connector);
	plan->connector_type = pstrdup(ddl_connector_type(type));
	PG_TRY();
	{
		locks = palloc(mul_size(Max(list_length(source->tables), 1), sizeof(*locks)));
		foreach(cell, source->tables)
		{
			const SynchdbDbzTableChange *change = lfirst(cell);
			DdlTable *table = palloc0(sizeof(*table));
			ListCell *column_cell;
			ListCell *other;

			ddl_validate_source(change);
			table->source = change;
			ddl_target_name(rules, &change->id, &table->schema, &table->name);
			if (change->previous_id)
				ddl_target_name(rules, change->previous_id, &table->old_schema, &table->old_name);
			else
			{
				table->old_schema = table->schema;
				table->old_name = table->name;
			}
			table->qualified = quote_qualified_identifier(table->old_schema, table->old_name);
			table->old_namespace_oid = get_namespace_oid(table->old_schema, true);
			table->namespace_oid = get_namespace_oid(table->schema, true);
			table->relation_oid = ddl_lookup_relation(table->old_schema, table->old_name);
			foreach(other, plan->tables)
			{
				DdlTable *earlier = lfirst(other);

				if ((OidIsValid(table->relation_oid) && earlier->relation_oid == table->relation_oid) ||
					(strcmp(earlier->schema, table->schema) == 0 && strcmp(earlier->name, table->name) == 0))
					ddl_unsupported(change->id.key, "multiple source table images resolve to the same target");
			}
			foreach(column_cell, change->columns)
			{
				DdlColumn *column = ddl_column(change, lfirst(column_cell), type, rules);
				ListCell *previous_column;

				foreach(previous_column, table->columns)
					if (strcmp(((DdlColumn *) lfirst(previous_column))->name, column->name) == 0)
						ddl_unsupported(change->id.key, "multiple source columns resolve to the same target column name");
				table->columns = lappend(table->columns, column);
			}
			if (OidIsValid(table->relation_oid))
				locks[lock_count++] = table->relation_oid;
			plan->tables = lappend(plan->tables, table);
		}
		qsort(locks, lock_count, sizeof(*locks), ddl_compare_oids);
		for (i = 0; i < lock_count; i++)
			if (i == 0 || locks[i] != locks[i - 1])
				LockRelationOid(locks[i], AccessExclusiveLock);
		foreach(cell, plan->tables)
			ddl_prepare_table(plan, lfirst(cell));
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(previous);
		MemoryContextDelete(context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(previous);
	return plan;
}

static void
ddl_execute(const char *query)
{
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI to execute schema plan");
	if (SPI_execute(query, false, 0) != SPI_OK_UTILITY)
		elog(ERROR, "schema plan did not execute as a utility statement");
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "could not finish schema plan SPI operation");
	CommandCounterIncrement();
}

static void
ddl_update_bindings(SynchdbDdlPlan *plan, const DdlTable *table)
{
	Oid relation_oid;
	Relation relation;
	SynchdbAttributeBinding *bindings;
	ListCell *cell;
	int count = list_length(table->columns);
	int i = 0;

	(void) synchdb_attribute_store_delete_source_table(plan->connector,
			plan->connector_type, table->source->id.key);
	if (table->source->previous_id &&
		strcmp(table->source->previous_id->key, table->source->id.key) != 0)
		(void) synchdb_attribute_store_delete_source_table(plan->connector,
				plan->connector_type, table->source->previous_id->key);
	if (table->source->operation == SYNCHDB_DBZ_SCHEMA_DROP)
	{
		if (OidIsValid(table->relation_oid) && get_rel_name(table->relation_oid) != NULL)
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("schema DROP did not remove its bound target relation")));
		return;
	}
	relation_oid = ddl_lookup_relation(table->schema, table->name);
	if (!OidIsValid(relation_oid) ||
		(OidIsValid(table->relation_oid) && relation_oid != table->relation_oid))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("schema plan target relation identity changed before binding update")));
	relation = table_open(relation_oid, AccessShareLock);
	if (RelationGetNamespace(relation) != table->namespace_oid)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("schema plan target namespace identity changed")));
	synchdb_target_validate_relation(relation);
	bindings = palloc0(mul_size(count, sizeof(*bindings)));
	foreach(cell, table->columns)
	{
		DdlColumn *column = lfirst(cell);
		AttrNumber attnum = get_attnum(relation_oid, column->name);
		Form_pg_attribute attribute;

		if (attnum <= 0)
			elog(ERROR, "schema plan did not produce its expected target column");
		attribute = TupleDescAttr(RelationGetDescr(relation), attnum - 1);
		if (attribute->atttypid != column->type || attribute->atttypmod != column->typmod ||
			attribute->attnotnull != !column->source->optional)
			elog(ERROR, "schema plan produced an unexpected target column type");
		column->old_attnum = attnum;
		bindings[i].target_relation = relation_oid;
		bindings[i].target_attnum = attnum;
		bindings[i].source_table = table->source->id.key;
		bindings[i].source_column = column->source->name;
		bindings[i].source_type = column->source->type_name;
		i++;
	}
	foreach(cell, table->old_bindings)
	{
		const SynchdbAttributeBinding *old = lfirst(cell);
		TupleDesc descriptor = RelationGetDescr(relation);

		if (old->source_type && !ddl_has_old_attribute(table, old->target_attnum) &&
			old->target_attnum > 0 && old->target_attnum <= descriptor->natts &&
			!TupleDescAttr(descriptor, old->target_attnum - 1)->attisdropped)
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("schema plan did not drop its bound target column")));
	}
	{
		char *constraint_name;
		List *primary_key = ddl_current_primary_key(relation, &constraint_name);

		if (!ddl_same_primary_key(table, primary_key))
			elog(ERROR, "schema plan produced an unexpected target primary key");
	}
	synchdb_attribute_store_upsert(plan->connector, plan->connector_type, bindings, count);
	table_close(relation, NoLock);
}

static void
ddl_verify_namespaces(const DdlTable *table)
{
	/* A held relation lock does not freeze its namespace's name. Consume SI
	 * before each postcondition rather than reusing an old syscache answer. */
	AcceptInvalidationMessages();
	if ((OidIsValid(table->old_namespace_oid) &&
		 get_namespace_oid(table->old_schema, true) != table->old_namespace_oid) ||
		(OidIsValid(table->namespace_oid) &&
		 get_namespace_oid(table->schema, true) != table->namespace_oid))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("target namespace changed during schema plan execution")));
}

SynchdbDdlApplyResult
synchdb_ddl_plan_apply(SynchdbDdlPlan *plan)
{
	SynchdbDdlApplyResult result = {0};
	ListCell *cell;
	MemoryContext previous;

	ddl_require_transaction();
	if (plan == NULL || plan->applied ||
		plan->rules != synchdb_rule_snapshot_current() ||
		plan->generation != synchdb_rule_snapshot_generation(plan->rules))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("schema plan rules changed before execution")));
	previous = MemoryContextSwitchTo(plan->context);
	PG_TRY();
	{
		foreach(cell, plan->tables)
		{
			DdlTable *table = lfirst(cell);
			ListCell *command;

			ddl_verify_namespaces(table);
			if (table->create_schema && !OidIsValid(table->namespace_oid))
			{
				Oid current = get_namespace_oid(table->schema, true);

				if (OidIsValid(current))
				{
					ListCell *previous_table;
					bool created_by_plan = false;

					foreach(previous_table, plan->tables)
					{
						DdlTable *earlier = lfirst(previous_table);

						if (earlier == table)
							break;
						if (earlier->namespace_oid == current &&
							strcmp(earlier->schema, table->schema) == 0)
							created_by_plan = true;
					}
					if (!created_by_plan)
						ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
										errmsg("target namespace appeared after schema planning")));
				}
				else
				{
					ddl_execute(psprintf("CREATE SCHEMA %s", quote_identifier(table->schema)));
					result.statements++;
					current = get_namespace_oid(table->schema, false);
				}
				table->namespace_oid = current;
				if (strcmp(table->schema, table->old_schema) == 0)
					table->old_namespace_oid = current;
			}
			foreach(command, table->commands)
			{
				ddl_verify_namespaces(table);
				ddl_execute(lfirst(command));
				ddl_verify_namespaces(table);
				result.statements++;
			}
			ddl_update_bindings(plan, table);
			if (table->changed)
				result.tables_changed++;
			if (table->source->operation == SYNCHDB_DBZ_SCHEMA_CREATE &&
				!OidIsValid(table->relation_oid))
				result.tables_created++;
		}
		plan->applied = true;
		synchdb_pg_metadata_mark_dirty();
	}
	PG_FINALLY();
	{
		MemoryContextSwitchTo(previous);
	}
	PG_END_TRY();
	return result;
}

void
synchdb_ddl_plan_destroy(SynchdbDdlPlan *plan)
{
	if (plan)
		MemoryContextDelete(plan->context);
}
