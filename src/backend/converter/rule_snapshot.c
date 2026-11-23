/* Candidate construction cannot mutate the active rules or target catalog. */
#include "postgres.h"

#include "access/xact.h"
#include "common/hashfn.h"
#include "converter/rule_snapshot.h"
#include "converter/transform_expression.h"
#include "nodes/parsenodes.h"
#include "parser/parser.h"
#include "catalog/pg_type_d.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"

typedef struct RuleKey
{
	const char *source;
	int discriminator;
} RuleKey;

typedef struct RuleEntry
{
	RuleKey key;
	const char *destination;
	SynchdbRuleTypeMapping datatype;
} RuleEntry;

typedef struct BuiltinType
{
	struct
	{
		const char *source;
		bool auto_incremented;
	} key;
	SynchdbRuleTypeMapping datatype;
} BuiltinType;

struct SynchdbRuleSnapshot
{
	MemoryContext context;
	char *connector;
	ConnectorType type;
	uint64 generation;
	HTAB *rules;
};

static SynchdbRuleSnapshot *active_snapshot = NULL;
static uint64 last_generation = 0;

static const BuiltinType mysql_builtin[] =
{
	{{"int", true}, {"serial", 0}},
	{{"int unsigned", true}, {"serial", 0}},
	{{"bigint", true}, {"bigserial", 0}},
	{{"bigint unsigned", true}, {"bigserial", 0}},
	{{"smallint", true}, {"smallserial", 0}},
	{{"smallint unsigned", true}, {"smallserial", 0}},
	{{"mediumint", true}, {"serial", 0}},
	{{"mediumint unsigned", true}, {"serial", 0}},
	{{"tinyint", true}, {"smallserial", 0}},
	{{"tinyint unsigned", true}, {"smallserial", 0}},
	{{"enum", false}, {"text", 0}},
	{{"set", false}, {"text", 0}},
	{{"bigint", false}, {"bigint", 0}},
	{{"bigint unsigned", false}, {"numeric", -1}},
	{{"numeric unsigned", false}, {"numeric", -1}},
	{{"dec", false}, {"decimal", -1}},
	{{"dec unsigned", false}, {"decimal", -1}},
	{{"decimal unsigned", false}, {"decimal", -1}},
	{{"fixed", false}, {"decimal", -1}},
	{{"fixed unsigned", false}, {"decimal", -1}},
	{{"bit(1)", false}, {"boolean", 0}},
	{{"bit", false}, {"bit", -1}},
	{{"bool", false}, {"boolean", -1}},
	{{"double", false}, {"double precision", 0}},
	{{"double precision", false}, {"double precision", 0}},
	{{"double precision unsigned", false}, {"double precision", 0}},
	{{"double unsigned", false}, {"double precision", 0}},
	{{"real", false}, {"real", 0}},
	{{"real unsigned", false}, {"real", 0}},
	{{"float", false}, {"real", 0}},
	{{"float unsigned", false}, {"real", 0}},
	{{"int", false}, {"int", 0}},
	{{"int unsigned", false}, {"bigint", 0}},
	{{"integer", false}, {"int", 0}},
	{{"integer unsigned", false}, {"bigint", 0}},
	{{"mediumint", false}, {"int", 0}},
	{{"mediumint unsigned", false}, {"int", 0}},
	{{"year", false}, {"int", 0}},
	{{"smallint", false}, {"smallint", 0}},
	{{"smallint unsigned", false}, {"int", 0}},
	{{"tinyint", false}, {"smallint", 0}},
	{{"tinyint unsigned", false}, {"smallint", 0}},
	{{"datetime", false}, {"timestamp", -1}},
	{{"timestamp", false}, {"timestamptz", -1}},
	{{"binary", false}, {"bytea", 0}},
	{{"varbinary", false}, {"bytea", 0}},
	{{"blob", false}, {"bytea", 0}},
	{{"mediumblob", false}, {"bytea", 0}},
	{{"longblob", false}, {"bytea", 0}},
	{{"tinyblob", false}, {"bytea", 0}},
	{{"long varchar", false}, {"text", 0}},
	{{"longtext", false}, {"text", 0}},
	{{"mediumtext", false}, {"text", 0}},
	{{"tinytext", false}, {"text", 0}},
	{{"json", false}, {"jsonb", -1}},
	{{"geometry", false}, {"text", 0}},
	{{"geometrycollection", false}, {"text", 0}},
	{{"geomcollection", false}, {"text", 0}},
	{{"linestring", false}, {"text", 0}},
	{{"multilinestring", false}, {"text", 0}},
	{{"multipoint", false}, {"text", 0}},
	{{"multipolygon", false}, {"text", 0}},
	{{"point", false}, {"text", 0}},
	{{"polygon", false}, {"text", 0}},
};

static const BuiltinType oracle_builtin[] =
{
	{{"binary_double", false}, {"double precision", 0}},
	{{"binary_float", false}, {"real", 0}},
	{{"float", false}, {"real", 0}},
	{{"number(0,0)", false}, {"numeric", -1}},
	{{"number(1,0)", false}, {"smallint", 0}},
	{{"number(2,0)", false}, {"smallint", 0}},
	{{"number(3,0)", false}, {"smallint", 0}},
	{{"number(4,0)", false}, {"smallint", 0}},
	{{"number(5,0)", false}, {"int", 0}},
	{{"number(6,0)", false}, {"int", 0}},
	{{"number(7,0)", false}, {"int", 0}},
	{{"number(8,0)", false}, {"int", 0}},
	{{"number(9,0)", false}, {"int", 0}},
	{{"number(10,0)", false}, {"bigint", 0}},
	{{"number(11,0)", false}, {"bigint", 0}},
	{{"number(12,0)", false}, {"bigint", 0}},
	{{"number(13,0)", false}, {"bigint", 0}},
	{{"number(14,0)", false}, {"bigint", 0}},
	{{"number(15,0)", false}, {"bigint", 0}},
	{{"number(16,0)", false}, {"bigint", 0}},
	{{"number(17,0)", false}, {"bigint", 0}},
	{{"number(18,0)", false}, {"bigint", 0}},
	{{"number(19,0)", false}, {"numeric", -1}},
	{{"number(20,0)", false}, {"numeric", -1}},
	{{"number(21,0)", false}, {"numeric", -1}},
	{{"number(22,0)", false}, {"numeric", -1}},
	{{"number(23,0)", false}, {"numeric", -1}},
	{{"number(24,0)", false}, {"numeric", -1}},
	{{"number(25,0)", false}, {"numeric", -1}},
	{{"number(26,0)", false}, {"numeric", -1}},
	{{"number(27,0)", false}, {"numeric", -1}},
	{{"number(28,0)", false}, {"numeric", -1}},
	{{"number(29,0)", false}, {"numeric", -1}},
	{{"number(30,0)", false}, {"numeric", -1}},
	{{"number(31,0)", false}, {"numeric", -1}},
	{{"number(32,0)", false}, {"numeric", -1}},
	{{"number(33,0)", false}, {"numeric", -1}},
	{{"number(34,0)", false}, {"numeric", -1}},
	{{"number(35,0)", false}, {"numeric", -1}},
	{{"number(36,0)", false}, {"numeric", -1}},
	{{"number(37,0)", false}, {"numeric", -1}},
	{{"number(38,0)", false}, {"numeric", -1}},
	{{"number", false}, {"numeric", -1}},
	{{"numeric", false}, {"numeric", -1}},
	{{"date", false}, {"timestamp", -1}},
	{{"long", false}, {"text", -1}},
	{{"interval day to second", false}, {"interval day to second", -1}},
	{{"interval year to month", false}, {"interval year to month", 0}},
	{{"timestamp", false}, {"timestamp", -1}},
	{{"timestamp with local time zone", false}, {"timestamptz", -1}},
	{{"timestamp with time zone", false}, {"timestamptz", -1}},
	{{"date", false}, {"date", -1}},
	{{"char", false}, {"char", -1}},
	{{"nchar", false}, {"char", -1}},
	{{"nvarchar2", false}, {"varchar", -1}},
	{{"varchar", false}, {"varchar", -1}},
	{{"varchar2", false}, {"varchar", -1}},
	{{"long raw", false}, {"bytea", 0}},
	{{"raw", false}, {"bytea", 0}},
	{{"decimal", false}, {"decimal", -1}},
	{{"rowid", false}, {"text", 0}},
	{{"urowid", false}, {"text", 0}},
	{{"xmltype", false}, {"text", 0}},
	{{"bfile", false}, {"text", 0}},
	{{"blob", false}, {"bytea", 0}},
	{{"clob", false}, {"text", 0}},
	{{"nclob", false}, {"text", 0}},
	{{"sdo_geometry", false}, {"text", 0}},
	{{"sdo_topo_geometry", false}, {"text", 0}},
	{{"sdo_georaster", false}, {"text", 0}},
	{{"uritype", false}, {"text", 0}},
	{{"anytype", false}, {"text", 0}},
	{{"anydata", false}, {"text", 0}},
	{{"anydataset", false}, {"text", 0}},
};

static const BuiltinType sqlserver_builtin[] =
{
	{{"int identity", true}, {"serial", 0}},
	{{"bigint identity", true}, {"bigserial", 0}},
	{{"smallint identity", true}, {"smallserial", 0}},
	{{"enum", false}, {"text", 0}},
	{{"int", false}, {"int", 0}},
	{{"bigint", false}, {"bigint", 0}},
	{{"smallint", false}, {"smallint", 0}},
	{{"tinyint", false}, {"smallint", 0}},
	{{"numeric", false}, {"numeric", -1}},
	{{"decimal", false}, {"numeric", -1}},
	{{"bit(1)", false}, {"bool", 0}},
	{{"bit", false}, {"bit", 0}},
	{{"money", false}, {"money", 0}},
	{{"smallmoney", false}, {"money", 0}},
	{{"real", false}, {"real", 0}},
	{{"float", false}, {"real", 0}},
	{{"date", false}, {"date", 0}},
	{{"time", false}, {"time", 0}},
	{{"datetime", false}, {"timestamp", 0}},
	{{"datetime2", false}, {"timestamp", 0}},
	{{"datetimeoffset", false}, {"timestamptz", 0}},
	{{"smalldatetime", false}, {"timestamp", 0}},
	{{"char", false}, {"char", -1}},
	{{"varchar", false}, {"varchar", -1}},
	{{"text", false}, {"text", 0}},
	{{"nchar", false}, {"char", 0}},
	{{"nvarchar", false}, {"varchar", -1}},
	{{"ntext", false}, {"text", 0}},
	{{"binary", false}, {"bytea", 0}},
	{{"varbinary", false}, {"bytea", 0}},
	{{"image", false}, {"bytea", 0}},
	{{"uniqueidentifier", false}, {"uuid", 0}},
	{{"xml", false}, {"xml", 0}},
	{{"json", false}, {"jsonb", -1}},
	{{"hierarchyid", false}, {"text", 0}},
	{{"vector", false}, {"text", 0}},
	{{"geometry", false}, {"text", 0}},
	{{"geography", false}, {"text", 0}},
};

static const BuiltinType postgres_builtin[] =
{
	{{"bigint", false}, {"bigint", 0}},
	{{"bigserial", false}, {"bigserial", 0}},
	{{"bigserial", true}, {"bigserial", 0}},
	{{"bit", false}, {"bit", -1}},
	{{"bit varying", false}, {"bit varying", -1}},
	{{"bool", false}, {"bool", 0}},
	{{"boolean", false}, {"boolean", 0}},
	{{"box", false}, {"box", 0}},
	{{"bytea", false}, {"bytea", 0}},
	{{"char", false}, {"char", -1}},
	{{"character", false}, {"character", -1}},
	{{"character varying", false}, {"character varying", -1}},
	{{"cidr", false}, {"cidr", 0}},
	{{"circle", false}, {"circle", 0}},
	{{"date", false}, {"date", 0}},
	{{"decimal", false}, {"decimal", -1}},
	{{"double precision", false}, {"double precision", 0}},
	{{"float", false}, {"float", 0}},
	{{"float4", false}, {"float4", 0}},
	{{"float8", false}, {"float8", 0}},
	{{"inet", false}, {"inet", 0}},
	{{"int", false}, {"int", 0}},
	{{"int2", false}, {"int2", 0}},
	{{"int4", false}, {"int4", 0}},
	{{"int8", false}, {"int8", 0}},
	{{"integer", false}, {"integer", 0}},
	{{"interval", false}, {"interval", -1}},
	{{"json", false}, {"json", 0}},
	{{"jsonb", false}, {"jsonb", 0}},
	{{"line", false}, {"line", 0}},
	{{"lseg", false}, {"lseg", 0}},
	{{"macaddr", false}, {"macaddr", 0}},
	{{"macaddr8", false}, {"macaddr8", 0}},
	{{"money", false}, {"money", 0}},
	{{"numeric", false}, {"numeric", -1}},
	{{"path", false}, {"path", 0}},
	{{"pg_lsn", false}, {"pg_lsn", 0}},
	{{"pg_snapshot", false}, {"pg_snapshot", 0}},
	{{"point", false}, {"point", 0}},
	{{"polygon", false}, {"polygon", 0}},
	{{"real", false}, {"real", 0}},
	{{"smallint", false}, {"smallint", 0}},
	{{"smallserial", false}, {"smallserial", 0}},
	{{"smallserial", true}, {"smallserial", 0}},
	{{"serial", false}, {"serial", 0}},
	{{"serial", true}, {"serial", 0}},
	{{"serial2", false}, {"serial2", 0}},
	{{"serial2", true}, {"serial2", 0}},
	{{"serial4", false}, {"serial4", 0}},
	{{"serial4", true}, {"serial4", 0}},
	{{"serial8", false}, {"serial8", 0}},
	{{"serial8", true}, {"serial8", 0}},
	{{"text", false}, {"text", 0}},
	{{"time without time zone", false}, {"time without time zone", -1}},
	{{"time with time zone", false}, {"time with time zone", -1}},
	{{"time", false}, {"time", -1}},
	{{"timetz", false}, {"timetz", -1}},
	{{"timestamp without time zone", false}, {"timestamp without time zone", -1}},
	{{"timestamp with time zone", false}, {"timestamp with time zone", -1}},
	{{"timestamp", false}, {"timestamp", -1}},
	{{"timestamptz", false}, {"timestamptz", -1}},
	{{"tsquery", false}, {"tsquery", 0}},
	{{"tsvector", false}, {"tsvector", 0}},
	{{"txid_snapshot", false}, {"txid_snapshot", 0}},
	{{"uuid", false}, {"uuid", 0}},
	{{"varbit", false}, {"varbit", -1}},
	{{"varchar", false}, {"varchar", -1}},
	{{"xml", false}, {"xml", 0}},
};


static ConnectorType
rule_dialect(ConnectorType type)
{
	switch (type)
	{
		case TYPE_MYSQL:
		case TYPE_ORACLE:
		case TYPE_SQLSERVER:
		case TYPE_POSTGRES:
			return type;
		case TYPE_OLR:
			return TYPE_ORACLE;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported connector type for mapping rules")));
	}
	pg_unreachable();
}

static uint32
rule_hash(const void *value, Size key_size)
{
	const RuleKey *key = value;

	return hash_combine(hash_bytes((const unsigned char *) key->source,
								  strlen(key->source)), key->discriminator);
}

static int
rule_compare(const void *left, const void *right, Size key_size)
{
	const RuleKey *a = left;
	const RuleKey *b = right;

	if (a->discriminator != b->discriminator)
		return a->discriminator < b->discriminator ? -1 : 1;
	return strcmp(a->source, b->source);
}

static char *
rule_lower(const char *source)
{
	char *result = pstrdup(source);
	char *at;

	for (at = result; *at; at++)
		if (*at >= 'A' && *at <= 'Z')
			*at += 'a' - 'A';
	return result;
}

/* Parse only a type name; do not accept trailing SQL or expression clauses. */
static void
rule_validate_type(const char *name)
{
	char *query = psprintf("SELECT NULL::%s", name);
	List *statements = raw_parser(query, RAW_PARSE_DEFAULT);
	RawStmt *raw;
	SelectStmt *select;
	ResTarget *target;
	TypeCast *cast;

	if (list_length(statements) != 1)
		goto invalid;
	raw = linitial_node(RawStmt, statements);
	if (!IsA(raw->stmt, SelectStmt))
		goto invalid;
	select = (SelectStmt *) raw->stmt;
	if (select->op != SETOP_NONE || select->intoClause != NULL ||
		select->distinctClause != NIL || select->windowClause != NIL ||
		select->valuesLists != NIL || select->fromClause != NIL ||
		select->whereClause != NULL || select->groupClause != NIL ||
		select->havingClause != NULL || select->sortClause != NIL ||
		select->limitOffset != NULL || select->limitCount != NULL ||
		select->withClause != NULL || select->lockingClause != NIL ||
		list_length(select->targetList) != 1)
		goto invalid;
	target = linitial_node(ResTarget, select->targetList);
	if (target->name != NULL || !IsA(target->val, TypeCast))
		goto invalid;
	cast = (TypeCast *) target->val;
	if (!IsA(cast->arg, A_Const) || !((A_Const *) cast->arg)->isnull)
		goto invalid;
	pfree(query);
	return;
invalid:
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("datatype mapping destination must be one PostgreSQL type name")));
}

void
synchdb_rule_parse_target_name(const char *mapped, char **database,
							 char **schema, char **table)
{
	ArrayType *parts;
	Datum *values;
	bool *nulls;
	int count;
	int i;
	char *components[3];

	if (!IsTransactionState() || !ActiveSnapshotSet() || CurrentResourceOwner == NULL)
		elog(ERROR, "target mapping parsing requires a transaction and active snapshot");
	if (!mapped || !database || !schema || !table)
		elog(ERROR, "target name parsing requires a mapping and output pointers");
	parts = DatumGetArrayTypeP(DirectFunctionCall2(parse_ident,
			CStringGetTextDatum(mapped), BoolGetDatum(true)));
	deconstruct_array(parts, TEXTOID, -1, false, TYPALIGN_INT, &values, &nulls, &count);
	if (count < 1 || count > 3)
		ereport(ERROR, (errcode(ERRCODE_INVALID_NAME),
						errmsg("target table mappings require one to three identifier components")));
	for (i = 0; i < count; i++)
	{
		if (nulls[i])
			elog(ERROR, "target name parser returned a null identifier");
		components[i] = TextDatumGetCString(values[i]);
		if (components[i][0] == '\0' || strlen(components[i]) >= NAMEDATALEN)
			ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG),
							errmsg("target mapping identifier is empty or exceeds PostgreSQL capacity"),
							errdetail("Identifiers must contain 1 to %d bytes.", NAMEDATALEN - 1)));
	}
	*database = count == 3 ? components[0] : NULL;
	*schema = count == 1 ? pstrdup("public") : components[count - 2];
	*table = components[count - 1];
	pfree(values);
	pfree(nulls);
	pfree(parts);
}

static void
rule_add(SynchdbRuleSnapshot *snapshot, const ObjectMap *source_rule)
{
	SynchdbMappingKind kind;
	RuleKey key;
	RuleEntry *entry;
	char *source;
	char *destination;
	char *separator;
	bool auto_incremented = false;
	bool found;
	int length = -1;

	if (memchr(source_rule->objtype, '\0', sizeof(source_rule->objtype)) == NULL ||
		source_rule->srcobj == NULL || source_rule->srcobj[0] == '\0' ||
		strnlen(source_rule->srcobj, SYNCHDB_SOURCE_ID_SIZE) >= SYNCHDB_SOURCE_ID_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid source mapping rule")));
	kind = synchdb_mapping_kind(source_rule->objtype);
	/* Disabled and absent rules leave no entry, so builtin fallback is exact. */
	if (!source_rule->enabled)
		return;
	if (memchr(source_rule->dstobj, '\0', sizeof(source_rule->dstobj)) == NULL ||
		source_rule->dstobj[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("mapping rule destination must be nonempty")));
	source = pstrdup(source_rule->srcobj);
	destination = pstrdup(source_rule->dstobj);
	if (kind == SYNCHDB_MAPPING_DATATYPE)
	{
		separator = strchr(source, '|');
		if (separator)
		{
			*separator++ = '\0';
			if (strchr(separator, '|') ||
				(*separator && pg_strcasecmp(separator, "true") != 0 &&
				 pg_strcasecmp(separator, "false") != 0))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid datatype mapping source suffix")));
			auto_incremented = pg_strcasecmp(separator, "true") == 0;
		}
		if (source[0] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("datatype mapping source must be nonempty")));
		if (strchr(source, '.') == NULL)
		{
			char *normalized = rule_lower(source);

			pfree(source);
			source = normalized;
		}
		separator = strchr(destination, '|');
		if (separator)
		{
			*separator++ = '\0';
			if (strchr(separator, '|'))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid datatype mapping destination suffix")));
			if (*separator)
				length = pg_strtoint32(separator);
		}
		if (length < -1 || destination[0] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid datatype mapping destination")));
		rule_validate_type(destination);
	}
	else if (kind == SYNCHDB_MAPPING_TRANSFORM)
		synchdb_transform_validate_template(destination);
	else if (kind == SYNCHDB_MAPPING_TABLE)
	{
		char *database;
		char *schema;
		char *table;

		synchdb_rule_parse_target_name(destination, &database, &schema, &table);
	}
	else if (kind == SYNCHDB_MAPPING_COLUMN && strlen(destination) >= NAMEDATALEN)
		ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG),
						errmsg("target column mapping exceeds PostgreSQL identifier capacity")));
	key.source = source;
	key.discriminator = kind * 2 + (auto_incremented ? 1 : 0);
	entry = hash_search(snapshot->rules, &key, HASH_ENTER, &found);
	if (found)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("multiple enabled mapping rules compile to the same source key")));
	entry->destination = destination;
	entry->datatype.name = kind == SYNCHDB_MAPPING_DATATYPE ? destination : NULL;
	entry->datatype.length = length;
}

SynchdbRuleSnapshot *
synchdb_rule_snapshot_build(const char *connector, ConnectorType type,
						  const ObjectMap *rules, int count, MemoryContext parent)
{
	MemoryContext caller = CurrentMemoryContext;
	MemoryContext context;
	SynchdbRuleSnapshot *snapshot;
	HASHCTL control = {0};
	int index;

	if (!IsTransactionState() || !ActiveSnapshotSet() || CurrentResourceOwner == NULL)
		elog(ERROR, "mapping candidate construction requires a transaction and active snapshot");
	(void) rule_dialect(type);
	if (connector == NULL || connector[0] == '\0' || parent == NULL ||
		count < 0 || (count > 0 && rules == NULL))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid mapping snapshot construction arguments")));
	context = AllocSetContextCreate(parent, "synchdb mapping candidate", ALLOCSET_DEFAULT_SIZES);
	snapshot = MemoryContextAllocZero(context, sizeof(*snapshot));
	PG_TRY();
	{
		MemoryContextSwitchTo(context);
		snapshot->context = context;
		snapshot->connector = pstrdup(connector);
		snapshot->type = type;
		control.keysize = sizeof(RuleKey);
		control.entrysize = sizeof(RuleEntry);
		control.hcxt = context;
		control.hash = rule_hash;
		control.match = rule_compare;
		snapshot->rules = hash_create("synchdb immutable rule overrides", 64, &control,
				HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
		for (index = 0; index < count; index++)
			rule_add(snapshot, &rules[index]);
		MemoryContextSwitchTo(caller);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(caller);
		MemoryContextDelete(context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return snapshot;
}

uint64
synchdb_rule_snapshot_activate(SynchdbRuleSnapshot *candidate)
{
	SynchdbRuleSnapshot *previous = active_snapshot;
	MemoryContext ancestor;

	if (candidate == NULL || candidate == active_snapshot || candidate->generation != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("mapping activation requires a new candidate snapshot")));
	if (IsTransactionOrTransactionBlock() || ActiveSnapshotSet())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("mapping snapshots may be activated only at an idle batch boundary")));
	if (last_generation == PG_UINT64_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("mapping snapshot generation is exhausted")));
	/* The converter owns the supplied parent. Do not silently extend an event
	 * allocation's lifetime or publish a child of the snapshot being retired. */
	for (ancestor = MemoryContextGetParent(candidate->context); ancestor;
		 ancestor = MemoryContextGetParent(ancestor))
		if ((previous && ancestor == previous->context) ||
			ancestor == CurTransactionContext || ancestor == TopTransactionContext)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("mapping candidate requires an independent run-owned memory context")));
	candidate->generation = ++last_generation;
	active_snapshot = candidate;
	if (previous)
		MemoryContextDelete(previous->context);
	return candidate->generation;
}

void
synchdb_rule_snapshot_destroy(SynchdbRuleSnapshot *snapshot)
{
	if (snapshot == NULL)
		return;
	if (snapshot == active_snapshot)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("cannot destroy the active mapping snapshot")));
	MemoryContextDelete(snapshot->context);
}

void
synchdb_rule_snapshot_clear(void)
{
	SynchdbRuleSnapshot *previous = active_snapshot;

	active_snapshot = NULL;
	if (previous)
		MemoryContextDelete(previous->context);
}

const SynchdbRuleSnapshot *
synchdb_rule_snapshot_current(void)
{
	return active_snapshot;
}

uint64
synchdb_rule_snapshot_generation(const SynchdbRuleSnapshot *snapshot)
{
	return snapshot ? snapshot->generation : 0;
}

const char *
synchdb_rule_snapshot_connector(const SynchdbRuleSnapshot *snapshot)
{
	return snapshot ? snapshot->connector : NULL;
}

ConnectorType
synchdb_rule_snapshot_type(const SynchdbRuleSnapshot *snapshot)
{
	return snapshot ? snapshot->type : TYPE_UNDEF;
}

static const RuleEntry *
rule_lookup(const SynchdbRuleSnapshot *snapshot, SynchdbMappingKind kind,
			const char *source, bool auto_incremented)
{
	RuleKey key = {source, kind * 2 + (auto_incremented ? 1 : 0)};

	if (snapshot == NULL || source == NULL)
		return NULL;
	return hash_search(snapshot->rules, &key, HASH_FIND, NULL);
}

const char *
synchdb_rule_object(const SynchdbRuleSnapshot *snapshot, SynchdbMappingKind kind,
					const char *source)
{
	const RuleEntry *entry;

	if (kind != SYNCHDB_MAPPING_TABLE && kind != SYNCHDB_MAPPING_COLUMN)
		elog(ERROR, "object rule lookup requires table or column kind");
	entry = rule_lookup(snapshot, kind, source, false);
	return entry ? entry->destination : NULL;
}

const char *
synchdb_rule_transform(const SynchdbRuleSnapshot *snapshot, const char *source_column)
{
	const RuleEntry *entry = rule_lookup(snapshot, SYNCHDB_MAPPING_TRANSFORM,
										 source_column, false);

	return entry ? entry->destination : NULL;
}

const SynchdbRuleTypeMapping *
synchdb_rule_builtin_type(ConnectorType type, const char *source_type,
						  bool auto_incremented)
{
	const BuiltinType *types = NULL;
	size_t count = 0;
	size_t index;

	if (source_type == NULL)
		return NULL;
	switch (rule_dialect(type))
	{
		case TYPE_MYSQL: types = mysql_builtin; count = lengthof(mysql_builtin); break;
		case TYPE_ORACLE: types = oracle_builtin; count = lengthof(oracle_builtin); break;
		case TYPE_SQLSERVER: types = sqlserver_builtin; count = lengthof(sqlserver_builtin); break;
		case TYPE_POSTGRES: types = postgres_builtin; count = lengthof(postgres_builtin); break;
		default: pg_unreachable();
	}
	for (index = 0; index < count; index++)
		if (types[index].key.auto_incremented == auto_incremented &&
			pg_strcasecmp(types[index].key.source, source_type) == 0)
			return &types[index].datatype;
	return NULL;
}

const SynchdbRuleTypeMapping *
synchdb_rule_type(const SynchdbRuleSnapshot *snapshot, ConnectorType type,
				  const char *source_column, const char *source_type,
				  bool auto_incremented)
{
	const RuleEntry *entry = NULL;
	char *normalized;

	if (snapshot && rule_dialect(snapshot->type) == rule_dialect(type))
	{
		entry = rule_lookup(snapshot, SYNCHDB_MAPPING_DATATYPE, source_column, auto_incremented);
		if (entry)
			return &entry->datatype;
		if (source_type)
		{
			normalized = rule_lower(source_type);
			entry = rule_lookup(snapshot, SYNCHDB_MAPPING_DATATYPE, normalized, auto_incremented);
			pfree(normalized);
		}
	}
	return entry ? &entry->datatype :
		synchdb_rule_builtin_type(type, source_type, auto_incremented);
}
