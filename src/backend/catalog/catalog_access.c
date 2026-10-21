/* Shared SPI and extension-owned relation access for SynchDB stores. */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/catalog_access.h"
#include "catalog/dependency.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

int
synchdb_store_begin_path(void)
{
	int level = NewGUCNestLevel();

	set_config_option("search_path", "pg_catalog", PGC_USERSET, PGC_S_SESSION,
					  GUC_ACTION_SAVE, true, 0, false);
	return level;
}

void
synchdb_store_end_path(int level)
{
	AtEOXact_GUC(true, level);
}

static void
synchdb_store_require_transaction(void)
{
	if (!IsTransactionState() || !ActiveSnapshotSet())
		elog(ERROR, "catalog store requires a transaction and an active snapshot");
}

void
synchdb_store_connect(void)
{
	synchdb_store_require_transaction();
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI for catalog store access");
}

void
synchdb_store_finish(void)
{
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "could not finish SPI catalog store access");
}

/*
 * Resolve afresh for each operation.  Keep the relation lock until transaction
 * end to protect the relation against a concurrent relation rename/move.
 * A same-named relation is not a substitute for the extension's own table.
 */
char *
synchdb_store_relation(const char *relation_name, LOCKMODE lockmode)
{
	Oid extension_oid;
	Oid namespace_oid;
	Oid relation_oid;
	Relation relation;
	char *qualified;

	synchdb_store_require_transaction();
	if (!relation_name || relation_name[0] == '\0' ||
		lockmode < AccessShareLock || lockmode > AccessExclusiveLock)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid catalog store relation or lock mode")));
	extension_oid = get_extension_oid("synchdb", false);
	namespace_oid = get_extension_schema(extension_oid);
	relation_oid = get_relname_relid(relation_name, namespace_oid);
	if (!OidIsValid(relation_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("synchdb catalog \"%s\" is missing from the extension schema",
						relation_name)));

	relation = table_open(relation_oid, lockmode);
	if (RelationGetNamespace(relation) != namespace_oid ||
		strcmp(RelationGetRelationName(relation), relation_name) != 0 ||
		getExtensionOfObject(RelationRelationId, relation_oid) != extension_oid)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("catalog \"%s\" is not owned by the synchdb extension",
						relation_name)));

	qualified = quote_qualified_identifier(get_namespace_name(namespace_oid),
										  RelationGetRelationName(relation));
	table_close(relation, NoLock);
	return qualified;
}

/* The ledger is a scheduler authority, not caller-authored configuration.
 * Resolve and lock its members before any trusted SPI statement. In particular,
 * an additional user trigger or rule must never inherit the trusted identity. */
char *
synchdb_store_control_relation(const char *name, LOCKMODE lockmode)
{
    char *qualified = synchdb_store_relation(name,lockmode);
    Oid extension_oid = get_extension_oid("synchdb",false);
    Oid namespace_oid = get_extension_schema(extension_oid);
    Relation relation = table_open(get_relname_relid(name,namespace_oid),NoLock);
    Trigger *trigger;
    HeapTuple tuple;
    bool trusted;

    trusted = relation->rd_rel->relkind == RELKIND_RELATION &&
        relation->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT &&
        superuser_arg(relation->rd_rel->relowner) &&
        !relation->rd_rel->relrowsecurity && !relation->rd_rel->relforcerowsecurity &&
        !relation->rd_rel->relhasrules && !relation->rd_rel->relispartition &&
        !has_subclass(RelationGetRelid(relation)) &&
        !has_superclass(RelationGetRelid(relation)) &&
        relation->trigdesc && relation->trigdesc->numtriggers == 1;
    if (!trusted)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("control ledger %s has an untrusted definition",name)));
    trigger = &relation->trigdesc->triggers[0];
    trusted = strcmp(trigger->tgname,"synchdb_control_ledger_guard") == 0 &&
        trigger->tgtype == (TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_INSERT |
            TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_DELETE | TRIGGER_TYPE_TRUNCATE) &&
        trigger->tgenabled == TRIGGER_FIRES_ALWAYS && !trigger->tgisinternal &&
        !OidIsValid(trigger->tgconstraint) && !trigger->tgdeferrable &&
        !trigger->tginitdeferred && trigger->tgnargs == 0 && trigger->tgnattr == 0 &&
        trigger->tgqual == NULL && trigger->tgoldtable == NULL && trigger->tgnewtable == NULL &&
        getExtensionOfObject(ProcedureRelationId,trigger->tgfoid) == extension_oid;
    tuple = SearchSysCache1(PROCOID,ObjectIdGetDatum(trigger->tgfoid));
    if (HeapTupleIsValid(tuple))
    {
        Form_pg_proc function = (Form_pg_proc) GETSTRUCT(tuple);

        trusted = trusted && function->pronamespace == namespace_oid &&
            strcmp(NameStr(function->proname),"synchdb_control_ledger_guard") == 0 &&
            superuser_arg(function->proowner) && !function->prosecdef;
        ReleaseSysCache(tuple);
    }
    else
        trusted = false;
    table_close(relation,NoLock);
    if (!trusted)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("control ledger %s has an untrusted guard",name)));
    return qualified;
}

