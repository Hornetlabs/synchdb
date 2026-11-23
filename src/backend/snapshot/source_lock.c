/* The FDW reads on its usual connections. One separate source session holds
 * shared table locks until the last read, or until its local owner aborts. */
#include "postgres.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/resowner.h"
#include <dlfcn.h>

/* mysql_fdw selects the native client suffix separately from PostgreSQL. */
#ifdef __APPLE__
#define MYSQL_CLIENT_SUFFIX ".dylib"
#else
#define MYSQL_CLIENT_SUFFIX DLSUFFIX
#endif

/* Only use the public client ABI. The FDWs own their loaded libraries;
 * SynchDB owns only the dedicated lock sessions created through them. */
typedef struct st_mysql MYSQL;
typedef struct tds_dblib_dbprocess DBPROCESS;
typedef struct tds_dblib_loginrec LOGINREC;

static struct
{
	MYSQL *(*mysql_init)(MYSQL *);
	bool (*mysql_ssl_set)(MYSQL *, const char *, const char *, const char *, const char *, const char *);
	MYSQL *(*mysql_real_connect)(MYSQL *, const char *, const char *, const char *, const char *,
								 unsigned int, const char *, unsigned long);
	int (*mysql_set_character_set)(MYSQL *, const char *);
	int (*mysql_query)(MYSQL *, const char *);
	const char *(*mysql_error)(MYSQL *);
	void (*mysql_close)(MYSQL *);
} my;

static struct
{
	int (*dbinit)(void);
	LOGINREC *(*dblogin)(void);
	int (*dbsetlname)(LOGINREC *, const char *, int);
	DBPROCESS *(*tdsdbopen)(LOGINREC *, const char *, int);
	int (*dbcmd)(DBPROCESS *, const char *);
	int (*dbsqlexec)(DBPROCESS *);
	int (*dbresults)(DBPROCESS *);
	int (*dbnextrow)(DBPROCESS *);
	void (*dbclose)(DBPROCESS *);
	void (*dbloginfree)(LOGINREC *);
	void (*dbexit)(void);
} tds;

/* FreeTDS DB-Library ABI values from sybdb.h. */
enum { DBSETUSER = 2, DBSETPWD = 3, DBSETCHARSET = 10, DBSETDBNAME = 14 };
enum { FAIL = 0, SUCCEED = 1, NO_MORE_RESULTS = 2, REG_ROW = -1, NO_MORE_ROWS = -2 };

static MYSQL *mysql_lock;
static DBPROCESS *tds_lock;
static LOGINREC *tds_login;
static bool tds_initialized;

static void *
client_symbol(void *handle, const char *symbol)
{
	void *address = lookup_external_function(handle, symbol);

	if (!address)
		elog(ERROR, "FDW snapshot lock requires client function %s", symbol);
	return address;
}

static void
load_client(bool mysql)
{
	static bool mysql_ready, tds_ready;
	void *handle;

	if (mysql ? mysql_ready : tds_ready)
		return;
#define RESOLVE(api, member) api.member = client_symbol(handle, #member)
	if (mysql)
	{
		load_external_function("$libdir/mysql_fdw", "mysql_fdw_handler", true, &handle);
		/* Some builds link the client; mysql_fdw 2.9.3 loads it privately
		 * in _PG_init. Reopen that same object without loading another. */
		if (!lookup_external_function(handle, "mysql_init"))
		{
			static void *private_client;
			const char *const names[] = {"libmysqlclient" MYSQL_CLIENT_SUFFIX,
				"libmariadbclient" MYSQL_CLIENT_SUFFIX, "libmariadb" MYSQL_CLIENT_SUFFIX};

			for (int i = 0; !private_client && i < lengthof(names); i++)
				private_client = dlopen(names[i], RTLD_LAZY | RTLD_NOLOAD);
			if (!private_client)
				elog(ERROR, "could not locate the MySQL client loaded by mysql_fdw");
			handle = private_client;
		}
		RESOLVE(my, mysql_init);
		RESOLVE(my, mysql_ssl_set);
		RESOLVE(my, mysql_real_connect);
		RESOLVE(my, mysql_set_character_set);
		RESOLVE(my, mysql_query);
		RESOLVE(my, mysql_error);
		RESOLVE(my, mysql_close);
		mysql_ready = true;
	}
	else
	{
		load_external_function("$libdir/tds_fdw", "tds_fdw_handler", true, &handle);
		RESOLVE(tds, dbinit);
		RESOLVE(tds, dblogin);
		RESOLVE(tds, dbsetlname);
		RESOLVE(tds, tdsdbopen);
		RESOLVE(tds, dbcmd);
		RESOLVE(tds, dbsqlexec);
		RESOLVE(tds, dbresults);
		RESOLVE(tds, dbnextrow);
		RESOLVE(tds, dbclose);
		RESOLVE(tds, dbloginfree);
		RESOLVE(tds, dbexit);
		tds_ready = true;
	}
#undef RESOLVE
}

PG_FUNCTION_INFO_V1(synchdb_fdw_lock_tables);
PG_FUNCTION_INFO_V1(synchdb_fdw_unlock_tables);

static ResourceOwner lock_owner;

static void
close_lock(void)
{
	if (mysql_lock)
		my.mysql_close(mysql_lock);
	mysql_lock = NULL;
	if (tds_lock)
		tds.dbclose(tds_lock);
	if (tds_login)
		tds.dbloginfree(tds_login);
	if (tds_initialized)
		tds.dbexit();
	tds_lock = NULL;
	tds_login = NULL;
	tds_initialized = false;
	lock_owner = NULL;
}

static void
release_lock(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg)
{
	if (phase == RESOURCE_RELEASE_BEFORE_LOCKS && lock_owner && lock_owner == CurrentResourceOwner)
		close_lock();
}

static char *
option(List *options, const char *name)
{
	ListCell *lc;

	foreach(lc, options)
	{
		DefElem *d = lfirst_node(DefElem, lc);
		if (strcmp(d->defname, name) == 0)
			return defGetString(d);
	}
	return NULL;
}

static void
append_identifier(StringInfo sql, const char *name, bool mysql)
{
	char end = mysql ? '`' : ']';

	name = pg_server_to_any(name, strlen(name), PG_UTF8);

	appendStringInfoChar(sql, mysql ? '`' : '[');
	for (; *name; name++)
	{
		if (*name == end)
			appendStringInfoChar(sql, end);
		appendStringInfoChar(sql, *name);
	}
	appendStringInfoChar(sql, end);
}

static void
tds_execute(const char *sql)
{
	int rc;

	if (tds.dbcmd(tds_lock, sql) == FAIL || tds.dbsqlexec(tds_lock) == FAIL)
		elog(ERROR, "SQL Server snapshot lock command failed");
	while ((rc = tds.dbresults(tds_lock)) == SUCCEED)
	{
		while ((rc = tds.dbnextrow(tds_lock)) == REG_ROW)
			;
		if (rc != NO_MORE_ROWS)
			elog(ERROR, "SQL Server snapshot lock query failed");
	}
	if (rc != NO_MORE_RESULTS)
		elog(ERROR, "SQL Server snapshot lock batch failed");
}

Datum
synchdb_fdw_lock_tables(PG_FUNCTION_ARGS)
{
	ForeignServer *server = GetForeignServer(PG_GETARG_OID(0));
	UserMapping *mapping = GetUserMapping(GetUserId(), server->serverid);
	const char *port = option(server->options, "port");
	const char *fdw = GetForeignDataWrapper(server->fdwid)->fdwname;
	bool mysql = strcmp(fdw, "mysql_fdw") == 0;
	char *schema = text_to_cstring(PG_GETARG_TEXT_PP(1));
	ArrayIterator tables = array_create_iterator(PG_GETARG_ARRAYTYPE_P(2), 0, NULL);
	StringInfoData sql;
	Datum table;
	bool isnull;
	int count = 0;
	static bool registered;

	if (!mysql && strcmp(fdw, "tds_fdw") != 0)
		elog(ERROR, "table locks are unsupported for %s", fdw);
	if (lock_owner)
		elog(ERROR, "this backend already holds FDW snapshot locks");
	initStringInfo(&sql);
	appendStringInfoString(&sql, mysql ? "LOCK TABLES " :
		"SET XACT_ABORT ON; SET LOCK_TIMEOUT 10000; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION; DECLARE @v int;");
	while (array_iterate(tables, &table, &isnull))
	{
		if (isnull)
			elog(ERROR, "null FDW snapshot table name");
		if (!mysql)
			appendStringInfoString(&sql, "SELECT TOP (1) @v=1 FROM ");
		else if (count > 0)
			appendStringInfoString(&sql, ", ");
		append_identifier(&sql, schema, mysql);
		appendStringInfoChar(&sql, '.');
		append_identifier(&sql, TextDatumGetCString(table), mysql);
		appendStringInfoString(&sql, mysql ? " READ" : " WITH (TABLOCK,HOLDLOCK);");
		count++;
	}
	if (!count)
		PG_RETURN_VOID();
	if (!registered)
	{
		RegisterResourceReleaseCallback(release_lock, NULL);
		registered = true;
	}
	load_client(mysql);
	lock_owner = CurTransactionResourceOwner;
	if (mysql)
	{
		mysql_lock = my.mysql_init(NULL);
		if (!mysql_lock)
			elog(ERROR, "could not allocate MySQL snapshot lock connection");
		my.mysql_ssl_set(mysql_lock, option(server->options, "ssl_key"), option(server->options, "ssl_cert"),
					  option(server->options, "ssl_ca"), NULL, option(server->options, "ssl_cipher"));
		if (!my.mysql_real_connect(mysql_lock, option(server->options, "host"),
								option(mapping->options, "username"), option(mapping->options, "password"),
								NULL, port ? atoi(port) : 3306, NULL, 0) ||
			my.mysql_set_character_set(mysql_lock, "utf8mb4") ||
			my.mysql_query(mysql_lock, "SET SESSION lock_wait_timeout = 10") ||
			my.mysql_query(mysql_lock, sql.data))
			elog(ERROR, "could not lock MySQL snapshot tables: %s", my.mysql_error(mysql_lock));
	}
	else
	{
		char *address = psprintf("%s:%s", option(server->options, "servername"), port ? port : "1433");

		if (tds.dbinit() == FAIL)
			elog(ERROR, "could not initialize FreeTDS");
		tds_initialized = true;
		tds_login = tds.dblogin();
		if (!tds_login ||
			tds.dbsetlname(tds_login, option(mapping->options, "username"), DBSETUSER) == FAIL ||
			tds.dbsetlname(tds_login, option(mapping->options, "password"), DBSETPWD) == FAIL ||
			tds.dbsetlname(tds_login, "UTF-8", DBSETCHARSET) == FAIL ||
			tds.dbsetlname(tds_login, option(server->options, "database"), DBSETDBNAME) == FAIL ||
			!(tds_lock = tds.tdsdbopen(tds_login, address, 0)))
			elog(ERROR, "could not connect to SQL Server to lock snapshot tables");
		tds_execute(sql.data);
	}
	PG_RETURN_VOID();
}

Datum
synchdb_fdw_unlock_tables(PG_FUNCTION_ARGS)
{
	/* A lost session also lost its locks. Surface that before publishing. */
	if (mysql_lock && my.mysql_query(mysql_lock, "DO 1"))
		elog(ERROR, "lost MySQL snapshot locks: %s", my.mysql_error(mysql_lock));
	if (tds_lock)
		tds_execute("IF XACT_STATE() <> 1 THROW 50000, 'Snapshot lock transaction ended', 1;");
	close_lock();
	PG_RETURN_VOID();
}
