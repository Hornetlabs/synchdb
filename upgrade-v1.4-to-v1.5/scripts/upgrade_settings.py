"""Preserve the two changed v1.4 defaults during an automatic upgrade.

These parameters are PGC_SIGHUP. Persist them with ALTER SYSTEM rather than
session SET or ALTER DATABASE, so future manual STARTs use the same policy.
"""
import json


SKIPPED = 'synchdb.dbz_skipped_operations'
LEGACY_SKIPPED = 'synchdb.dbz_skipped_oeprations'
THREADS = 'synchdb.dbz_snapshot_thread_num'
BATCH = 'synchdb.dbz_batch_size'
QUEUE = 'synchdb.dbz_queue_size'
HEAP = 'synchdb.jvm_max_heap_size'
NAMES = (SKIPPED, LEGACY_SKIPPED, THREADS, BATCH, QUEUE, HEAP)


def normalized_skipped(value):
    if not isinstance(value, str):
        raise ValueError('Skipped operations must be a string')
    if not value.strip() or value.lower() == 'none':
        return 'none'
    tokens = [token.strip() for token in value.split(',')]
    if any(token not in ('c', 'u', 'd', 't') for token in tokens):
        raise ValueError('Use none or a comma-separated list of c, u, d and t for skipped operations')
    return ','.join(tokens)


def compatibility_plan(snapshot):
    values = snapshot['values']
    if snapshot.get('scoped_overrides'):
        raise ValueError('SynchDB SIGHUP parameters have role/database overrides; '
                         'move these settings to the PostgreSQL configuration before upgrading')
    # An administrator may already have prepared the correctly spelled setting
    # as an old-version placeholder. The new runtime explicitly gives it
    # precedence over the historical spelling, so retain that choice.
    canonical = values.get(SKIPPED)
    legacy = values.get(LEGACY_SKIPPED)
    skipped = normalized_skipped(canonical if canonical is not None else
                                 legacy if legacy is not None else 't')
    numeric = {}
    for name, default in ((THREADS, 2), (BATCH, 2048), (QUEUE, 8192), (HEAP, 1024)):
        raw = values.get(name)
        try:
            numeric[name] = int(raw) if raw is not None else default
        except (TypeError, ValueError):
            raise ValueError('Invalid existing setting: ' + name) from None
    if numeric[THREADS] < 1 or numeric[THREADS] > 16:
        raise ValueError('Existing snapshot thread count is outside 1..16')
    if numeric[BATCH] < 1 or numeric[QUEUE] <= numeric[BATCH]:
        raise ValueError('Set synchdb.dbz_queue_size greater than synchdb.dbz_batch_size before upgrading')
    if numeric[HEAP] < 1:
        raise ValueError('Set synchdb.jvm_max_heap_size to at least 1 before upgrading')
    if (snapshot.get('sources', {}).get(THREADS) == 'command line' and
            numeric[THREADS] != 1):
        raise ValueError('The PostgreSQL service command line forces snapshot threads above 1; '
                         'change that startup option to 1 before upgrading')
    return {
        'old_values': {name: values.get(name) for name in NAMES},
        'old_sources': snapshot.get('sources', {}),
        'set': {SKIPPED: skipped, THREADS: '1'},
        'skipped_policy': 'explicit canonical setting' if canonical is not None
                          else 'existing legacy value or v1.4 default t',
        'snapshot_threads': '1 is required by the current native continuation protocol',
        'persistence': 'ALTER SYSTEM after cold backup; original postgresql.auto.conf is in that backup',
    }


def capture(json_query):
    names = ','.join("'" + name + "'" for name in NAMES)
    snapshot = json_query("SELECT json_build_object("
        "'values',(SELECT json_object_agg(name,current_setting(name,true)) FROM "
        "unnest(ARRAY[" + names + "]) AS settings(name)),"
        "'sources',coalesce((SELECT json_object_agg(name,source) FROM pg_settings "
        "WHERE name IN (" + names + ")), '{}'::json),"
        "'scoped_overrides',coalesce((SELECT json_agg(json_build_object("
        "'database_oid',setdatabase,'role_oid',setrole,'setting',split_part(setting,'=',1))) "
        "FROM pg_db_role_setting CROSS JOIN LATERAL unnest(setconfig) AS settings(setting) "
        "WHERE split_part(setting,'=',1) IN (" + names + ")), '[]'::json));")
    return compatibility_plan(snapshot)


def persist(query, plan):
    settings = plan['set']
    if set(settings) != {SKIPPED, THREADS} or settings[THREADS] != '1':
        raise ValueError('Invalid saved compatibility settings')
    skipped = normalized_skipped(settings[SKIPPED])
    # The validated values contain only known operation letters and commas.
    # Each ALTER SYSTEM must run outside a transaction block.
    query("ALTER SYSTEM SET " + SKIPPED + " = '" + skipped + "';")
    query("ALTER SYSTEM SET " + THREADS + " = '1';")
    if query('SELECT pg_reload_conf();') != 't':
        raise RuntimeError('PostgreSQL did not accept the compatibility configuration reload')


def verify(json_query, plan):
    actual = json_query("SELECT json_build_object('" + SKIPPED + "',"
        "current_setting('" + SKIPPED + "',true),'" + THREADS + "',"
        "current_setting('" + THREADS + "',true));")
    if (actual.get(SKIPPED) is None or actual.get(THREADS) != '1' or
            normalized_skipped(actual[SKIPPED]) != plan['set'][SKIPPED]):
        raise RuntimeError('Compatibility settings are overridden after restart: ' +
                           json.dumps(actual, ensure_ascii=False) +
                           '; inspect service command-line and role/database settings before START')
    return actual
