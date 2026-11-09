#!/usr/bin/env python3
"""SynchDB upgrade helpers: inspect, preflight, migrate offsets and control.

Use libpq services/.pgpass for authentication. Inspection is read-only;
migrate-offsets is a plan unless --apply is supplied. Control submits exactly
once, commits, then waits. No command installs files, upgrades catalogs, creates
keyrings or starts/stops PostgreSQL itself.
"""

import argparse
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time


def new_private_file(path, contents):
    """Create a private report without replacing an existing file."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(contents)
        stream.flush()
        os.fsync(stream.fileno())


def query(args, source, variables=None):
    command = [args.psql, "-X", "-qAt", "-w", "--dbname=" + args.dsn,
               "--set=ON_ERROR_STOP=1"]
    for key, value in (variables or {}).items():
        command.append("--set=" + key + "=" + str(value))
    result = subprocess.run(command, input=source, text=True,
                            encoding="utf-8", capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "psql failed")
    return result.stdout.strip()


def inspect(args):
    source = Path(__file__).with_name("inventory.sql").read_text(encoding="utf-8")
    inventory = json.loads(query(args, source))
    new_private_file(args.output, json.dumps(inventory, ensure_ascii=False,
                                           indent=2) + "\n")
    print("Inventory saved: " + str(args.output))
    print("Connectors: %s; mappings: %s; extension: %s" %
          (len(inventory["connectors"]), len(inventory["mappings"]),
           inventory["extension_version"]))
    print("Connection secrets omitted; do not import this inventory as configuration.")
    print("Existing 1.0 credentials remain readable; no password conversion is required.")
    print("Only caller-visible rows and currently recognized settings are included.")


def identifier(value):
    if "\x00" in value:
        raise ValueError("invalid schema identifier")
    return '"' + value.replace('"', '""') + '"'


def extension_metadata(args, connector=None):
    result = query(args, """
        SELECT pg_catalog.json_build_object('schema', n.nspname, 'version', e.extversion)
        FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace
        WHERE e.extname='synchdb';
    """)
    if not result:
        raise ValueError('synchdb is not installed in this database')
    metadata=json.loads(result)
    if metadata['version']!='1.1':
        raise ValueError('This command requires synchdb 1.1; no operation was submitted')
    if connector is not None:
        if not connector or '\x00' in connector:
            raise ValueError('connector name must be non-empty without NUL')
        valid=query(args,"SELECT pg_catalog.octet_length(:'connector') <= "
                    "pg_catalog.current_setting('max_identifier_length')::integer;", {'connector':connector})
        if valid!='t':
            raise ValueError('connector name would be truncated by PostgreSQL; no operation was submitted')
    return metadata


def preload_has_synchdb(value):
    return any(re.split(r'[/\\]',item.strip().strip('"'))[-1]
               in ('synchdb','synchdb.so','synchdb.dll','synchdb.dylib')
               for item in value.split(','))


LEGACY_PREFIXES = {'mysql':'mysql', 'oracle':'oracle', 'sqlserver':'sqlserver',
                   'postgres':'postgres', 'postgresql':'postgres', 'olr':'olr'}


def infinispan_migration_details(connector):
    """Describe the supported persistent Oracle transaction-cache import.

    These are configuration checks only. The server validates and copies the
    stopped cache tree together with its offset/history in one publication.
    """
    cache_type=str(connector.get('ispn_cache_type',connector.get('ispn','null')) or 'null').lower()
    if cache_type=='null':
        return None
    if str(connector.get('connector','')).lower()!='oracle':
        raise ValueError('Infinispan migration requires the Oracle LogMiner capture backend')
    if cache_type!='embedded':
        raise ValueError('Only embedded Infinispan caches can be migrated')
    memory_type=str(connector.get('ispn_memory_type') or '').lower()
    if memory_type not in ('heap','off heap'):
        raise ValueError('Infinispan memory type must be heap or off heap')
    size=connector.get('ispn_memory_size')
    if isinstance(size,bool) or not isinstance(size,(int,str)) or not str(size).isdigit() or not 0<int(size)<=2**31-1:
        raise ValueError('Infinispan memory size must be a positive signed 32-bit MB value')
    name=connector['name']
    database=connector['target_database']
    if any('/' in value or '\x00' in value for value in (name,database)):
        raise ValueError('Legacy cache alias contains a path separator or NUL')
    return {'old_cache_directory':'pg_synchdb/ispn_'+name+'_'+database,
            'cache_type':'embedded','memory_type':memory_type,'memory_size_mb':int(size),
            'required_caches':['events','transactions','processed','schema'],
            'resume_mode':'no_data','original_cache':'preserved',
            'required':'Cleanly STOP the old connector; preserve its matching offset/history and retained Oracle redo'}


def validate_legacy_claims(claims):
    """Validate a privileged cluster inventory and reject real filename collisions.

    The server separately checks the live system ID/database coverage and local
    connector rows. This function never treats a partial inventory as proof that
    an unexamined database has no connector.
    """
    if not isinstance(claims,dict) or not isinstance(claims.get('system_identifier'),str) \
            or not claims['system_identifier'].isdigit() or not isinstance(claims.get('databases'),list):
        raise ValueError('Invalid cluster ownership inventory')
    database_ids=set()
    database_names=set()
    paths={}
    cache_paths={}
    normalized=[]
    for database in claims['databases']:
        if not isinstance(database,dict) or isinstance(database.get('oid'),bool) \
                or not isinstance(database.get('oid'),int) or not 0<database['oid']<=2**32-1 \
                or not isinstance(database.get('name'),str) or not database['name'] \
                or not isinstance(database.get('connectors'),list):
            raise ValueError('Malformed database in cluster ownership inventory')
        if database['oid'] in database_ids or database['name'] in database_names:
            raise ValueError('Duplicate database in cluster ownership inventory')
        database_ids.add(database['oid'])
        database_names.add(database['name'])
        connector_names=set()
        connectors=[]
        for connector in database['connectors']:
            if not isinstance(connector,dict) or any(not isinstance(connector.get(field),str)
                    or not connector[field] or '\x00' in connector[field]
                    for field in ('name','connector','target_database')):
                raise ValueError('Malformed connector in cluster ownership inventory')
            name=connector['name']
            target=connector['target_database']
            backend=connector['connector'].lower()
            if backend not in LEGACY_PREFIXES:
                raise ValueError('Unsupported legacy backend in ownership inventory: '+backend)
            if '/' in name or '/' in target:
                raise ValueError('Legacy artifact alias contains a path separator')
            if name in connector_names:
                raise ValueError('Duplicate connector claim in database '+database['name'])
            connector_names.add(name)
            path='pg_synchdb/'+LEGACY_PREFIXES[backend]+'_'+name+'_'+target+'_offsets.dat'
            if path in paths:
                previous=paths[path]
                raise ValueError('Legacy artifact filename collision: '+path+' is claimed by '+
                    previous+' and '+database['name']+'/'+name)
            paths[path]=database['name']+'/'+name
            normalized_connector={'name':name,'connector':backend,'target_database':target}
            if 'ispn_cache_type' in connector:
                cache_type=connector['ispn_cache_type']
                if not isinstance(cache_type,str) or not cache_type:
                    raise ValueError('Malformed Infinispan cache ownership claim')
                cache_type=cache_type.lower()
                normalized_connector['ispn_cache_type']=cache_type
                if cache_type!='null':
                    cache_path='pg_synchdb/ispn_'+name+'_'+target
                    if cache_path in cache_paths:
                        raise ValueError('Legacy Infinispan cache directory collision: '+cache_path+
                            ' is claimed by '+cache_paths[cache_path]+' and '+database['name']+'/'+name)
                    cache_paths[cache_path]=database['name']+'/'+name
            connectors.append(normalized_connector)
        normalized.append({'oid':database['oid'],'name':database['name'],
                           'connectors':sorted(connectors,key=lambda item:item['name'])})
    return {'system_identifier':claims['system_identifier'],
            'databases':sorted(normalized,key=lambda item:item['oid'])}


def load_legacy_claims(path):
    path=Path(path)
    details=path.lstat()
    if not stat.S_ISREG(details.st_mode) or details.st_size>16*1024*1024:
        raise ValueError('Ownership inventory must be a regular JSON file of at most 16 MiB')
    return validate_legacy_claims(json.loads(path.read_text(encoding='utf-8')))


def assess_preflight(report, maintenance=False, require_legacy=False):
    blockers=[]
    warnings=[]
    version=report['extension_version']
    if report['extension_schema']!='public':
        blockers.append('This v1.4 upgrade supports only the public extension schema')
    if version not in ('1.0','1.1'):
        blockers.append('Only the documented 1.0 baseline and 1.1 catalogs are handled')
    if version=='1.0':
        profile=json.loads(Path(__file__).with_name('baseline-v1.4.json').read_text(encoding='utf-8'))
        if (profile.get('source_tag')!='v1.4'
                or profile.get('source_commit')!='0756e247ca07083244559cb4616c5dedf1b3da97'):
            raise ValueError('Invalid v1.4 baseline profile')
        if (report.get('function_members')!=53
                or report.get('catalog_profile')!=profile['catalog']):
            blockers.append('Old catalog does not match tag v1.4 (SQL 1.0): exact 53-function/catalog profile required')
        if not report['update_path_1_0_to_1_1']:
            message='Install the matching bundled upgrade SQL before ALTER EXTENSION'
            (blockers if maintenance else warnings).append(message)
    if not report.get('superuser'):
        warnings.append('Use a superuser for complete inventory, catalog upgrade and checkpoint import; RLS may hide rows')
        if maintenance:
            blockers.append('Maintenance checks require a superuser')
    if maintenance:
        if preload_has_synchdb(report['shared_preload_libraries']):
            blockers.append('Remove only synchdb from preload and restart in maintenance mode')
        if report['synchdb_workers']:
            blockers.append('SynchDB workers are still active')
        if any(row['isactive'] for row in report['connectors']):
            blockers.append('Stop every legacy connector before upgrade/checkpoint import')
        if any(row['desired_state']!='STOPPED' for row in report.get('control_intents',[])):
            blockers.append('Stop every accepted connector running intent before checkpoint import')
        if report.get('pending_commands',0):
            blockers.append('Wait for or cancel unfinished control requests')
    for connector in report['connectors']:
        if connector['legacy_resume_check']=='FILE_IDENTITY_CHECK_REQUIRED':
            warnings.append(connector['name']+': underscores are allowed; server checks or a complete cluster inventory must confirm file ownership')
        elif connector['legacy_resume_check']!='SERVER_FILE_AND_OFFSET_CHECK_REQUIRED':
            message=connector['name']+': '+connector['legacy_resume_check']
            (blockers if require_legacy else warnings).append(message)
        try:
            cache=infinispan_migration_details(connector)
        except (ValueError,KeyError) as error:
            blockers.append(connector['name']+': '+str(error))
        else:
            if cache:
                warnings.append(connector['name']+': stopped Infinispan cache tree must match the imported offset/history; server cache checks still required')
        if any(kind=='object' for kind in connector['credential_storage'].values()):
            warnings.append(connector['name']+': transitional object credentials need a separate check; ordinary 1.0 strings need no conversion')
        if any(kind not in ('string','object') for kind in connector['credential_storage'].values()):
            blockers.append(connector['name']+': unexpected credential storage type')
    report['checks']={'maintenance':maintenance,'require_legacy_resume':require_legacy,
                      'blockers':blockers,'warnings':warnings,
                      'result':('REVIEW_REQUIRED' if blockers else
                                'MAINTENANCE_CHECKS_PASSED' if maintenance else 'INVENTORY_ONLY')}
    return report


def preflight_report(args):
    source=Path(__file__).with_name('upgrade_preflight.sql').read_text(encoding='utf-8')
    return json.loads(query(args,source))


def preflight(args):
    report=assess_preflight(preflight_report(args),args.maintenance,args.require_legacy_resume)
    contents=json.dumps(report,ensure_ascii=False,indent=2)+'\n'
    if args.output:
        new_private_file(args.output,contents)
        print('Preflight saved: '+str(args.output))
        print(json.dumps(report['checks'],ensure_ascii=False,indent=2))
    else:
        print(contents,end='')
    return 2 if report['checks']['blockers'] else 0


def migrate_offsets(args):
    metadata=extension_metadata(args,args.connector)
    report=preflight_report(args)
    selected=[row for row in report['connectors'] if row['name']==args.connector]
    if len(selected)!=1:
        raise ValueError('Exactly one visible connector configuration is required')
    claims_file=getattr(args,'claims_file',None)
    claims=load_legacy_claims(claims_file) if claims_file else None
    olr_resume = None
    infinispan_resume=infinispan_migration_details(selected[0])
    if str(selected[0].get('connector') or '').lower() == 'olr':
        olr_resume = {
            'offset_file': '24 bytes: native-endian uint64 scn/c_scn/c_idx; preserve the original platform',
            'snapshot_state': 'schemahistory.dat must contain exactly one byte: t',
            'resume_mode': 'no_data',
            'legacy_baseline_consistency': 'not_revalidated',
            'required': 'Cleanly STOP the old connector; preserve the source, target, capture scope and retained redo',
        }
    # The server function rechecks its own revision, ownership and runtime
    # locks. These read-only checks explain common refusals before mutation.
    checked=dict(report)
    checked['connectors']=selected
    checked=assess_preflight(checked,maintenance=True,require_legacy=True)
    if checked['checks']['blockers']:
        raise ValueError('; '.join(checked['checks']['blockers']))
    schema=identifier(metadata['schema'])
    existing=json.loads(query(args,"SELECT coalesce(json_agg(json_build_object('generation',a.generation,"
        "'origin',a.origin,'phase',a.phase)), '[]'::json) FROM "+schema+".synchdb_artifacts a "
        "JOIN "+schema+".synchdb_conninfo c USING(connector_id) WHERE c.name=:'connector' AND a.active;",
        {'connector':args.connector}))
    if existing:
        if len(existing)==1 and existing[0]['origin']=='LEGACY_COPY' and existing[0]['phase']=='ENGINE_OWNED':
            print(json.dumps({'connector':args.connector,'action':'ALREADY_IMPORTED','artifact':existing[0]},indent=2))
            return 0
        raise ValueError('Connector already has an active generation; inspect it instead of importing again')
    if not args.apply:
        planned={'connector':args.connector,'action':'PLAN_ONLY',
            'next':'Repeat with --apply after preserving the exact old offset/history files',
            'server_checks':'The server will verify files, artifact identity and revision during import; source checks run on START'}
        planned['ownership_inventory']='provided; live server checks still required' if claims else 'server database-suffix check; full cluster inventory may be required'
        if olr_resume:
            planned['olr_resume']=olr_resume
        if infinispan_resume:
            planned['infinispan_resume']=infinispan_resume
        print(json.dumps(planned,indent=2))
        return 0
    try:
        if claims:
            generation=query(args,"SELECT "+schema+".synchdb_artifact_migrate_legacy(:'connector'::pg_catalog.name,:'claims'::pg_catalog.jsonb);",
                             {'connector':args.connector,'claims':json.dumps(claims,ensure_ascii=False)})
        else:
            generation=query(args,"SELECT "+schema+".synchdb_artifact_migrate_legacy(:'connector'::pg_catalog.name);",
                             {'connector':args.connector})
    except (OSError,RuntimeError):
        print('Import failed or its result was lost. Query this connector\'s artifact manifest before retrying; original files are preserved.',file=sys.stderr)
        raise
    imported={'connector':args.connector,'action':'IMPORTED','generation':generation,
              'old_files':'preserved'}
    if olr_resume:
        imported['olr_resume']=olr_resume
    if infinispan_resume:
        imported['infinispan_resume']=infinispan_resume
    print(json.dumps(imported,indent=2))
    return 0


def wait_operation(args,schema,operation_id):
    if operation_id<=0:
        raise ValueError('operation ID must be positive')
    if not math.isfinite(args.timeout) or args.timeout<0:
        raise ValueError('timeout must be a finite nonnegative number')
    deadline=time.monotonic()+args.timeout
    while True:
        remaining=deadline-time.monotonic()
        wait_ms=min(1000,max(0,int(remaining*1000)))
        result=json.loads(query(args,'SELECT '+schema+
            ".synchdb_operation(:'op'::pg_catalog.int8,:'wait_ms'::pg_catalog.int4);\n",
            {'op':operation_id,'wait_ms':wait_ms}))
        status=result['status']
        if status not in ('PENDING','EXECUTING'):
            print(json.dumps(result,ensure_ascii=False,indent=2))
            return 0 if status=='SUCCEEDED' else 1
        if time.monotonic()>=deadline:
            print(json.dumps(result,ensure_ascii=False,indent=2))
            print('Still active; timeout does not cancel the committed operation. Query this ID again.',file=sys.stderr)
            return 2


def operation(args):
    metadata=extension_metadata(args)
    return wait_operation(args,identifier(metadata['schema']),args.id)


def control(args):
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    if args.mode is not None and args.operation not in ("START", "RESTART"):
        raise ValueError("--mode is only valid for START or RESTART")
    metadata=extension_metadata(args,args.connector)
    schema = identifier(metadata["schema"])
    payload = ":'mode'" if args.operation in ("START", "RESTART") else "NULL"
    # This psql call runs one statement in autocommit and exits before waiting.
    # Never resubmit the operation when polling times out or the client exits.
    try:
        operation_id = int(query(args, "SELECT " + schema +
                                 ".synchdb_control(:'connector'::pg_catalog.name, "
                                 ":'operation', " + payload + ");\n",
                                 {"connector": args.connector, "operation": args.operation,
                                  "mode": args.mode or "initial"}))
    except (OSError, ValueError, RuntimeError):
        print("Submission failed or its result was lost. Inspect this connector's "
              "synchdb_control_view.last_operation before retrying.", file=sys.stderr)
        raise
    print("Submitted operation_id=%d (committed)" % operation_id, flush=True)
    return wait_operation(args,schema,operation_id)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    inventory = commands.add_parser("inspect", help="read-only inventory without connection credentials")
    checks=commands.add_parser('preflight',help='read-only catalog, maintenance and legacy-file identity checks')
    offsets=commands.add_parser('migrate-offsets',help='plan one legacy checkpoint import; --apply copies/registers it')
    run = commands.add_parser("control", help="submit once, commit, then wait on 1.1")
    receipt=commands.add_parser('operation',help='query/wait an existing operation ID; never submit another control request')
    for command in (inventory, checks, offsets, run, receipt):
        command.add_argument("--dsn", required=True, help="prefer a libpq service=NAME")
        command.add_argument("--psql", default="psql", help="psql executable or absolute path")
    inventory.add_argument("--output", required=True, type=Path)
    checks.add_argument('--output',type=Path,help='private JSON report; otherwise print to stdout')
    checks.add_argument('--maintenance',action='store_true',help='require stopped workers/intents, no synchdb preload and installed upgrade SQL')
    checks.add_argument('--require-legacy-resume',action='store_true',help='fail on unsupported backend/cache or invalid target; file and cache ownership are checked during import')
    offsets.add_argument('--connector',required=True)
    offsets.add_argument('--apply',action='store_true',help='perform the server-side import; default is plan only')
    offsets.add_argument('--claims-file',type=Path,help='complete cluster ownership inventory produced by the automatic upgrader')
    receipt.add_argument('--id',type=int,required=True,help='previously returned operation ID')
    receipt.add_argument('--timeout',type=float,default=0,help='wait seconds; default 0 only reads current status')
    run.add_argument("--connector", required=True)
    run.add_argument("--operation", required=True,
                     choices=("START", "STOP", "PAUSE", "RESUME", "RESTART", "RELOAD"))
    run.add_argument("--mode", help="START/RESTART snapshot mode; default: initial")
    run.add_argument("--timeout", type=float, default=180, help="receipt wait in seconds")
    args = parser.parse_args(argv)
    try:
        if args.command == "inspect":
            inspect(args)
        elif args.command=='preflight':
            return preflight(args)
        elif args.command=='migrate-offsets':
            return migrate_offsets(args)
        elif args.command=='operation':
            return operation(args)
        elif args.command=='control':
            return control(args)
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        print("Error: " + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
