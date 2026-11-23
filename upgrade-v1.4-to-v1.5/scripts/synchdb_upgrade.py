#!/usr/bin/env python3
"""Plan or execute a local, stopped SynchDB 1.0 -> 1.1 upgrade.

One config describes the existing PostgreSQL installation and a staged new
installation. PostgreSQL binaries and persistent service settings stay in place.
The journal and cold backup are private. Use --apply to execute and --resume to
continue an interrupted execution. Failures preserve the current stage; this
tool never automatically rewinds source positions or restores old PGDATA.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pwd
import shlex
import shutil
import subprocess
import sys
import time

try:
    from psycopg2.extensions import make_dsn, parse_dsn
except ImportError:
    make_dsn = parse_dsn = None

# Keep the user-facing upgrade package free of generated Python cache files.
sys.dont_write_bytecode = True

import synchdb_migrate as helper
import upgrade_settings
import verify_bundle

BUNDLE = Path(__file__).resolve().parents[1]


def literal(value):
    if not isinstance(value,str) or '\x00' in value:
        raise ValueError('Expected a PostgreSQL text value without NUL')
    # query() fixes standard_conforming_strings=on. Do not use a connectionless
    # psycopg adapter: its default Latin-1 codec rejects valid UTF-8 names and
    # its backslash escaping need not match the target session setting.
    return "'"+value.replace("'","''")+"'"


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def tree_digest(directory):
    entries=[]
    for path in sorted(Path(directory).rglob('*')):
        if path.is_symlink():
            raise ValueError('Staged Java artifacts must not contain symlinks: '+str(path))
        if path.is_file():
            entries.append((str(path.relative_to(directory)),digest(path)))
    return hashlib.sha256(json.dumps(entries,separators=(',',':')).encode()).hexdigest()


def existing_java_target(process_environment, pgdata, library):
    configured=process_environment.get(b'DBZ_ENGINE_DIR')
    # Match initialize_jvm(): without the environment override it always
    # uses pkglib_path/dbz_engine, independently of the staged Java layout.
    target=Path(os.fsdecode(configured)) if configured else Path(library)/'dbz_engine'
    return (target if target.is_absolute() else Path(pgdata)/target).resolve()


def other_postmasters(postgres, own_pid, process_root=Path('/proc')):
    """Identify other main servers using the same program installation."""
    found=[]
    expected=Path(postgres).resolve()
    for process in process_root.iterdir():
        if not process.name.isdecimal() or int(process.name)==own_pid:
            continue
        try:
            if (process/'exe').resolve(strict=True)!=expected:
                continue
            arguments=(process/'cmdline').read_bytes().split(b'\0')
            name=Path(os.fsdecode(arguments[0])).name
            # Child backends replace argv[0] with a postgres: process title.
            if name in ('postgres','postmaster'):
                found.append(int(process.name))
        except (FileNotFoundError,ProcessLookupError,PermissionError):
            continue
    return found


def atomic_json(path, value):
    temporary = path.with_name(path.name + '.tmp')
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, 'w') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    with open_directory(path.parent) as directory:
        os.fsync(directory)


class open_directory:
    def __init__(self, path):
        self.path = path
    def __enter__(self):
        self.fd = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY)
        return self.fd
    def __exit__(self, *unused):
        os.close(self.fd)


def copy_file(source, destination):
    shutil.copy2(source, destination)
    if os.geteuid()==0:
        st=Path(source).stat()
        os.chown(destination,st.st_uid,st.st_gid)
    with Path(destination).open('rb') as stream:
        os.fsync(stream.fileno())
    return str(destination)


def copy_tree(source, destination):
    shutil.copytree(source, destination, symlinks=True, copy_function=copy_file)
    for directory, _, _ in os.walk(destination, followlinks=False):
        if os.geteuid()==0:
            original=Path(source)/Path(directory).relative_to(destination)
            st=original.stat()
            os.chown(directory,st.st_uid,st.st_gid)
            for entry in Path(directory).iterdir():
                if entry.is_symlink():
                    link_stat=(original/entry.name).lstat()
                    os.lchown(entry,link_stat.st_uid,link_stat.st_gid)
        with open_directory(directory) as descriptor:
            os.fsync(descriptor)


def legacy_filename(connector):
    backend = str(connector['connector']).lower()
    return backend + '_' + connector['name'] + '_' + connector['target_database']


def validate_claims(claims):
    owners = {}
    cache_owners = {}
    for database in claims['databases']:
        for connector in database['connectors']:
            path = legacy_filename(connector)
            owner = (database['name'], connector['name'])
            if path in owners:
                raise ValueError('Old artifact path collision: ' + path + ': ' +
                                 repr(owners[path]) + ' and ' + repr(owner))
            owners[path] = owner
            if str(connector.get('ispn_cache_type','null')).lower()!='null':
                cache='ispn_'+connector['name']+'_'+connector['target_database']
                if cache in cache_owners:
                    raise ValueError('Old Infinispan cache directory collision: '+cache+': '+
                                     repr(cache_owners[cache])+' and '+repr(owner))
                cache_owners[cache]=owner


class Upgrade:
    def __init__(self, config_file, apply=False):
        if parse_dsn is None:
            raise ValueError('This entry point requires psycopg2; use a Python environment containing it')
        self.config_file = Path(config_file).resolve()
        self.config = json.loads(self.config_file.read_text())
        self.config_hash = digest(self.config_file)
        self.apply = apply
        for field in ('pgdata', 'pg_bin', 'new_prefix', 'work_dir', 'dsn'):
            if field not in self.config:
                raise ValueError('Missing configuration field: ' + field)
        for field in ('pgdata', 'pg_bin', 'new_prefix', 'work_dir'):
            if not Path(self.config[field]).is_absolute():
                raise ValueError(field + ' must be an absolute path')
        if Path(self.config['work_dir']).is_symlink():
            raise ValueError('work_dir must not be a symlink')
        self.data = Path(self.config['pgdata']).resolve()
        self.bin = Path(self.config['pg_bin']).resolve()
        self.new = Path(self.config['new_prefix']).resolve()
        self.work = Path(self.config['work_dir']).resolve()
        self.timeout = float(self.config.get('timeout', 300))
        if not 0 < self.timeout <= 86400:
            raise ValueError('timeout must be between 0 and 86400 seconds')
        for ancestor in (self.data, self.new, self.bin.parent):
            if self.work == ancestor or self.work.is_relative_to(ancestor):
                raise ValueError('work_dir must be outside PGDATA and both program installations')
        if not self.data.is_dir() or not (self.bin / 'pg_ctl').is_file():
            raise ValueError('Existing PGDATA and PostgreSQL tools are required')
        self.owner = pwd.getpwuid(self.data.stat().st_uid)
        if os.geteuid() not in (0, self.owner.pw_uid):
            raise ValueError('Run as the PostgreSQL OS owner, or root for automatic installation')
        self.dsn_options = parse_dsn(self.config['dsn'])
        self.secrets = [self.config['dsn']]
        if self.dsn_options.get('password'):
            self.secrets.append(self.dsn_options['password'])
        self.state = None
        self.env = os.environ.copy()
        self.env['PYTHONDONTWRITEBYTECODE'] = '1'
        if os.geteuid() == 0:
            for variable, filename in (('PGPASSFILE', '.pgpass'), ('PGSERVICEFILE', '.pg_service.conf')):
                candidate = Path(self.owner.pw_dir) / filename
                if variable not in self.env and candidate.is_file():
                    self.env[variable] = str(candidate)
        java = shutil.which('java')
        library_paths = [str(self.bin.parent / 'lib')]
        if java:
            library_paths.append(str(Path(java).resolve().parents[1] / 'lib/server'))
        library_paths.append(self.env.get('LD_LIBRARY_PATH', ''))
        self.env['LD_LIBRARY_PATH'] = os.pathsep.join(library_paths)

    def clean(self, value):
        if isinstance(value,str):
            for secret in sorted(self.secrets,key=len,reverse=True):
                if secret:
                    value=value.replace(secret,'<REDACTED_CONNECTION>')
            return value
        if isinstance(value,dict):
            return {key:self.clean(item) for key,item in value.items()}
        if isinstance(value,(tuple,list)):
            return [self.clean(item) for item in value]
        if value is None or isinstance(value,(int,float,bool)):
            return value
        return self.clean(str(value))

    def record(self, kind, **details):
        if self.state is None:
            return
        record = self.clean({'at': datetime.datetime.now().astimezone().isoformat(),
                             'kind': kind, **details})
        with (self.work / 'execution.jsonl').open('a') as stream:
            stream.write(json.dumps(record, ensure_ascii=False) + '\n')
            stream.flush()
            os.fsync(stream.fileno())

    def save(self):
        atomic_json(self.work / 'state.json', self.state)

    def command(self, argv, *, input=None, owner=False, check=True, timeout=None, environment=None):
        argv = [str(part) for part in argv]
        def become_owner():
            os.initgroups(self.owner.pw_name, self.owner.pw_gid)
            os.setgid(self.owner.pw_gid)
            os.setuid(self.owner.pw_uid)
        result = subprocess.run(argv, input=input, text=True, capture_output=True,
            env=environment or self.env, timeout=timeout or self.timeout,
            preexec_fn=become_owner if owner and os.geteuid() == 0 else None,
            cwd=str(self.data.parent))
        safe_argv = list(argv)
        if '--dbname' in safe_argv:
            safe_argv[safe_argv.index('--dbname') + 1] = '<ADMIN_CONNECTION>'
        self.record('command', argv=safe_argv, stdin=input, exit_code=result.returncode,
                    stdout=result.stdout, stderr=result.stderr)
        if check and result.returncode:
            raise RuntimeError(str(self.clean(result.stderr.strip() or result.stdout.strip() or
                                               'Command failed: ' + argv[0])))
        return result

    def query(self, statement, database=None):
        options = dict(self.dsn_options)
        if database is not None:
            options['dbname'] = database
        result = self.command([self.bin / 'psql', '-X', '-qAt', '-w', '-v', 'ON_ERROR_STOP=1',
                               '--dbname', make_dsn(**options)],
                              input="SET standard_conforming_strings=on; SET client_encoding='UTF8';\n"+statement, owner=True)
        return result.stdout.strip()

    def json_query(self, statement, database=None):
        return json.loads(self.query(statement, database))

    def file_metadata(self, path):
        st = path.stat()
        return {'path': str(path), 'uid': st.st_uid, 'gid': st.st_gid, 'mode': st.st_mode & 0o7777}

    def discover(self):
        verify_bundle.verify(BUNDLE / 'sql/manifest.json')
        metadata = self.json_query("SELECT json_build_object('data_directory',current_setting('data_directory'),"
            "'version',current_setting('server_version_num'),'preload',current_setting('shared_preload_libraries'),"
            "'snapshot_engine',coalesce(current_setting('synchdb.snapshot_engine',true),'debezium'),"
            "'superuser',(SELECT rolsuper FROM pg_roles WHERE rolname=current_user),"
            "'system_identifier',(SELECT system_identifier::text FROM pg_control_system()),"
            "'config',current_setting('config_file'),'hba',current_setting('hba_file'),"
            "'ident',current_setting('ident_file'),'port',current_setting('port'),"
            "'listen_addresses',current_setting('listen_addresses'),"
            "'unix_socket_directories',current_setting('unix_socket_directories'));")
        if Path(metadata['data_directory']).resolve() != self.data or not metadata['superuser']:
            raise ValueError('Admin connection must address this PGDATA and be a PostgreSQL superuser')
        pid = int((self.data / 'postmaster.pid').read_text().splitlines()[0])
        process = Path('/proc') / str(pid) / 'exe'
        if process.exists() and process.resolve() != (self.bin / 'postgres').resolve():
            raise ValueError('pg_bin does not match the running postmaster executable')
        shared=other_postmasters(self.bin/'postgres',pid)
        if shared:
            raise ValueError('Other running PostgreSQL instances share this installation; arrange their upgrade separately. PIDs: '+str(shared))
        try:
            process_environment=dict(entry.split(b'=',1) for entry in
                (Path('/proc')/str(pid)/'environ').read_bytes().split(b'\0') if b'=' in entry)
        except PermissionError:
            raise ValueError('Cannot inspect the postmaster Java directory; run as its OS owner or root')
        service = self.config.get('service')
        cgroup = Path('/proc') / str(pid) / 'cgroup'
        if cgroup.exists() and '/system.slice/' in cgroup.read_text() and not service:
            raise ValueError('Set service to this PostgreSQL systemd unit so it cannot restart during maintenance')
        if service:
            service_pid = self.command(['systemctl', 'show', '--property=MainPID', '--value', service]).stdout.strip()
            if service_pid != str(pid):
                raise ValueError('Configured systemd service does not own this postmaster')
        config_tool = self.config.get('pg_config')
        if config_tool is None:
            candidate = self.bin / 'pg_config'
            config_tool = str(candidate) if candidate.is_file() else shutil.which('pg_config')
        if not config_tool:
            raise ValueError('Matching pg_config is required; configure pg_config explicitly')
        reported_bin = Path(self.command([config_tool, '--bindir']).stdout.strip()).resolve()
        if reported_bin != self.bin:
            raise ValueError('pg_config does not belong to pg_bin')
        library = Path(self.command([config_tool, '--pkglibdir']).stdout.strip()).resolve()
        share = Path(self.command([config_tool, '--sharedir']).stdout.strip()).resolve() / 'extension'
        metadata['library'] = str(library)
        metadata['extension_directory'] = str(share)
        expected_major = int(metadata['version']) // 10000
        new_version = self.command([self.new / 'bin/postgres', '--version']).stdout.split()
        if not new_version or int(new_version[-1].split('.')[0]) != expected_major:
            raise ValueError('The staged installation must use the same PostgreSQL major version')
        databases = self.json_query("SELECT coalesce(json_agg(json_build_object('oid',oid::bigint,'name',datname) ORDER BY oid),'[]') "
                                    "FROM pg_database WHERE datallowconn AND NOT datistemplate;")
        claims = {'system_identifier': metadata['system_identifier'], 'databases': []}
        installed = []
        for database in databases:
            name = database['name']
            extension = self.json_query("SELECT coalesce((SELECT json_build_object('schema',n.nspname,'version',e.extversion) "
                "FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='synchdb'),'null');", name)
            claim = dict(database, connectors=[])
            claims['databases'].append(claim)
            if extension is None:
                continue
            if extension['version'] != '1.0':
                raise ValueError(name + ': expected old extension 1.0; use --resume for an interrupted run')
            preflight=self.json_query((Path(__file__).with_name('upgrade_preflight.sql')).read_text(),name)
            assessed=helper.assess_preflight(preflight,maintenance=False,require_legacy=True)
            if assessed['checks']['blockers']:
                raise ValueError(name+': '+'; '.join(assessed['checks']['blockers']))
            schema = helper.identifier(extension['schema'])
            rows = self.json_query("SELECT coalesce(json_agg(json_build_object('name',name,'active',isactive,"
                "'connector',lower(data->>'connector'),'target_database',coalesce(data->>'dstdb',current_database()),"
                "'ispn_cache_type',coalesce(data->>'ispn_cache_type','null'),"
                "'ispn_memory_type',data->>'ispn_memory_type','ispn_memory_size',data->>'ispn_memory_size') ORDER BY name),'[]') FROM " + schema + '.synchdb_conninfo;', name)
            states = self.json_query('SELECT coalesce(json_agg(row_to_json(s)),\'[]\') FROM ' + schema + '.synchdb_state_view s;', name)
            live = {row['name']: row for row in states}
            for row in rows:
                if row['connector'] not in ('mysql','oracle','sqlserver','postgres','olr'):
                    raise ValueError(name + ': unsupported capture backend')
                if row['target_database'] != name or '/' in row['name'] or '/' in name:
                    raise ValueError(name + ': target database or old filename is not valid')
                cache=helper.infinispan_migration_details(row)
                if cache:
                    row['infinispan_resume']=cache
                actual = live.get(row['name'], {})
                row['restore'] = ('PAUSED' if 'paus' in str(actual.get('state','')).lower() else 'RUNNING') if row['active'] else 'STOPPED'
                row['old_pid'] = actual.get('pid')
                if row['active'] and 'snapshot' in str(actual.get('stage','')).lower():
                    raise ValueError(name + ': finish the old snapshot before upgrading')
                claim['connectors'].append({key: row[key] for key in ('name','connector','target_database','ispn_cache_type')})
            installed.append({'name':name, **extension, 'connectors':rows})
        if not installed:
            raise ValueError('No supported SynchDB 1.0 deployment was found')
        compatibility_settings = upgrade_settings.capture(self.json_query)
        validate_claims(claims)
        helper.validate_legacy_claims(claims)
        java_subdir = Path(self.config.get('java_subdir', 'lib/dbz_engine'))
        if java_subdir.is_absolute() or '..' in java_subdir.parts:
            raise ValueError('java_subdir must be relative to each PostgreSQL prefix')
        java_source = self.new / java_subdir
        if not (java_source / 'dbz-engine-1.1.0.jar').is_file() or not (java_source / 'lib').is_dir():
            raise ValueError('Staged Java JAR and dependency directory are required')
        files = [
            (self.new / 'lib/synchdb.so', library / 'synchdb.so'),
            (self.new / 'share/extension/synchdb.control', share / 'synchdb.control'),
            (self.new / 'share/extension/synchdb--1.1.sql', share / 'synchdb--1.1.sql'),
            (BUNDLE / 'sql/synchdb--1.0--1.1.sql', share / 'synchdb--1.0--1.1.sql')]
        for parser in (self.new / 'lib').glob('*oracle*parser*.so'):
            files.append((parser, library / parser.name))
        if any(row['connector']=='olr' for db in installed for row in db['connectors']) and not any('parser' in str(src.name) for src,_ in files):
            raise ValueError('OLR requires its staged Oracle parser library')
        for src,dst in files:
            if not src.is_file() or src.is_symlink() or src.resolve() == dst.resolve():
                raise ValueError('Staged artifacts must be separate regular files: ' + str(src))
            if not os.access(dst.parent, os.W_OK):
                raise ValueError('Cannot install into ' + str(dst.parent) + '; run with installation privileges')
        java_target=existing_java_target(process_environment,self.data,library)
        if java_target==java_source.resolve() or java_target==Path('/') or os.path.ismount(java_target):
            raise ValueError('The existing Java directory must be a separate, dedicated directory')
        if java_target.is_relative_to(self.work) or self.work.is_relative_to(java_target):
            raise ValueError('Java directory and work_dir must not contain each other')
        if not (java_target/'dbz-engine-1.1.0.jar').is_file():
            raise ValueError('The existing Java installation is incomplete: '+str(java_target))
        if not os.access(java_target.parent, os.W_OK):
            raise ValueError('Cannot install Java artifacts into ' + str(java_target.parent))
        external = []
        links = [self.data/'pg_wal'] + list((self.data/'pg_tblspc').iterdir())
        for link in links:
            if link.is_symlink():
                target = link.resolve(strict=True)
                if not target.is_relative_to(self.data):
                    if os.path.ismount(target) or self.work.is_relative_to(target):
                        raise ValueError('External data must be a restorable directory outside work_dir: ' + str(target))
                    if not target.is_dir():
                        raise ValueError('External PostgreSQL data is not a directory: ' + str(target))
                    if str(target) not in [entry['path'] for entry in external]:
                        external.append(self.file_metadata(target))
        return {'server':metadata, 'databases':installed, 'claims':claims,
                'compatibility_settings':compatibility_settings,
                'files':[{'source':str(src),'destination':str(dst),'sha256':digest(src)} for src,dst in files],
                'java_source':str(java_source),'java_target':str(java_target),'java_sha256':tree_digest(java_source),'external':external,
                'checks':self.config.get('checks',[])}

    def plan(self):
        plan = self.discover()
        return self.clean({'action':'PLAN_ONLY','pgdata':str(self.data),'databases':plan['databases'],
            'claims':plan['claims'],
            'configuration_changes':plan['compatibility_settings'],
            'installs':plan['files'],'external_data':plan['external'],
            'steps':['stop old connectors','stop PostgreSQL','cold backup','install matching artifacts',
                     'maintenance start','persist compatibility settings','upgrade each database','import original checkpoints',
                     'normal start','restore RUNNING connectors','run configured checks'],
            'paused_connectors':'kept STOPPED until explicitly resumed; no temporary CDC',
            'data_validation':'configured checks only; ready workers do not prove all business data correct'})

    def node(self, action, maintenance=False):
        if action=='stop' and not self.running():
            return
        if action=='start' and self.running():
            preload=self.query("SELECT current_setting('shared_preload_libraries');")
            if helper.preload_has_synchdb(preload)==maintenance:
                raise ValueError('Running PostgreSQL does not match the expected maintenance state')
            return
        service = self.config.get('service')
        if service and not maintenance:
            self.command(['systemctl',action,service])
            return
        argv=[self.bin/'pg_ctl','-D',self.data,'-w','-t',str(int(self.timeout))]
        if action=='start':
            argv += ['-l',self.data/'synchdb-upgrade-postgres.log']
            server=self.state['plan']['server']
            options=[]
            # A service can keep its configuration outside PGDATA or supply
            # connection settings on the command line. Reuse the running
            # server's actual paths/address so the maintenance server neither
            # reads a different configuration nor listens on a default port.
            # Missing optional fields permit resuming older saved executions.
            for field,setting in (('config','config_file'),('hba','hba_file'),
                                  ('ident','ident_file'),('port','port'),
                                  ('listen_addresses','listen_addresses'),
                                  ('unix_socket_directories','unix_socket_directories')):
                if field in server:
                    options += ['-c',setting+'='+str(server[field])]
            if maintenance:
                preload = [item.strip() for item in server['preload'].split(',')
                           if item.strip() and not helper.preload_has_synchdb(item)]
                options += ['-c','shared_preload_libraries='+','.join(preload)]
            else:
                options += ['-c','shared_preload_libraries='+server['preload']]
            if options:
                argv += ['-o', shlex.join(options)]
        if action=='stop':
            argv += ['-m','fast']
        argv += [action]
        environment = self.env.copy()
        environment['DBZ_ENGINE_DIR'] = self.state['plan']['java_target']
        self.command(argv,owner=True,environment=environment)

    def running(self):
        return self.command([self.bin/'pg_ctl','-D',self.data,'status'],owner=True,check=False).returncode==0

    def stop_connectors(self):
        for database in self.state['plan']['databases']:
            schema=helper.identifier(database['schema'])
            for row in database['connectors']:
                if row['restore']!='STOPPED':
                    active=self.query('SELECT isactive FROM '+schema+'.synchdb_conninfo WHERE name='+literal(row['name'])+';',database['name'])
                    if active=='t':
                        self.query('SELECT '+schema+'.synchdb_stop_engine_bgw('+literal(row['name'])+');', database['name'])
        # Submit every stop before waiting, including multi-database installs.
        databases=','.join(literal(db['name']) for db in self.state['plan']['databases'])
        deadline=time.monotonic()+self.timeout
        while True:
            remaining=int(self.query("SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() "
                "AND backend_type ILIKE '%synchdb%' AND backend_type NOT ILIKE '%launcher%' AND datname IN ("+databases+');'))
            if remaining==0:
                break
            if time.monotonic()>=deadline:
                raise RuntimeError('Old connector processes did not exit')
            time.sleep(0.2)

    def backup(self):
        if self.running():
            raise RuntimeError('Cold backup requires PostgreSQL stopped')
        backup=self.work/'backup'
        if backup.exists():
            shutil.rmtree(backup)
        backup.mkdir(mode=0o700)
        copy_tree(self.data,backup/'pgdata')
        for index,external in enumerate(self.state['plan']['external']):
            copy_tree(external['path'],backup/('external-'+str(index)))
        config=backup/'config'
        config.mkdir()
        for field in ('config','hba','ident'):
            path=Path(self.state['plan']['server'][field])
            if path.is_file():
                copy_file(path,config/field)
        original=[]
        for index,item in enumerate(self.state['plan']['files']):
            path=Path(item['destination'])
            entry={'destination':str(path),'exists':path.exists(),'index':index}
            if path.exists():
                if not path.is_file() or path.is_symlink():
                    raise ValueError('Existing installed artifact is not a regular file')
                copy_file(path,backup/('artifact-'+str(index)))
                entry.update(self.file_metadata(path))
            original.append(entry)
        java=Path(self.state['plan']['java_target'])
        self.state['old_java_exists']=java.exists()
        if java.exists():
            copy_tree(java,backup/'java')
        self.state['original_files']=original
        self.save()

    def install(self):
        if self.running():
            raise RuntimeError('Installing SynchDB requires PostgreSQL stopped')
        for item in self.state['plan']['files']:
            source=Path(item['source']); destination=Path(item['destination'])
            if digest(source)!=item['sha256']:
                raise ValueError('Staged artifact changed since preflight: '+str(source))
            temporary=destination.with_name(destination.name+'.synchdb-upgrade')
            copy_file(source,temporary)
            temporary.chmod(0o644)
            if os.geteuid()==0:
                original=next(entry for entry in self.state['original_files'] if entry['destination']==str(destination))
                os.chown(temporary,original.get('uid',0),original.get('gid',0))
            os.replace(temporary,destination)
        target=Path(self.state['plan']['java_target'])
        if tree_digest(self.state['plan']['java_source'])!=self.state['plan']['java_sha256']:
            raise ValueError('Staged Java artifacts changed since preflight')
        temporary=target.with_name(target.name+'.synchdb-upgrade')
        if temporary.exists():
            shutil.rmtree(temporary)
        copy_tree(self.state['plan']['java_source'],temporary)
        for directory,_,files in os.walk(temporary):
            Path(directory).chmod(0o755)
            if os.geteuid()==0:
                os.chown(directory,0,0)
            for filename in files:
                path=Path(directory)/filename
                if not path.is_symlink():
                    path.chmod(0o644)
                    if os.geteuid()==0:
                        os.chown(path,0,0)
        if target.exists():
            shutil.rmtree(target)
        os.replace(temporary,target)

    def catalogs(self):
        for database in self.state['plan']['databases']:
            version=self.query("SELECT extversion FROM pg_extension WHERE extname='synchdb';",database['name'])
            if version=='1.0':
                self.query("ALTER EXTENSION synchdb UPDATE TO '1.1';",database['name'])
            elif version!='1.1':
                raise ValueError('Unexpected extension version: '+version)

    def configure_compatibility(self):
        upgrade_settings.persist(self.query, self.state['plan']['compatibility_settings'])
        self.record('compatibility_settings_persisted',
                    settings=self.state['plan']['compatibility_settings']['set'])

    def import_checkpoints(self):
        claims=json.dumps(self.state['plan']['claims'],separators=(',',':'))
        for database in self.state['plan']['databases']:
            schema=helper.identifier(database['schema'])
            for row in database['connectors']:
                original=self.data/'pg_synchdb'/(legacy_filename(row)+'_offsets.dat')
                if not original.exists() and not original.is_symlink() and row['restore']=='STOPPED':
                    history=original.with_name(legacy_filename(row)+'_schemahistory.dat')
                    if history.exists() or history.is_symlink():
                        raise ValueError('Existing schema history has no matching offset; preserve the old deployment for recovery')
                    cache=row.get('infinispan_resume')
                    if cache and ((self.data/cache['old_cache_directory']).exists() or
                                  (self.data/cache['old_cache_directory']).is_symlink()):
                        raise ValueError('Existing Infinispan cache has no matching offset; preserve the old deployment for recovery')
                    self.record('never_started_connector',database=database['name'],connector=row['name'])
                    continue
                existing=self.json_query("SELECT coalesce(json_agg(json_build_object('origin',a.origin,'generation',a.generation,'phase',a.phase)),'[]') FROM "+
                    schema+'.synchdb_artifacts a JOIN '+schema+'.synchdb_conninfo c USING(connector_id) WHERE a.active AND c.name='+literal(row['name'])+';',database['name'])
                if existing:
                    if len(existing)!=1 or existing[0]['origin']!='LEGACY_COPY' or existing[0]['phase']!='ENGINE_OWNED':
                        raise ValueError('An incompatible artifact generation already exists')
                    self.record('already_imported',database=database['name'],connector=row['name'],artifact=existing[0])
                    continue
                generation=self.query('SELECT '+schema+'.synchdb_artifact_migrate_legacy('+literal(row['name'])+'::name,'+
                    literal(claims)+'::jsonb);',database['name'])
                self.record('imported',database=database['name'],connector=row['name'],generation=generation)

    def restore_running(self):
        if 'compatibility_settings' in self.state['plan']:
            actual = upgrade_settings.verify(self.json_query, self.state['plan']['compatibility_settings'])
            self.record('compatibility_settings_verified', settings=actual)
        self.state['cdc_may_have_started']=True
        self.save()  # Durable before any command can reach the source.
        for database in self.state['plan']['databases']:
            schema=helper.identifier(database['schema'])
            for row in database['connectors']:
                if row['restore']!='RUNNING':
                    continue
                identity=database['name']+'\x1f'+row['name']
                operation=self.state.setdefault('operations',{}).get(identity)
                if operation is None:
                    # Remember the submission boundary before sending SQL. A
                    # lost result is recovered by reading the persistent intent.
                    pending=self.state.setdefault('submitting',{})
                    if identity not in pending:
                        baseline=self.query('SELECT coalesce((SELECT last_operation FROM '+schema+'.synchdb_control_state WHERE connector_name='+literal(row['name'])+'),0);',database['name'])
                        pending[identity]={'baseline':int(baseline)}
                        self.save()
                    baseline=pending[identity].get('baseline',0) if isinstance(pending[identity],dict) else 0
                    mode='initial' if row['connector']=='mysql' else 'no_data'
                    # Reconcile and submit under a transaction-scoped lock.
                    # A previous psql whose result was lost must finish its
                    # transaction before this execution decides to submit.
                    operation=int(self.query('BEGIN; SELECT pg_advisory_xact_lock(pg_catalog.hashtextextended('+literal('synchdb-upgrade:'+identity)+',0)); '+
                        "SELECT coalesce((SELECT last_operation FROM "+schema+'.synchdb_control_state WHERE connector_name='+literal(row['name'])+
                        ' AND last_operation>'+str(baseline)+'), '+schema+'.synchdb_control('+literal(row['name'])+'::name,\'START\','+literal(mode)+')); COMMIT;',database['name']))
                    self.state['operations'][identity]=operation
                    pending.pop(identity,None)
                    self.save()
                deadline=time.monotonic()+self.timeout
                while True:
                    receipt=self.json_query('SELECT '+schema+'.synchdb_operation('+str(int(operation))+',1000);',database['name'])
                    if receipt['status']=='SUCCEEDED':
                        break
                    if receipt['status'] not in ('PENDING','EXECUTING'):
                        raise RuntimeError('Connector operation failed: '+str(self.clean(receipt)))
                    if time.monotonic()>=deadline:
                        raise RuntimeError('Connector operation still pending; use --resume without resubmitting it')

    def verify(self):
        results=[]
        for check in self.state['plan']['checks']:
            statement=Path(check['sql_file']).read_text()
            deadline=time.monotonic()+self.timeout
            while True:
                actual=self.json_query('BEGIN READ ONLY;\n'+statement+'\nCOMMIT;',check['database'])
                okay=actual==check['expected'] if 'expected' in check else bool(actual)
                if okay or time.monotonic()>=deadline:
                    break
                time.sleep(0.5)
            results.append({'database':check['database'],'sql_file':check['sql_file'],'passed':okay,'actual':actual})
            self.record('data_check',**results[-1])
            if not okay:
                raise RuntimeError('Configured data check failed')
        self.state['verification']=results
        self.state['business_data_validation']='CONFIGURED_CHECKS_PASSED' if results else 'NOT_CONFIGURED'
        self.state['paused_kept_stopped']=[{'database':db['name'],'connector':row['name']} for db in self.state['plan']['databases'] for row in db['connectors'] if row['restore']=='PAUSED']
        self.save()

    def stage(self, name, action):
        if name in self.state['completed']:
            return
        self.state['current_stage']=name
        self.save()
        print('Running: '+name,flush=True)
        self.record('stage_started',stage=name)
        action()
        self.state['completed'].append(name)
        self.save()
        self.record('stage_completed',stage=name)

    def execute(self, resume=False):
        lock_path=self.data.parent/('.synchdb-upgrade-'+hashlib.sha256(str(self.data).encode()).hexdigest()[:16]+'.lock')
        descriptor=os.open(lock_path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
        try:
            try:
                fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError('An upgrade is already running for this PGDATA')
            self.execute_locked(resume)
        finally:
            os.close(descriptor)

    def execute_locked(self, resume=False):
        if self.work.exists():
            if not resume:
                raise ValueError('work_dir already exists; use --resume for this execution')
            if self.work.is_symlink() or self.work.stat().st_mode & 0o077 or self.work.stat().st_uid!=os.geteuid():
                raise ValueError('work_dir must be private and not a symlink')
            state_file=self.work/'state.json'
            if state_file.is_symlink() or state_file.stat().st_uid!=os.geteuid() or state_file.stat().st_mode & 0o077:
                raise ValueError('Saved state must be private and owned by the executing OS user')
            self.state=json.loads((self.work/'state.json').read_text())
            if self.state['config_hash']!=self.config_hash:
                raise ValueError('Configuration changed; resume requires the original configuration')
            if self.state.get('rolled_back'):
                raise ValueError('This execution was rolled back; inspect the journal and use a new work_dir')
            control=self.command([self.bin/'pg_controldata','-D',self.data],environment={**self.env,'LC_ALL':'C'})
            identifiers=[line.split(':',1)[1].strip() for line in control.stdout.splitlines() if line.startswith('Database system identifier:')]
            if identifiers!=[self.state['plan']['server']['system_identifier']]:
                raise ValueError('Saved execution belongs to a different PostgreSQL cluster')
        else:
            if resume:
                raise ValueError('There is no saved execution to resume')
            plan=self.discover()
            self.work.mkdir(mode=0o700,parents=True)
            self.state={'format':'synchdb-automatic-upgrade-v1','config_hash':self.config_hash,'plan':plan,
                        'completed':[],'cdc_may_have_started':False,'owner_uid':self.owner.pw_uid,
                        'owner_gid':self.owner.pw_gid}
            self.save()
            atomic_json(self.work/'claims.json',plan['claims'])
        if self.state.get('complete'):
            print('Already complete; no commands submitted')
            return
        if 'compatibility_settings' not in self.state['plan']:
            if 'stop_server' in self.state['completed'] or not self.running():
                raise ValueError('Saved execution predates the compatibility-settings snapshot; '
                                 'inspect its cold backup and original configuration before continuing')
            self.state['plan']['compatibility_settings'] = upgrade_settings.capture(self.json_query)
            self.save()
        try:
            self.stage('stop_connectors',self.stop_connectors)
            self.stage('stop_server',lambda:self.node('stop'))
            self.stage('cold_backup',self.backup)
            self.stage('install',self.install)
            self.stage('maintenance_start',lambda:self.node('start',maintenance=True))
            self.stage('compatibility_settings',self.configure_compatibility)
            self.stage('upgrade_catalogs',self.catalogs)
            self.stage('import_checkpoints',self.import_checkpoints)
            self.stage('maintenance_stop',lambda:self.node('stop',maintenance=True))
            self.stage('normal_start',lambda:self.node('start'))
            self.stage('restore_running',self.restore_running)
            self.stage('verify',self.verify)
            self.state['complete']=True
            self.state.pop('failure',None)
            self.save()
            print('Upgrade complete. Report: '+str(self.work/'state.json'))
            if not self.state['plan']['checks']:
                print('No business data check configured; worker readiness does not prove all business data correct')
        except Exception as error:
            self.state['failure']=str(self.clean(str(error)))
            self.save()
            self.record('failure',error=str(error),cdc_may_have_started=self.state['cdc_may_have_started'])
            print('Stopped at '+self.state.get('current_stage','unknown')+'; execution preserved in '+str(self.work),file=sys.stderr)
            raise


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--apply',action='store_true',help='execute the reviewed local upgrade')
    parser.add_argument('--resume',action='store_true',help='continue this saved execution without resubmitting successful operations')
    parser.add_argument('--claims-output',type=Path,
                        help='plan only: save the complete legacy ownership inventory to a new private JSON file')
    args=parser.parse_args(argv)
    try:
        if args.resume and not args.apply:
            raise ValueError('--resume requires --apply')
        if args.claims_output and args.apply:
            raise ValueError('--claims-output is only valid for a read-only plan')
        upgrade=Upgrade(args.config,args.apply)
        if args.apply:
            upgrade.execute(args.resume)
        else:
            plan=upgrade.plan()
            if args.claims_output:
                helper.new_private_file(args.claims_output,
                    json.dumps(plan['claims'],ensure_ascii=False,indent=2)+'\n')
            print(json.dumps(plan,ensure_ascii=False,indent=2))
        return 0
    except (OSError,ValueError,RuntimeError,subprocess.SubprocessError) as error:
        print('Upgrade failed: '+str(error),file=sys.stderr)
        return 1


if __name__=='__main__':
    sys.exit(main())
