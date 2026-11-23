#!/usr/bin/env python3
"""Verify generated SQL and helper hashes without connecting to a database."""
import argparse
import hashlib
import json
from pathlib import Path
import sys


BUNDLE=Path(__file__).resolve().parents[1]
SOURCE_COMMIT='0756e247ca07083244559cb4616c5dedf1b3da97'


def checked_path(root,name):
    path=(root/name).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError('Manifest path is missing or outside its root: '+name)
    return path


def verify(manifest,repo=None):
    metadata=json.loads(manifest.read_text(encoding='utf-8'))
    if metadata.get('format')!='synchdb-upgrade-bundle' or metadata.get('format_version')!=1:
        raise ValueError('Unrecognized bundle manifest')
    bundle=manifest.resolve().parents[1]
    if (metadata.get('source_tag')!='v1.4' or metadata.get('source_release')!='v1.4'
            or metadata.get('source_version')!='1.0' or metadata.get('target_version')!='1.1'
            or metadata.get('source_commit')!=SOURCE_COMMIT):
        raise ValueError('Bundle must identify release tag v1.4 (SQL 1.0) as its source')
    entries=[(manifest.parent,metadata['sql_file'],metadata['sql_sha256'])]
    entries.extend((bundle,item['path'],item['sha256']) for item in metadata['tools'])
    profile_entry=metadata['source_catalog_profile']
    profile_path=checked_path(bundle,profile_entry['path'])
    entries.append((bundle,profile_entry['path'],profile_entry['sha256']))
    runtime_sources=metadata.get('runtime_sources',[])
    if metadata.get('target_worktree_dirty') is True and not runtime_sources:
        raise ValueError('Dirty target checkout requires runtime source hashes')
    if repo:
        entries.extend((repo,item['path'],item['sha256']) for item in metadata['sql_sources'])
        entries.extend((repo,item['path'],item['sha256']) for item in runtime_sources)
        entries.append((repo,'Makefile',metadata['makefile_sha256']))
    failures=[]
    for root,name,expected in entries:
        path=checked_path(root,name)
        if hashlib.sha256(path.read_bytes()).hexdigest()!=expected:
            failures.append(name)
    if failures:
        raise ValueError('Hash mismatch: '+', '.join(failures))
    profile=json.loads(profile_path.read_text(encoding='utf-8'))
    for field in ('source_tag','source_release','source_commit','source_version','source_tag_object','source_sql'):
        if profile.get(field)!=metadata.get(field):
            raise ValueError('Baseline profile/manifest identity mismatch: '+field)
    if len(profile.get('catalog',{}).get('functions',[]))!=53:
        raise ValueError('Baseline profile must contain the v1.4 53-function catalog')
    return {'checked_files':len(entries),'source_tag':metadata['source_tag'],
            'source_release':metadata['source_release'],'source_version':metadata['source_version'],
            'target_version':metadata['target_version'],'source_sql_sha256':metadata['source_sql']['sha256'],
            'source_commit':metadata['source_commit'],
            'target_commit':metadata['target_commit'],
            'target_worktree_dirty':metadata.get('target_worktree_dirty'),
            'runtime_source_files':len(runtime_sources),
            'runtime_sources_verified':bool(repo and runtime_sources),
            'runtime_build_requirement':metadata.get('runtime_build_requirement',
                'Use C/JAR artifacts matching the target source tree'),
            'sql_sha256':metadata['sql_sha256']}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest',type=Path,default=BUNDLE/'sql/manifest.json')
    parser.add_argument('--repo',type=Path,help='also verify SQL and recorded runtime sources against a matching repository')
    args=parser.parse_args(argv)
    try:
        print(json.dumps(verify(args.manifest,args.repo),ensure_ascii=False,indent=2))
    except (OSError,ValueError,KeyError,TypeError) as error:
        print('Error: '+str(error),file=sys.stderr)
        return 1
    return 0


if __name__=='__main__':
    sys.exit(main())
