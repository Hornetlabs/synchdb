---
weight: 70
---
# Connector Auto Launcher

## **Enable SynchDB Auto Launcher**
A connection worker becomes eligible for automatic Launch when `synchdb_start_engine_bgw()` is issued on a particular `connector name`. Likewise, it becomes ineligible when `synchdb_stop_engine_bgw()` is issued. 

Automatic connector launcher can be enabled by:

* Add `synchdb` to `shared_preload_libraries` GUC option in postgresql.conf
* Set new GUC option `synchdb.synchdb_auto_launcher` to true in postgresql.conf
* Restart the PostgreSQL server for the changes to take effect

For example:
```
shared_preload_libraries = 'synchdb'
synchdb.synchdb_auto_launcher = true
```

At startup, the SynchDB extension is preloaded early. With `synchdb.synchdb_auto_launcher` set to true, SynchDB spawns a `synchdb_auto_launcher` background worker, which in turn spawns one
`synchdb_db_launcher` worker for every connectable, non-template database. Each `synchdb_db_launcher` retrieves all the conninfos in the `synchdb_conninfo` table (if it exists) that are marked as `active` (i.e., with the `isactive` flag set to `true`), and then starts them automatically as separate background workers, in the same way as when `synchdb_start_engine_bgw()` is called. The `synchdb_auto_launcher` worker exits after every `synchdb_db_launcher` finishes its job.
