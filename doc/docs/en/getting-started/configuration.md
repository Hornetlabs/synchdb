---
weight: 40
---
# SynchDB Configuration

SynchDB supports the following GUC variables in postgresql.conf. These are common parameters that apply the all connectors managed by SynchDB:

| GUC Variable| Type | Default Value | Description |
|-|-|-|-|
| synchdb.naptime | integer | 100 | The delay in milliseconds between each data polling from Debezium runner engine |
| synchdb.dml_use_spi | boolean | false | Option to use SPI to handle DML operations |
| synchdb.synchdb_auto_launcher | boolean | true | Option to automatically launch active SynchDB connector workers. This option only works when SynchDB is included in `shared_preload_library` GUC option |
| synchdb.dbz_batch_size | integer | 2048 | The maximum number of change events produced by Debezium embedded engine for SynchDB to process. This batch of changes is processed within a single transaction by SynchDB |
| synchdb.dbz_queue_size | integer | 8192 | The maximum size (measured in number of change events) of Debezium embedded engine's change event queue. It should be set at least twice of `synchdb.dbz_batch_size` |
| synchdb.dbz_connect_timeout_ms | integer | 30000 | The timeout value in milliseconds for Debezium embedded engine to established an initial connection to a remote database |
| synchdb.dbz_query_timeout_ms | integer | 600000 | The timeout value in milliseconds for Debezium embedded engine to execute a query on a remote database |
| synchdb.dbz_skipped_oeprations | string | "t" | A comma-separated list of operations Debezium shall skip when processing change events. "c" is for inserts, "u" is for updates, "d" is for deletes, "t" is for truncates |
| synchdb.jvm_max_heap_size | integer | 1024 | The maximum heap size in MB to be allocated to Java Virtual Machine (JVM) when starting a connector. |
| synchdb.dbz_snapshot_thread_num | integer | 2 | The number of threads Debezium embedded connector should spawn during initial snapshot. Please note that according to Debezium, multi-threaded snapshot is an `incubating feature` |
| synchdb.dbz_snapshot_fetch_size | integer | 0 | The number of rows Debezium embedded connector should fetch at a time during initial snapshot. Set it to 0 to let the engine choose automatically |
| synchdb.<br>dbz_snapshot_min_row_<br>to_stream_results | integer | 0 | The minimum number of rows a remote table should contain before Debezium embedded engine will switch to streaming mode during initial snapshot. Set it to 0 to always switching to stream mode |
| synchdb.<br>dbz_incremental_<br>snapshot_chunk_size | integer | 2048 | The maximum number of change events produced by Debezium embedded engine for SynchDB to process during incremental snapshot |
| synchdb.<br>dbz_incremental_<br>snapshot_watermarking_strategy | string | "insert-insert | The watermarking strategy used by Debezium embedded engine to resolve potential conflicts during incremental snapshot. Possible values are "insert-insert" and "insert-delete" |
| synchdb.dbz_offset_flush_interval_ms | integer | 60000 | The interval in milliseconds that Debezium embedded engine flushes offset data to disk |
| synchdb.<br>dbz_capture_only_selected_table_ddl | boolean | true | whether or not Debezium embedded engine should capture the schema of all tables (false) or selected tables(true) during initial snapshot |
| synchdb.max_connector_workers | integer | 30 | the maximum number of connector workers that can be running at a time |
| synchdb.error_handling_strategy | enum | "exit" | configures the error handling strategy of a connector worker. Possible values are "exit" for exiting on error, "skip" for continuing on error, "retry" for retrying on error |
| synchdb.dbz_log_level | enum | "warn" | the log level setting for Debezium Runner. Possible values are "debug", "info", "warn", "error", "all", "fatal", "off", "trace" |
| synchdb.log_change_on_error | boolean | true | whether the connector should log the original JSON change event in case of error |
| synchdb.jvm_max_direct_buffer_size | integer | 1024 | The maximum direct buffer size in MB to be allocated to hold JSON change events |
| synchdb.dbz_logminer_stream_mode | enum | "uncommitted" | The streaming mode for Debezium based Oracle connector. The default is uncommitted, which means all the changes streamed from Oracle via Debezium is uncommitted. This indicates Debezium has to do some work to ensure the integrity of transactions and all associated changes. Setting to "committed" shifts this work on Oralce side |
| synchdb.olr_connect_timeout_ms | integer | 5000 | (affects OLR connector only) the connect timeout in milliseconds when connecting to openlog replicator service |
| synchdb.olr_read_timeout_m | integer | 5000 | (affects OLR connector only) the read timeout in milliseconds when reading from a socket |
| synchdb.olr_snapshot_engine | enum | "debezium" | the underlining engine to complete the initial snapshot process. Could be "debezium" or "fdw". If "fdw" is selected, you need to ensure the corresponding FDW is installed prior. For example, for Oracle connector, ensure "oracle_fdw" is preinstalled. |
| synchdb.cdc_start_delay_ms | integer | 0 | a delay waited after initial snapshot completes and before CDC streaming begins. |
| synchdb.fdw_migrate_with_subtx | boolean | true | option to use sub transactions to migrate a table during FDW based snapshot |
| synchdb.letter_casing_strategy | enum | "lowercase" | how should synchdb deal with potentially different object letter casings from different database source. Possible values are: "lower" for normalizing the lower case, "upper" to normalize to upper case, or "asis" to preserve whatever letter casing it receives |


## **Technical Notes**

- GUC (Grand Unified Configuration) variables are global configuration parameters in PostgreSQL
- Values are set in the `postgresql.conf` file
- Changes require a server restart to take effect
- `shared_preload_library` is a critical system configuration that determines which libraries are loaded at startup, synchdb must be put here to enable [connector auto launcher](../../user-guide/connector_auto_launcher/)

## **Configuration Examples**

```conf
# Example configuration in postgresql.conf
synchdb.naptime = 1000                                                  # Increase wait time to 1 second
synchdb.dml_use_spi = true                                              # Enable SPI usage for DML operations
synchdb.synchdb_auto_launcher = true                                    # Enable automatic connector startup
synchdb.dbz_batch_size=4096                                             # Each batch can have at most 4096 change events
synchdb.dbz_queue_size=8192                                             # Debezium will use 8192 change event queue size
synchdb.jvm_max_heap_size=2048                                          # 2GB heap memory to be allocated to a connector
synchdb.dbz_snapshot_fetch_size=0                                       # Let Debezium figure out the optimal number of rows to fetch during initial snapshot
synchdb.dbz_min_row_to_stream_results=0                                 # Always stream the results during initial snapshot
synchdb.dbz_snapshot_thread_num=1                                       # Single thread during Debezium's initial snapshot
synchdb.dbz_incremental_snapshot_chunk_size=4096                        # Incremental snapshot produces change events in batches of 4096 max
synchdb.dbz_incremental_snapshot_watermarking_strategy='insert_insert'  # Use insert_insert watermarking strategy
synchdb.dbz_offset_flush_interval_ms=60000                              # Flush offset data to disk every minute if needed    
synchdb.dbz_capture_only_selected_table_ddl=false                       # Debezium will only capture the schema of selected tables rather than all tables
synchdb.max_connector_workers=10                                        # 10 connector workers can be run at a time
synchdb.error_handling_strategy='retry'                                 # connector should retry on error
synchdb.dbz_log_leve='error'                                            # Debezium Runner should log error messages only
synchdb.log_change_on_error=true                                        # log JSON change event on error
synchdb.cdc_start_delay_ms=30000                                        # wait 30s after snapshot completes and before CDC begins
synchdb.olr_snapshot_engine="fdw"                                       # use FDW based snapshot engine to complete the snapshot process
synchdb.letter_casing_strategy="asis"                                   # preserve all object letter casings as they appear in the sources
```

## **Usage Recommendations**

1. **synchdb.naptime**
    - Lower values: Higher update frequency but more system load
    - Higher values: Lower system load but less frequent updates
    - Adjust based on data latency requirements

2. **synchdb.dml_use_spi**
    - Enable if specific SPI integration is needed
    - Keep `false` for standard DML operations

3. **synchdb.synchdb_auto_launcher**
    - Recommended to keep `true` for automatic connector resume upon PostgreSQL restarts
    - Change to `false` only if manual connector control is required

4. **synchdb.dbz_batch_size**
    - Lower values: Slower processing of change events at lower JVM memory usage
    - Higher values: Faster processing of change events at higher JVM memory usage
    - Adjust based on resource requirements

5. **synchdb.dbz_queue_size**
    - Lower values: Smaller Debezium queue to hold change events
    - Higher values: Larger Debezium queue to hold change events
    - Need to be set at least twice of `synchdb.dbz_batch_size`

6. **synchdb.jvm_max_heap_size**
    - Lower values: Smaller heap memory allocated to JVM
    - Higher values: Larger heap memory allocated to JVM
    - Adjust based on system resource and workload requirements
    - Needs increase when working with large number of tables

7. **synchdb.dbz_snapshot_fetch_size**
    - Lower values: Less rows to be fetched from a table during snapshot
    - Higher values: More rows to be fetched from a table during snapshot
    - Recommended to keep it 0 to let Debezium figure out an optimal value

8. **synchdb.dbz_min_row_to_stream_results**
    - Lower values: Less JVM memory requirement, slower processing of change events
    - Higher values: More JVM memory requirement, faster processing of change events
    - Recommended to keep it 0 to let Debezium use streaming mode always to reduce memory usage

9. **synchdb.dbz_snapshot_thread_num**
    - Lower values: Slower data export to SynchDB for processing 
    - Higher values: Faster data export to SyncDB for processing
    - Recommended to set it to the same number of CPU cores

10. **synchdb.dbz_incremental_snapshot_chunk_size**
    - Lower values: Slower processing of change events at lower JVM memory usage during incremental snapshot
    - Higher values: Faster processing of change events at higher JVM memory usage during incremental snapshot
    - Recommended to set it the same as `synchdb.dbz_batch_size` and adjust Adjust based on resource requirements

11. **synchdb.dbz_offset_flush_interval_ms**
    - Lower values: More frequent update to offset file, more IO, less old batches to re-preocess after fault restored
    - Higher values: Less frequent update to offset file, less IO, more old batches to re-preocess after fault restored
    - Recommended to set it to 60000 as Debezium's recommendation

12. **synchdb.max_connector_workers**
    - Lower values: less connector workers can be run at a time, less shared memory requirement
    - Higher values: more connector workers can be run at a time, more shared memory requirement

## **Performance Considerations**

- Adjust `synchdb.naptime` based on system load and latency requirements
- Adjust `synchdb.dbz_batch_size` and `synchdb.dbz_queue_size` higher to increase processing throughput
- Adjust `synchdb.jvm_max_heap_size` based on workload
    - Smaller number of tables (10k or less) + large amount of data per table: 512MB ~ 1024MB should suffice
    - Larger number of tables (100k or more) + moderate amount of data per table: consider increasing to 2048MB or above
- Set `synchdb.dbz_snapshot_fetch_size` to 0 to let Debezium pick optimal fetch value
- Set `synchdb.dbz_snapshot_thread_num` to match number of CPU cores
- Set `synchdb.dbz_min_row_to_stream_results` to 0 to always use stream mode to reduce memory usage

## **Common Use Cases**

### **High-Throughput Systems**

```conf
synchdb.naptime = 10            # Faster polling for real-time updates
synchdb.dml_use_spi = false     # Standard DML for better performance
synchdb.dbz_batch_size = 16384
synchdb.dbz_queue_size = 32768
synchdb.jvm_max_heap_size = 2048
synchdb.dbz_snapshot_thread_num = 4
synchdb.dbz_snapshot_fetch_size = 0
synchdb.dbz_min_row_to_stream_results = 0
```

### **Resource-Constrained Systems**

```conf
synchdb.naptime = 1000          # Reduced polling frequency
synchdb.dml_use_spi = false     # Minimize additional overhead
synchdb.dbz_batch_size = 1024
synchdb.dbz_queue_size = 2048
synchdb.jvm_max_heap_size = 512
synchdb.dbz_snapshot_thread_num = 1
synchdb.dbz_snapshot_fetch_size = 0
synchdb.dbz_min_row_to_stream_results = 0
```

### **Development/Testing**

```conf
synchdb.naptime = 500           # Default polling
synchdb.dml_use_spi = true      # Enable advanced features for testing
synchdb.dbz_batch_size = 2048
synchdb.dbz_queue_size = 4096
synchdb.jvm_max_heap_size = 1024
synchdb.dbz_snapshot_thread_num = 2
synchdb.dbz_snapshot_fetch_size = 0
synchdb.dbz_min_row_to_stream_results = 0
```

## **Troubleshooting**

1. **High CPU Usage**
    - Increase `synchdb.naptime`
    - Review DML operation patterns
    - Reduce `synchdb.dbz_batch_size` and `synchdb.dbz_queue_size`
    - Increase `synchdb.dbz_snapshot_thread_num`

2. **Data Latency Issues**
    - Decrease `synchdb.naptime`
    - Increase `synchdb.dbz_batch_size` and `synchdb.dbz_queue_size`
    - Check network connectivity
    - Increase `shared_buffers`
    - Split workload to more connectors rather than just one
    - Start the connector with `no_data` mode to obtain schema only and begin CDC rather than `initial` mode which capture both schema and initial data before CDC begins.

3. **Startup Problems**
    - Verify `shared_preload_library` configuration
    - Check error messages from `synchdb_get_state()`
    - Check connector worker status

4. **Out of Memory Problems**
    - Increase `synchdb.jvm_max_heap_size`
    - Increase `shared_buffers`

## **Best Practices**

1. **Initial Setup**
    - Start with default values
    - Monitor system performance
    - Adjust gradually based on requirements

2. **Production Environment**
    - Document all configuration changes
    - Test changes in staging first
    - Maintain backup of working configurations

3. **Monitoring**
    - Track system resource usage
    - Monitor data synchronization latency
    - Log configuration changes