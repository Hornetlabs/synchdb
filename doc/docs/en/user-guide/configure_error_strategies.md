# Configure Error Handling Strategies

## **Configure Error Handling Strategies**

During initial snapshot or CDC, errors such as conflicting primary keys, invalid data...etc may occur that SynchDB needs to know how to handle them. For errors that occur during Synchdb's processing or during applying to PostgreSQL, there are several available strategies to handle them via the configruation parameter "synchdb.error_handling_strategy".

Please note that the error strategies mentioned below are only executed if the error originated or detected during the change event processing on C side. If the error originated from Debezium Runner on the Java side, for example, connectivity issues or issues with connection to the remote database, the above strategies will not be executed. In this case, the error messages of Debezium Runner will be propagated to SynchDB on the C side, and displayed in `synchdb_state_view()` and the connector will exit.

### **exit (default)**

This is the default error strategy, which causes the connector worker to exit when an error has occured. The batch that it is currently working on, which resulted in error will not be marked as completed and the change events that have been successfully completed in the same batch will not be committed. User is expected to check the error message as returned in `synchdb_state_view()` or the log file to resolve the error. When the connector worker is restarted, the connector will automatically retry the same batch that has failed previously.

### **retry**

This strategy adds a `restart_time` of 5 second to the connector worker, which causes PostgreSQL's bgworker engine to automatically start the worker every 5 second should it has exited. This means that when an error occurs, the connector worker will still exit, but unlike the `exit` strategy above, it will automatically be restarted by bgworker engine, which will retry on the same batch that has failed. It will continue to exit and restart until the error has been resolved.

### **skip**

As the name suggests, when in `skip` error strategy, any error that the connector worker encountered will not cause the worker to exit, the error messages, however, will still be written to the log or `synchdb_state_view()`, but the connector itself will ignore the error and move on to processing the next change event and even the next batch. 

## **View the Last Error Message of a Connector**

As mentioned above, errors originated from Debezium Runner on the Java side or SynchDB on the C side will have the error messages propagated and displayed in `synchdb_state_view()`. For example:

```sql
select name, pid, err from synchdb_state_view;

   name    | pid |                                                                                                          err
-----------+-----+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 mysqlconn |  -1 | Connector configuration is not valid. Unable to connect: Communications link failure  The last packet sent successfully to the server was 0 milliseconds ago. The driver has not received any packets from the server.
(1 row)

```

If the error happens during processing of a JSON change event, and the GUC parameter `synchdb.log_change_on_error` is set to true, SynchDB will also output the original JSON change event in the PostgreSQL log file that caused the error for troubleshooting. 