package com.example;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.ChangeEvent;
import io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.LinkedList;
import java.util.Queue;
import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DebeziumRunner {
	private static Logger logger = Logger.getRootLogger();
	private List<String> changeEvents = new ArrayList<>();
	private DebeziumEngine<ChangeEvent<String, String>> engine;
	private ExecutorService executor;
	private Future<?> future;
	private String lastDbzMessage;
	private boolean lastDbzSuccess;
	private Throwable lastDbzError;
	private HashMap<Integer, ChangeRecordBatch> activeBatchHash = new HashMap<>();
	private BatchManager batchManager = new BatchManager();

	final int TYPE_MYSQL = 1;
	final int TYPE_ORACLE = 2;
	final int TYPE_SQLSERVER = 3;
	final int BATCH_QUEUE_SIZE = 5;
	
	final int LOG_LEVEL_UNDEF = 0;
	final int LOG_LEVEL_ALL = 1;
	final int LOG_LEVEL_DEBUG = 2;
	final int LOG_LEVEL_INFO = 3;
	final int LOG_LEVEL_WARN = 4;
	final int LOG_LEVEL_ERROR = 5;
	final int LOG_LEVEL_FATAL = 6;
	final int LOG_LEVEL_OFF = 7;
	final int LOG_LEVEL_TRACE = 8;
	

	/* MyParameters class - encapsulates all supported debezium parameters */
	public class MyParameters
	{
		/* required parameters - needed to construct this class */
		private String connectorName;
		private int connectorType;
		private String hostname;
		private int port;
		private String user;
		private String password;
		private String database;
		private String table;
		private String snapshotMode;

		/* extended parameters - set incrementally */
		private int batchSize;
		private int queueSize;
		private String skippedOperations;
		private int connectTimeout;
		private int queryTimeout;
		private int snapshotThreadNum;
		private int snapshotFetchSize;
		private int snapshotMinRowToStreamResults;
		private int incrementalSnapshotChunkSize;
		private String incrementalSnapshotWatermarkingStrategy;
		private int offsetFlushIntervalMs;
		private boolean captureOnlySelectedTableDDL;
		private String sslmode;
		private String sslKeystore;
		private String sslKeystorePass;
		private String sslTruststore;
		private String sslTruststorePass;
		private int logLevel;

		/* constructor requires all required parameters for a connector to work */
		public MyParameters(String connectorName, int connectorType, String hostname, int port, String user, String password, String database, String table, String snapshotMode)
		{
			this.connectorName = connectorName;
			this.connectorType = connectorType;
			this.hostname = hostname;
			this.port = port;
			this.user = user;
			this.password = password;
			this.database = database;
			this.table = table;
			this.snapshotMode = snapshotMode;
		}
		public MyParameters setBatchSize(int batchSize)
		{
			this.batchSize = batchSize;
			return this;
		}
		public MyParameters setQueueSize(int queueSize)
		{
			this.queueSize = queueSize;
			return this;
		}
		public MyParameters setSkippedOperations(String skippedOperations)
		{
			this.skippedOperations = skippedOperations;
			return this;
		}
		public MyParameters setConnectTimeout(int connectTimeout)
		{
			this.connectTimeout = connectTimeout;
			return this;
		}
		public MyParameters setQueryTimeout(int queryTimeout)
		{
			this.queryTimeout = queryTimeout;
			return this;
		}
		public MyParameters setSnapshotThreadNum(int snapshotThreadNum)
		{
			this.snapshotThreadNum = snapshotThreadNum;
			return this;
		}
		public MyParameters setSnapshotFetchSize(int snapshotFetchSize)
		{
			this.snapshotFetchSize = snapshotFetchSize;
			return this;
		}
		public MyParameters setSnapshotMinRowToStreamResults(int snapshotMinRowToStreamResults)
		{
			this.snapshotMinRowToStreamResults = snapshotMinRowToStreamResults;
			return this;
		}
		public MyParameters setIncrementalSnapshotChunkSize(int incrementalSnapshotChunkSize)
		{
			this.incrementalSnapshotChunkSize = incrementalSnapshotChunkSize;
			return this;
		}
		public MyParameters setIncrementalSnapshotWatermarkingStrategy(String incrementalSnapshotWatermarkingStrategy)
		{
			this.incrementalSnapshotWatermarkingStrategy = incrementalSnapshotWatermarkingStrategy;
			return this;
		}
		public MyParameters setOffsetFlushIntervalMs(int offsetFlushIntervalMs)
		{
			this.offsetFlushIntervalMs = offsetFlushIntervalMs;
			return this;
		}
		public MyParameters setCaptureOnlySelectedTableDDL(boolean captureOnlySelectedTableDDL)
		{
			this.captureOnlySelectedTableDDL = captureOnlySelectedTableDDL;
			return this;
		}
		public MyParameters setSslmode(String sslmode)
		{
			this.sslmode = sslmode;
			return this;
		}
		public MyParameters setSslKeystore(String sslKeystore)
		{
			this.sslKeystore = sslKeystore;
			return this;
		}
		public MyParameters setSslKeystorePass(String sslKeystorePass)
		{
			this.sslKeystorePass = sslKeystorePass;
			return this;
		}
		public MyParameters setSslTruststore(String sslTruststore)
		{
			this.sslTruststore = sslTruststore;
			return this;
		}
		public MyParameters setSslTruststorePass(String sslTruststorePass)
		{
			this.sslTruststorePass = sslTruststorePass;
			return this;
		}
		public MyParameters setLogLevel(int logLevel)
		{
			this.logLevel = logLevel;
			return this;
		}

		/* add more setters here to incrementally set parameters */
		public void print()
		{
			logger.warn("connectorName = " + this.connectorName);
			logger.warn("connectorType= " + this.connectorType);
			logger.warn("hostname = " + this.hostname);
			logger.warn("port = " + this.port);
			logger.warn("user = " + this.user);
			logger.warn("database = " + this.database);
			logger.warn("table = " + this.table);
			logger.warn("snapshotMode = " + this.snapshotMode);

			logger.warn("batchSize = " + this.batchSize);
			logger.warn("queueSize = " + this.queueSize);
			logger.warn("skippedOperations = " + this.skippedOperations);
			logger.warn("connectTimeout = " + this.connectTimeout);
			logger.warn("queryTimeout = " + this.queryTimeout);
			logger.warn("snapshotThreadNum = " + this.snapshotThreadNum);
			logger.warn("snapshotFetchSize = " + this.snapshotFetchSize);
			logger.warn("snapshotMinRowToStreamResults= " + this.snapshotMinRowToStreamResults);
			logger.warn("incrementalSnapshotChunkSize = " + this.incrementalSnapshotChunkSize);
			logger.warn("incrementalSnapshotWatermarkingStrategy = " + this.incrementalSnapshotWatermarkingStrategy);
			logger.warn("offsetFlushIntervalMs = " + this.offsetFlushIntervalMs);
			logger.warn("captureOnlySelectedTableDDL = " + this.captureOnlySelectedTableDDL);
			logger.warn("sslmode = " + this.sslmode);
			logger.warn("sslKeystore = " + this.sslKeystore);
			logger.warn("sslKeystorePass = " + this.sslKeystorePass);
			logger.warn("sslTruststore = " + this.sslTruststore);
			logger.warn("sslTruststorePass = " + this.sslTruststorePass);
			logger.warn("logLevel = " + this.logLevel);
		}

	}
	/* BatchMaanger represents a Batch request queue */
	public class BatchManager
	{
		private Queue<ChangeRecordBatch> batchQueue;
		private int batchid;
		private boolean isShutdown;

		public BatchManager()
		{
			this.batchQueue = new LinkedList<>();
			this.batchid = 0;
			this.isShutdown = false;
		}

		public synchronized int getQueueSize()
		{
			return batchQueue.size();
		}
		public synchronized void addBatch(ChangeRecordBatch batch) throws InterruptedException
		{
			while (batchQueue.size() >= BATCH_QUEUE_SIZE && !this.isShutdown)
			{
				wait();
			}

			if (this.isShutdown)
			{
				logger.warn("BatchManager has been shutdown...");
				return;
			}

			batch.batchid = this.batchid;
			batchQueue.offer(batch);
			this.batchid++;
			logger.info("added a batch task: id = " + batch.batchid + " size = " + batch.records.size());
			notifyAll();
		}

		public synchronized ChangeRecordBatch getNextBatch()
		{
			ChangeRecordBatch batch = batchQueue.poll();
			if (batch != null)
			{
            	notifyAll();
        	}
			return batch;
		}

		public synchronized void shutdown()
		{
			this.isShutdown = true;
			notifyAll();
		}
	}
	
	/* ChangeRecordBatch represents a batch with our own identifier 'batchid' added to it */
	public class ChangeRecordBatch
	{
		public int batchid;
		public List<ChangeEvent<String, String>> records;
		public DebeziumEngine.RecordCommitter committer;

		public ChangeRecordBatch(List<ChangeEvent<String, String>> records, DebeziumEngine.RecordCommitter committer) 
		{
			//this.records = new ArrayList<>(records);
			this.records = records;
			this.committer = committer;
		}
	}

	public void checkMemoryStatus()
	{
		MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
	    MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
	    MemoryUsage nonHeapUsage = memoryMXBean.getNonHeapMemoryUsage();

		logger.warn("Heap Memory:");
	    logger.warn("  Used: " + heapUsage.getUsed() + " bytes");
	    logger.warn("  Committed: " + heapUsage.getCommitted() + " bytes");
	    logger.warn("  Max: " + heapUsage.getMax() + " bytes");

		logger.warn("Non-Heap Memory:");
	    logger.warn("  Used: " + nonHeapUsage.getUsed() + " bytes");
	    logger.warn("  Committed: " + nonHeapUsage.getCommitted() + " bytes");
	    logger.warn("  Max: " + nonHeapUsage.getMax() + " bytes");
	}

	public void startEngine(MyParameters myParameters) throws Exception
	{
		String offsetfile = "/dev/shm/offsets.dat";
		String schemahistoryfile = "/dev/shm/schemahistory.dat";
		String signalfile= "/dev/shm/signalfile.dat";
		Properties props = new Properties();

		/* Initialize Logging */
		ConsoleAppender consoleAppender = new ConsoleAppender();
        consoleAppender.setLayout(new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n"));
        consoleAppender.setTarget(ConsoleAppender.SYSTEM_OUT);
        consoleAppender.activateOptions();
		logger.addAppender(consoleAppender);

		switch (myParameters.logLevel)
		{
			case LOG_LEVEL_ALL:
			{
				logger.setLevel(Level.ALL);
				break;
			}
			case LOG_LEVEL_DEBUG:
			{
				logger.setLevel(Level.DEBUG);
				break;
			}
			case LOG_LEVEL_INFO:
			{
				logger.setLevel(Level.INFO);
				break;
			}
			case LOG_LEVEL_ERROR:
			{
				logger.setLevel(Level.ERROR);
				break;
			}
			case LOG_LEVEL_FATAL:
			{
				logger.setLevel(Level.FATAL);
				break;
			}
			case LOG_LEVEL_OFF:
			{
				logger.setLevel(Level.OFF);
				break;
			}
			case LOG_LEVEL_TRACE:
			{
				logger.setLevel(Level.TRACE);
				break;
			}
			default:	
			case LOG_LEVEL_UNDEF:
			case LOG_LEVEL_WARN:
			{
				logger.setLevel(Level.WARN);
				break;
			}
		}

		myParameters.print();
        /* Setting connector specific properties */
        props.setProperty("name", "engine");
		switch(myParameters.connectorType)
		{
			case TYPE_MYSQL:
			{
		        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
				int hash = myParameters.connectorName.hashCode();
				long unsignedhash = Integer.toUnsignedLong(hash);
				long serverid = 1 + (unsignedhash % (4294967295L - 1));

				logger.warn("derived server id " + serverid);
				props.setProperty("database.server.id", String.valueOf(serverid));	/* todo: make configurable */

				offsetfile = "pg_synchdb/mysql_" + myParameters.connectorName + "_offsets.dat";
				schemahistoryfile = "pg_synchdb/mysql_" + myParameters.connectorName + "_schemahistory.dat";
				signalfile = "pg_synchdb/mysql_" + myParameters.connectorName + "_signal.dat";

				if (myParameters.database.equals("null"))
					logger.warn("database is null - skip setting database.include.list property");
				else
				{
					props.setProperty("database.include.list", myParameters.database);
					props.setProperty("database.names", myParameters.database);
				}

				if (myParameters.sslmode != null)
					props.setProperty("database.ssl.mode", myParameters.sslmode);
				if (myParameters.sslKeystore != null)
					props.setProperty("database.ssl.keystore", myParameters.sslKeystore);
				if (myParameters.sslKeystorePass != null)
					props.setProperty("database.ssl.keystore.password", myParameters.sslKeystorePass);
				if (myParameters.sslTruststore != null)
					props.setProperty("database.ssl.truststore", myParameters.sslTruststore);
				if (myParameters.sslTruststorePass != null)
					props.setProperty("database.ssl.truststore.password", myParameters.sslTruststorePass);

				break;
			}
			case TYPE_ORACLE:
			{
		        props.setProperty("connector.class", "io.debezium.connector.oracle.OracleConnector");
				offsetfile = "pg_synchdb/oracle_" + myParameters.connectorName + "_offsets.dat";
				schemahistoryfile = "pg_synchdb/oracle_" + myParameters.connectorName + "_schemahistory.dat";
				signalfile = "pg_synchdb/oracle_" + myParameters.connectorName + "_signal.dat";

                props.setProperty("database.dbname", myParameters.database);
				if (myParameters.database.equals("null"))
                    logger.warn("database is null - skip setting database.include.list property");
                else
                {
                    props.setProperty("database.dbname", myParameters.database);
                }
				/* limit to this Oracle user's schema for now so we do not replicate tables from other schemas */
				props.setProperty("schema.include.list", myParameters.user);
				props.setProperty("lob.enabled", "true");
				props.setProperty("unavailable.value.placeholder", "__synchdb_unavailable_value");
				break;
			}
			case TYPE_SQLSERVER:
			{
		        props.setProperty("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector");
				offsetfile = "pg_synchdb/sqlserver_" + myParameters.connectorName + "_offsets.dat";
				schemahistoryfile = "pg_synchdb/sqlserver_" + myParameters.connectorName + "_schemahistory.dat";
				signalfile = "pg_synchdb/sqlserver_" + myParameters.connectorName + "_signal.dat";
				
				if (myParameters.database.equals("null"))
					logger.warn("database is null - skip setting database.include.list property");
				else
				{
					props.setProperty("database.include.list", myParameters.database);
					props.setProperty("database.names", myParameters.database);
				}

				if (myParameters.sslmode != null && !myParameters.sslmode.equals("disabled"))
					props.setProperty("database.encrypt", "true");
				else
					props.setProperty("database.encrypt", "false");

				if (myParameters.sslKeystore != null)
					props.setProperty("database.ssl.keystore", myParameters.sslKeystore);
				if (myParameters.sslKeystorePass != null)
					props.setProperty("database.ssl.keystore.password", myParameters.sslKeystorePass);
				if (myParameters.sslTruststore != null)
					props.setProperty("database.ssl.truststore", myParameters.sslTruststore);
				if (myParameters.sslTruststorePass != null)
					props.setProperty("database.ssl.truststore.password", myParameters.sslTruststorePass);
				break;
			}
		}
		
		/* setting common properties */
		props.setProperty("poll.interval.ms", "500");
		if (myParameters.table.equals("null"))
			logger.warn("table is null - skip setting table.include.list property");
		else if (myParameters.table.startsWith("file:"))
		{
			logger.warn("reading table list from file...");
			String filepath = myParameters.table.substring(5);
			File tablefile = new File(filepath);
			if (tablefile.exists())
			{
				ObjectMapper objmapper = new ObjectMapper();
				try
				{
					JsonNode js = objmapper.readTree(tablefile);
					if (js.has("table_list") && js.get("table_list").isArray())
					{
						JsonNode tableListNode = js.get("table_list");
						StringBuilder tablelist = new StringBuilder();
						for (JsonNode table : tableListNode)
						{
							tablelist.append(table.asText()).append(",");
						}
						if (tablelist.length() > 0)
							tablelist.setLength(tablelist.length() - 1);

						logger.warn("tables to capture: " + tablelist.toString());
						props.setProperty("table.include.list", tablelist.toString());
					}
					else
					{
						logger.warn("file has no array named 'table_list' - skip setting table.include.list property");
					}
				}
				catch (IOException e)
				{
					logger.warn("table file fails to parse - skip setting table.include.list property");
				}
			}
			else
			{
				logger.warn("table file does not exist - skip setting table.include.list property");
			}
		}
		else
			props.setProperty("table.include.list", myParameters.table);
		
		if (myParameters.snapshotMode.equals("null"))
			logger.warn("snapshot_mode is null - skip setting snapshot.mode property");
		else
			props.setProperty("snapshot.mode", myParameters.snapshotMode);

		if (myParameters.snapshotFetchSize == 0)
			logger.warn("snapshotFetchSize is 0 - skip setting snapshot.fetch.size property");
		else
			props.setProperty("snapshot.fetch.size", String.valueOf(myParameters.snapshotFetchSize));

		props.setProperty("database.hostname", myParameters.hostname);
		props.setProperty("database.port", String.valueOf(myParameters.port));
		props.setProperty("database.user", myParameters.user);
		props.setProperty("database.password", myParameters.password);
		props.setProperty("topic.prefix", "synchdb-connector");
		props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
		props.setProperty("schema.history.internal.file.filename", schemahistoryfile);
		props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
		props.setProperty("offset.storage.file.filename", offsetfile);
		props.setProperty("offset.flush.interval.ms", String.valueOf(myParameters.offsetFlushIntervalMs));
		props.setProperty("schema.history.internal.store.only.captured.tables.ddl", myParameters.captureOnlySelectedTableDDL ? "true" : "false");
		props.setProperty("max.batch.size", String.valueOf(myParameters.batchSize));
		props.setProperty("max.queue.size", String.valueOf(myParameters.queueSize));
		props.setProperty("record.processing.order", "ORDERED");
		props.setProperty("skipped.operations", myParameters.skippedOperations);
		props.setProperty("connect.timeout", String.valueOf(myParameters.connectTimeout));
		props.setProperty("database.query.timeout", String.valueOf(myParameters.queryTimeout));
		props.setProperty("snapshot.max.threads", String.valueOf(myParameters.snapshotThreadNum));
		props.setProperty("signal.enabled.channels", "file");
		props.setProperty("signal.file", signalfile);
		//props.setProperty("signal.data.collection", "synchdb.dbzsignal");	/* todo: make it configurable */
		props.setProperty("incremental.snapshot.chunk.size", String.valueOf(myParameters.incrementalSnapshotChunkSize));
		props.setProperty("incremental.snapshot.watermarking.strategy", myParameters.incrementalSnapshotWatermarkingStrategy);
		props.setProperty("incremental.snapshot.allow.schema.changes", "false");
		props.setProperty("min.row.count.to.stream.results", String.valueOf(myParameters.snapshotMinRowToStreamResults));

		//props.setProperty("read.only", "true");
		//props.setProperty("snapshot.include.collection.list", "");

		logger.info("Hello from DebeziumRunner class!");

		DebeziumEngine.CompletionCallback completionCallback = (success, message, error) ->
		{
			lastDbzMessage = message.replace("\n", " ").replace("\r", " ");
			lastDbzSuccess = success;
			lastDbzError = error;
		};
		
		engine = DebeziumEngine.create(Json.class)
		//engine = DebeziumEngine.create(KeyValueHeaderChangeEventFormat.of(Json.class, Json.class, Json.class),
        //        "io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory")
                .using(props)
				.using(completionCallback)
				.notifying((records, committer) -> {
					synchronized (this)
					{
						if (batchManager == null)
						{
							batchManager = new BatchManager();
						}
						if (activeBatchHash == null)
						{
							activeBatchHash = new HashMap<>();
						}

						try
						{
							batchManager.addBatch(new ChangeRecordBatch(records, committer));
						}
						catch (InterruptedException e)
						{
							Thread.currentThread().interrupt();
							logger.error("Interrupted while adding batch", e);
						}
					}
				})
				.build();

		if (myParameters.snapshotThreadNum > 1)
			executor = Executors.newFixedThreadPool(myParameters.snapshotThreadNum);
		else
			executor = Executors.newSingleThreadExecutor();

		logger.info("submit future to executor");
		future = executor.submit(() ->
		{
			try
			{
				engine.run();
			}
			catch (Exception e)
			{
				logger.error("Task failed with exception: " + e.getMessage());
				throw e;
			}
		});
    }

	public void stopEngine() throws Exception
	{
		/* wake up any waiting threads about shutdown */
		logger.warn("stopping Debezium engine...");
		if (batchManager != null)
		{
			batchManager.shutdown();
			batchManager = null;
		}
		if (engine != null)
		{
			logger.warn("closing Debezium engine...");
			engine.close();
			engine = null; // clear the reference to ensure it's properly garbage collected
		}
		if (executor != null)
		{
			logger.warn("shutting down executor...");
            executor.shutdown();
            try
			{
                if (!executor.awaitTermination(5, TimeUnit.SECONDS))
				{
                    executor.shutdownNow();
                }
            } 
			catch (InterruptedException e) 
			{
                executor.shutdownNow();
            }
        }
		logger.warn("done...");
	}

	public List<String> getChangeEvents()
	{
		List<String> listCopy = null;
		if (activeBatchHash == null)
		{
			activeBatchHash = new HashMap<>();
		}
		if (batchManager == null)
		{
			batchManager = new BatchManager();
		}

		//checkMemoryStatus();
        if (!future.isDone())
		{
			int i = 0;
			ChangeRecordBatch myNextBatch;
			myNextBatch = batchManager.getNextBatch();
			if (myNextBatch != null)
			{
				listCopy = new ArrayList<>(myNextBatch.records.size() + 1);
				logger.info("Debezium -> Synchdb: sent batchid(" + myNextBatch.batchid + ") with size(" + myNextBatch.records.size() + ")");
				/* first element: batch id */
				listCopy.add("B-" + String.valueOf(myNextBatch.batchid));

				/* remaining elements: individual changes*/
				for (i = 0; i < myNextBatch.records.size(); i++)
				{
					listCopy.add(myNextBatch.records.get(i).value());
				}

				/* save this batch in active batch hash struct */
				activeBatchHash.put(myNextBatch.batchid, myNextBatch);
			}
		}
		else
		{
			/* conector task is not running, get exit messages */
			logger.warn("connector is no longer running");
			logger.info("success flag = " + lastDbzSuccess + " | message = " + lastDbzMessage + " | error = " + lastDbzError);

			/*
			 * add the last captured connector exit message and send it to synchdb
			 * the K- prefix indicated an error rather than a change event
			 */
			listCopy = new ArrayList<>();
			listCopy.add("K-" + lastDbzSuccess + ";" + lastDbzMessage);
		}
		return listCopy;
    }

	/* 
	 * method to mark a batch as done. This would cause dbz engine to commit the offset.
	 * if markbatchdone = true, the entire batch task is marked as completed.
	 * if markbatchdon = false, it indicates a partial completion, then we will only mark
	 * task from 'markfrom' to 'markto' as completed within the batch
	 *
	 */
	public void markBatchComplete(int batchid, boolean markall, int markfrom, int markto) throws InterruptedException
    {
		int i = 0;
		ChangeRecordBatch myBatch;

		if (activeBatchHash == null)
		{
			activeBatchHash = new HashMap<>();
			return;
		}
		
		logger.info("Debezium receivd batchid(" + batchid + ") completion request");		
		myBatch = activeBatchHash.get(batchid);
		if (myBatch == null)
		{
			logger.error("batch id " + batchid + " is not found in active batch hash");
			return;
		}
		
		if (markall)
		{
			logger.info("debezium marked all records in batchid(" + batchid + ") as processed");

			/*
			 * mark only the last change event in batch as done. This has the same effect as
			 * marking the entire batch as done that does not require individually mark each
			 * change event as done, which takes a longer time
			 */
			myBatch.committer.markProcessed(myBatch.records.get(myBatch.records.size()-1));
			/* mark this batch complete to allow debezium to commit and flush offset */
			myBatch.committer.markBatchFinished();
			
			/* remove hash entry at batch completion */
			activeBatchHash.remove(batchid);

			/* nullify the allocated objects for garbage collection */
			myBatch.records.clear();
			myBatch.records = null;
			myBatch.committer = null;
			myBatch = null;
		}
		else
		{
			/* sanity check on the given range */
			if (markfrom >= myBatch.records.size() ||
				markto >= myBatch.records.size() ||
				markfrom < 0 || markto < 0)
			{
				logger.error("invalid range to mark completion: markfrom = " + markfrom + 
						" markto = " + markto + " sizeof batch = " + myBatch.records.size());
				return;
			}

			/* mark only the tasks within given range */
			for (i = markfrom; i <= markto; i++)
			{
				myBatch.committer.markProcessed(myBatch.records.get(i));
				logger.info("marked record(" + i + ") as processed within batchid " + batchid);
			}

			logger.warn("debezium marked " + (markto - markfrom + 1) + " records in batchid(" + batchid + ") as processed");
			/* we assumes that the only case to mark a batch as partially done is during error
			 * encounter and that we will exit soon after this function call. So let's mark this
			 * batch as finished and remove it from active batch hash
			 */
			myBatch.committer.markBatchFinished();
			activeBatchHash.remove(batchid);
		}
		System.gc();
	}
	
	public String getConnectorOffset(int connectorType, String db, String name)
	{
		File inputFile = null;
		Map<ByteBuffer, ByteBuffer> originalData = null;
		String ret = "NULL";
		String key = null;

		switch (connectorType)
		{
			case TYPE_MYSQL:
			{
				inputFile = new File("pg_synchdb/mysql_" + name + "_offsets.dat");
				key = "[\"engine\",{\"server\":\"synchdb-connector\"}]";
				break;
			}
			case TYPE_ORACLE:
			{
				inputFile = new File("pg_synchdb/oracle_" + name + "_offsets.dat");
				key = "[\"engine\",{\"server\":\"synchdb-connector\"}]";
				break;
			}
			case TYPE_SQLSERVER:
			{
				inputFile = new File("pg_synchdb/sqlserver_" + name + "_offsets.dat");
				key = "[\"engine\",{\"server\":\"synchdb-connector\",\"database\":\"" + db + "\"}]";
				break;
			}
		}

		if (!inputFile.exists())
        {
            logger.info("dbz offset file does not exist yet. Skipping");
			ret = "offset file not flushed yet";
            return ret;
        }

		ByteBuffer keyBuffer = ByteBuffer.wrap(key.getBytes(StandardCharsets.US_ASCII));
		originalData = readOffsetFile(inputFile);
		for (Map.Entry<ByteBuffer, ByteBuffer> entry : originalData.entrySet())
		{
			if (entry.getKey().equals(keyBuffer))
			{
				ret = StandardCharsets.UTF_8.decode(entry.getValue()).toString();
				logger.info("offset = " + ret);
			}
			else
			{
				//logger.warn("key entry not found");
			}
		}
		return ret;
	}

	public void setConnectorOffset(String filename, int type, String db, String newvalue)
	{
		File inputFile = new File(filename);
		String key = null;
		String value = newvalue;
		Map<byte[], byte[]> rawData = new HashMap<>();
		Map<ByteBuffer, ByteBuffer> originalData = null;

		
		switch (type)
		{
			case TYPE_MYSQL:
			{
				key = "[\"engine\",{\"server\":\"synchdb-connector\"}]";
				break;
			}
			case TYPE_ORACLE:
			{
				key = "[\"engine\",{\"server\":\"synchdb-connector\"}]";
				break;
			}
			case TYPE_SQLSERVER:
			{
				key = "[\"engine\",{\"server\":\"synchdb-connector\",\"database\":\"" + db + "\"}]";
				break;
			}
		}

		if (!inputFile.exists())
		{
			logger.warn("dbz offset file does not exist yet. Skip setting offset");
			return;
		}

		originalData = readOffsetFile(inputFile);	
		ByteBuffer keyBuffer = ByteBuffer.wrap(key.getBytes(StandardCharsets.US_ASCII));
		ByteBuffer valueBuffer = ByteBuffer.wrap(value.getBytes(StandardCharsets.US_ASCII));
		for (Map.Entry<ByteBuffer, ByteBuffer> entry : originalData.entrySet())
		{
			if (entry.getKey().equals(keyBuffer))
			{
				logger.warn("updating existing offset record: " + key + " with new value: " + value);
				rawData.put(entry.getKey().array(), valueBuffer.array());
			}
			else
			{
				logger.warn("inserting new offset record: " + key + " with new value: " + value);
				rawData.put(keyBuffer.array(), valueBuffer.array());
			}
		}
		writeOffsetFile(inputFile, rawData);
	}
	
	public Map<ByteBuffer, ByteBuffer> readOffsetFile(File inputFile)
	{
        Map<ByteBuffer, ByteBuffer> originalData = new HashMap<>();

        try (FileInputStream fileIn = new FileInputStream(inputFile);
             ObjectInputStream objectIn = new ObjectInputStream(fileIn)) {
            Object obj = objectIn.readObject();
            if (!(obj instanceof HashMap)) {
                throw new IllegalStateException("Expected HashMap but found " + obj.getClass());
            }
            Map<byte[], byte[]> raw = (Map<byte[], byte[]>) obj;
            for (Map.Entry<byte[], byte[]> mapEntry : raw.entrySet()) {
                ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey()) : null;
                ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue()) : null;
                originalData.put(key, value);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return originalData;
    }

    public void writeOffsetFile(File outputFile, Map<byte[], byte[]> rawData)
	{
        try (FileOutputStream fileOut = new FileOutputStream(outputFile);
             ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)) {
                objectOut.writeObject(rawData);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

	public void jvmMemDump()
	{
		checkMemoryStatus();
	}
	public static void main(String[] args)
	{
		/* testing code can be put here */
    }
}
