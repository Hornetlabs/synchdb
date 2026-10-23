package com.example;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.ChangeEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DebeziumRunner {
	private static Logger logger = LogManager.getRootLogger();
	/* Each start owns its callbacks, bridge, engine and executor until closure. */
	private volatile EngineSession session;
	private final AtomicLong nextBatchId = new AtomicLong();
	private volatile String activeArtifactBase;
	private volatile OlrSnapshotBoundary olrSnapshotBoundary;

	public String getOlrSnapshotBoundary(String ignored)
	{
		if (olrSnapshotBoundary == null)
			throw new IllegalStateException("No OLR snapshot boundary is owned by this runner");
		return olrSnapshotBoundary.completed();
	}

	public String prepareOlrHandoff(String request) throws Exception
	{
		return OlrHandoff.prepare(request);
	}

	private int allocateBatchId()
	{
		long id = nextBatchId.getAndUpdate(value -> value <= Integer.MAX_VALUE ? value + 1 : value);
		if (id > Integer.MAX_VALUE)
			throw new IllegalStateException("Debezium batch identifiers are exhausted");
		return (int) id;
	}

	final int TYPE_MYSQL = 1;
	final int TYPE_ORACLE = 2;
	final int TYPE_SQLSERVER = 3;
	final int TYPE_OPENLOG_REPLICATOR = 4;
	final int TYPE_POSTGRES = 5;


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
		private String snapshottable;
		private String dstdb;
		private String olrHost;
		private int olrPort;
		private String olrSource;
		private String ispnCacheType;
		private String ispnMemoryType;
		private int ispnMemorySize;
		private String logminerStreamMode;
		private int cdcDelay;
		private String srcschema;
		private String artifactBase;
		private boolean artifactResume;
		private boolean artifactResumeConfigured;
		private String postgresLegacySlot;
		private String postgresLegacyPublication;

		/* constructor requires all required parameters for a connector to work */
		public MyParameters(String connectorName, int connectorType, String hostname, int port, String user, String password, String database, String table, String snapshottable,String snapshotMode, String dstdb, String srcschema)
		{
			this.connectorName = connectorName;
			this.connectorType = connectorType;
			this.hostname = hostname;
			this.port = port;
			this.user = user;
			this.password = password;
			this.database = database;
			this.table = table;
			this.snapshottable = snapshottable;
			this.snapshotMode = snapshotMode;
			this.dstdb = dstdb;
			this.srcschema = srcschema;
		}
		public MyParameters setArtifactBase(String artifactBase)
		{
			if (artifactBase == null || !artifactBase.matches(
				"pg_synchdb/[1-9][0-9]*_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"))
				throw new IllegalArgumentException("Artifact identity must be an ASCII database/connector/generation path");
			this.artifactBase = artifactBase;
			return this;
		}
		public MyParameters setArtifactResume(boolean resumeExisting)
		{
			this.artifactResume = resumeExisting;
			this.artifactResumeConfigured = true;
			return this;
		}
		public MyParameters setPostgresLegacyIdentity(String slot, String publication)
		{
			if (slot == null || publication == null)
				throw new IllegalArgumentException("Legacy PostgreSQL source names are required together");
			this.postgresLegacySlot = slot;
			this.postgresLegacyPublication = publication;
			return this;
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
		public MyParameters setOlr(String olrHost, int olrPort, String olrSource)
		{
			this.olrHost = olrHost;
			this.olrPort = olrPort;
			this.olrSource = olrSource;
			return this;
		}
		public MyParameters setIspn(String ispnCacheType, String ispnMemoryType, int ispnMemorySize)
		{
			this.ispnCacheType = ispnCacheType;
			this.ispnMemoryType = ispnMemoryType;
			this.ispnMemorySize = ispnMemorySize;
			return this;
		}
		public MyParameters setLogminerStreamMode(String logminerStreamMode)
		{
			this.logminerStreamMode = logminerStreamMode;
			return this;
		}
		public MyParameters setCdcDelay(int cdcDelay)
        {
            this.cdcDelay = cdcDelay;
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
			logger.warn("dstdb = " + this.dstdb);
			logger.warn("srcschema = " + this.srcschema);

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
			logger.warn("sslTruststore = " + this.sslTruststore);
			logger.warn("logLevel = " + this.logLevel);
			logger.warn("snapshottable = " + this.snapshottable);
			logger.warn("logminerStreamMode = " + this.logminerStreamMode);
			logger.warn("cdcDelay = " + this.cdcDelay);

			logger.warn("olrHost = " + this.olrHost);
			logger.warn("olrPort = " + this.olrPort);
			logger.warn("olrSource = " + this.olrSource);

			logger.warn("ispnCacheType = " + this.ispnCacheType);
			logger.warn("ispnMemoryType = " + this.ispnMemoryType);
			logger.warn("ispnMemorySize = " + this.ispnMemorySize);
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

	private static String resolveTableList(String setting, String listName) throws IOException
	{
		if (setting == null || setting.isBlank())
			throw new IllegalArgumentException(listName + " must not be empty");
		if (!setting.startsWith("file:"))
			return setting;

		File tableFile = new File(setting.substring(5));
		if (!tableFile.isFile() || !tableFile.canRead())
			throw new IOException("Cannot read table filter file: " + tableFile);

		ObjectMapper mapper = new ObjectMapper();
		mapper.enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
		JsonNode root = mapper.readTree(tableFile);
		JsonNode tables = root == null ? null : root.get(listName);
		if (tables == null || !tables.isArray() || tables.isEmpty())
			throw new IllegalArgumentException("Table filter file must contain a non-empty array named '" + listName + "'");

		List<String> tableNames = new ArrayList<>();
		for (JsonNode table : tables)
		{
			if (!table.isTextual() || table.textValue().isBlank())
				throw new IllegalArgumentException("Every entry in '" + listName + "' must be a non-empty string");
			tableNames.add(table.textValue());
		}
		return String.join(",", tableNames);
	}

	public synchronized void startEngine(MyParameters myParameters) throws Exception
	{
		if (session != null && !session.restartable())
			throw new IllegalStateException("Previous Debezium session has not closed successfully");
		if (myParameters.artifactBase == null || !myParameters.artifactResumeConfigured)
			throw new IllegalStateException("A committed engine artifact generation and explicit resume state are required");
		if (myParameters.postgresLegacySlot != null && !myParameters.artifactResume)
			throw new IllegalStateException("Legacy PostgreSQL identity requires an existing engine offset");
		activeArtifactBase = myParameters.artifactBase;
		OffsetStore.requireResumePartition(myParameters.connectorType == TYPE_OPENLOG_REPLICATOR ? TYPE_ORACLE : myParameters.connectorType,
			myParameters.database, java.nio.file.Path.of(activeArtifactBase + "_offsets.dat"));
		ArtifactFiles.validateHistory(myParameters.connectorType, java.nio.file.Path.of(activeArtifactBase + "_schemahistory.dat"),
			myParameters.artifactResume);

		final BatchBridge batches = new BatchBridge(this::allocateBatchId);
		final EngineSession next = new EngineSession(batches);
		session = next;
		try
		{
			String offsetfile = activeArtifactBase + "_offsets.dat";
			String schemahistoryfile = activeArtifactBase + "_schemahistory.dat";
			String signalfile = activeArtifactBase + "_signal.dat";
			String ispn_metadata_dir = activeArtifactBase + "_ispn";

			Properties props = new Properties();

			/*
			 * Initialize Logging: console appender and pattern are declared in
			 * src/main/resources/log4j2.xml; only the level is set at runtime
			 */
			switch (myParameters.logLevel)
			{
				case LOG_LEVEL_ALL:
				{
					Configurator.setRootLevel(Level.ALL);
					break;
				}
				case LOG_LEVEL_DEBUG:
				{
					Configurator.setRootLevel(Level.DEBUG);
					break;
				}
				case LOG_LEVEL_INFO:
				{
					Configurator.setRootLevel(Level.INFO);
					break;
				}
				case LOG_LEVEL_ERROR:
				{
					Configurator.setRootLevel(Level.ERROR);
					break;
				}
				case LOG_LEVEL_FATAL:
				{
					Configurator.setRootLevel(Level.FATAL);
					break;
				}
				case LOG_LEVEL_OFF:
				{
					Configurator.setRootLevel(Level.OFF);
					break;
				}
				case LOG_LEVEL_TRACE:
				{
					Configurator.setRootLevel(Level.TRACE);
					break;
				}
				default:
				case LOG_LEVEL_UNDEF:
				case LOG_LEVEL_WARN:
				{
					Configurator.setRootLevel(Level.WARN);
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
					int hash = (myParameters.connectorName + myParameters.dstdb).hashCode();
					long unsignedhash = Integer.toUnsignedLong(hash);
					long serverid = 1 + (unsignedhash % (4294967295L - 1));

					logger.warn("derived server id " + serverid);
					props.setProperty("database.server.id", String.valueOf(serverid));	/* todo: make configurable */


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

	                props.setProperty("database.dbname", myParameters.database);
					if (myParameters.database.equals("null"))
	                    logger.warn("database is null - skip setting database.include.list property");
	                else
	                {
	                    /* srcdb may be in "CDB/PDB" format; split if a slash is present */
	                    int slashIdx = myParameters.database.indexOf('/');
	                    if(slashIdx > 0){
	                        String cdb = myParameters.database.substring(0, slashIdx);
	                        String pdb = myParameters.database.substring(slashIdx + 1);
	                        props.setProperty("database.dbname", cdb);
	                        props.setProperty("database.pdb.name", pdb);
	                        logger.info("Oracle CDB/PDB mode: dbname=" + cdb + " pdb.name=" + pdb);
	                    }else{
	                        props.setProperty("database.dbname", myParameters.database);
	                    }
	                }
					/* limit to this Oracle user's schema for now so we do not replicate tables from other schemas */
					props.setProperty("schema.include.list", myParameters.srcschema);
					props.setProperty("lob.enabled", "true");
					props.setProperty("unavailable.value.placeholder", "__synchdb_unavailable_value");

					/* openlog replicator - via debezium */
					if (myParameters.olrHost != null && myParameters.olrSource != null && myParameters.olrPort > 0)
					{
						props.setProperty("openlogreplicator.source", myParameters.olrSource);
						props.setProperty("openlogreplicator.host", myParameters.olrHost);
						props.setProperty("openlogreplicator.port", String.valueOf(myParameters.olrPort));
						props.setProperty("database.connection.adapter", "olr");
					}
			else
			logger.warn("olrHost is null - skip setting Openlog Replicator property");
					if (myParameters.logminerStreamMode != null)
					{
						props.setProperty("database.connection.adapter", myParameters.logminerStreamMode);
					}
					else
						logger.warn("logminerStreamMode is null - using the default mode");

					if (myParameters.ispnCacheType != null &&
						myParameters.ispnMemoryType != null &&
						myParameters.ispnMemorySize > 0)
					{
						String memtype = "HEAP";

						if (myParameters.ispnCacheType.equals("embedded"))
							props.setProperty("log.mining.buffer.type", "infinispan_embedded");
						else
							/* todo: we always default to embedded as that is the mode we support now */
							props.setProperty("log.mining.buffer.type", "infinispan_embedded");

						if (myParameters.ispnMemoryType.equals("off heap"))
							memtype = "OFF_HEAP";
						else if (myParameters.ispnMemoryType.equals("heap"))
							memtype = "HEAP";
						else
							memtype = "HEAP";

						logger.warn("using infinispan with memtype " + memtype);
						props.setProperty(
						"log.mining.buffer.infinispan.cache.global",
						  "<infinispan xmlns=\"urn:infinispan:config:14.0\">" +
						    "<threads>" +
						      "<blocking-bounded-queue-thread-pool name=\"myexec\" max-threads=\"10\" keepalive-time=\"10000\" queue-length=\"5000\"/>" +
						    "</threads>" +
						  "</infinispan>"
						);

						/* EVENT cache */
						props.setProperty(
						  "log.mining.buffer.infinispan.cache.events",
						  "<local-cache name=\"events\">" +
						    "<encoding>" +
						      "<key media-type=\"application/x-protostream\"/>" +
						  "<value media-type=\"application/x-protostream\"/>" +
						    "</encoding>" +
						    "<memory storage=\"" + memtype + "\" max-size=\"" + myParameters.ispnMemorySize + "MB\" when-full=\"REMOVE\"/>" +
						    "<persistence passivation=\"true\">" +
						      "<file-store read-only=\"false\" preload=\"false\" shared=\"false\">" +
						        "<data path=\"" + ispn_metadata_dir + "/events/data\"/>" +
						    "<index path=\"" + ispn_metadata_dir + "/events/index\"/>" +
						"<write-behind/>" +
						      "</file-store>" +
						    "</persistence>" +
						  "</local-cache>"
						);

						/* TRANSACTIONS cache */
						props.setProperty(
						  "log.mining.buffer.infinispan.cache.transactions",
						  "<local-cache name=\"transactions\">" +
						    "<encoding>" +
						      "<key media-type=\"application/x-protostream\"/>" +
						      "<value media-type=\"application/x-protostream\"/>" +
						    "</encoding>" +
						    "<memory storage=\"" + memtype + "\" max-size=\"" + myParameters.ispnMemorySize +"MB\" when-full=\"REMOVE\"/>" +
						"<persistence passivation=\"true\">" +
						      "<file-store read-only=\"false\" preload=\"false\" shared=\"false\">" +
						        "<data path=\"" + ispn_metadata_dir + "/transactions/data\"/>" +
						        "<index path=\"" + ispn_metadata_dir + "/transactions/index\"/>" +
						        "<write-behind/>" +
						      "</file-store>" +
						"</persistence>" +
						  "</local-cache>"
						);

						/* PROCESSED TRANSACTIONS cache */
						props.setProperty(
						  "log.mining.buffer.infinispan.cache.processed_transactions",
						  "<local-cache name=\"processed-transactions\">" +
						    "<encoding>" +
						      "<key media-type=\"application/x-protostream\"/>" +
						      "<value media-type=\"application/x-protostream\"/>" +
						    "</encoding>" +
						"<memory storage=\"" + memtype + "\" max-size=\"" + myParameters.ispnMemorySize +"MB\" when-full=\"REMOVE\"/>" +
						    "<persistence passivation=\"true\">" +
						      "<file-store read-only=\"false\" preload=\"false\" shared=\"false\">" +
						        "<data path=\"" + ispn_metadata_dir + "/processed/data\"/>" +
						        "<index path=\"" + ispn_metadata_dir + "/processed/index\"/>" +
						        "<write-behind/>" +
						  "</file-store>" +
						    "</persistence>" +
						  "</local-cache>"
						);

						/* SCHEMA CHANGES cache */
						props.setProperty(
						  "log.mining.buffer.infinispan.cache.schema_changes",
						  "<local-cache name=\"schema-changes\">" +
						    "<encoding>" +
						  "<key media-type=\"application/x-protostream\"/>" +
						      "<value media-type=\"application/x-protostream\"/>" +
						    "</encoding>" +
						    "<memory storage=\"" + memtype + "\" max-size=\"" + myParameters.ispnMemorySize + "MB\" when-full=\"REMOVE\"/>" +
						    "<persistence passivation=\"true\">" +
						      "<file-store read-only=\"false\" preload=\"false\" shared=\"false\">" +
						    "<data path=\"" + ispn_metadata_dir + "/schema/data\"/>" +
						        "<index path=\"" + ispn_metadata_dir + "/schema/index\"/>" +
						        "<write-behind/>" +
						      "</file-store>" +
						    "</persistence>" +
						  "</local-cache>"
						);
					}
					break;
				}
				case TYPE_OPENLOG_REPLICATOR:
	            {
					/* openlog replicator - native. Rely on debezium for initial snapshot only */
	                props.setProperty("connector.class", "io.debezium.connector.oracle.OracleConnector");

	                props.setProperty("database.dbname", myParameters.database);
	                if (myParameters.database.equals("null"))
	                    logger.warn("database is null - skip setting database.include.list property");
	                else
	                {
	                    /* srcdb may be in "CDB/PDB" format; split if a slash is present */
	                    int slashIdx = myParameters.database.indexOf('/');
	                    if(slashIdx > 0){
	                        String cdb = myParameters.database.substring(0, slashIdx);
	                        String pdb = myParameters.database.substring(slashIdx + 1);
	                        props.setProperty("database.dbname", cdb);
	                        props.setProperty("database.pdb.name", pdb);
	                        logger.info("Oracle CDB/PDB mode: dbname=" + cdb + " pdb.name=" + pdb);
	                    }else{
	                        props.setProperty("database.dbname", myParameters.database);
	                    }
	                }
	                /* limit to this Oracle user's schema for now so we do not replicate tables from other schemas */
	                props.setProperty("schema.include.list", myParameters.srcschema);
	                props.setProperty("lob.enabled", "true");
	                props.setProperty("unavailable.value.placeholder", "__synchdb_unavailable_value");
	                break;
	            }
				case TYPE_SQLSERVER:
				{
			        props.setProperty("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector");

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

					props.setProperty("schema.include.list", myParameters.srcschema);
					break;
				}
				case TYPE_POSTGRES:
				{
					props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");

					props.setProperty("tasks.max", "1");
					props.setProperty("plugin.name", "pgoutput");

					NativeSourcePreflight.configurePostgres(props, activeArtifactBase,
						myParameters.postgresLegacySlot, myParameters.postgresLegacyPublication);

					if (myParameters.table.equals("null"))
						props.setProperty("publication.autocreate.mode", "all_tables");
					else
						props.setProperty("publication.autocreate.mode", "filtered");
					props.setProperty("database.dbname", myParameters.database);
					props.setProperty("schema.include.list", myParameters.srcschema);


	                if (myParameters.database.equals("null"))
	                    logger.warn("database is null - skip setting database.include.list property");
	                else
	                {
	                    props.setProperty("database.dbname", myParameters.database);
	                }

				}
			}

			/* setting common properties */
			props.setProperty("poll.interval.ms", "500");
			if (myParameters.table.equals("null"))
				logger.warn("table is null - skip setting table.include.list property");
			else
				props.setProperty("table.include.list", resolveTableList(myParameters.table, "table_list"));

			if (myParameters.snapshotMode.equals("null"))
				logger.warn("snapshot_mode is null - skip setting snapshot.mode property");
			else
				props.setProperty("snapshot.mode", myParameters.snapshotMode);

			if (myParameters.snapshotFetchSize == 0)
				logger.warn("snapshotFetchSize is 0 - skip setting snapshot.fetch.size property");
			else
				props.setProperty("snapshot.fetch.size", String.valueOf(myParameters.snapshotFetchSize));

			if (myParameters.snapshottable.equals("null"))
				logger.warn("snapshottable is null - skip setting snapshot.include.collection.list property");
			else
				props.setProperty("snapshot.include.collection.list", resolveTableList(myParameters.snapshottable, "snapshot_table_list"));

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
			/* Custom batch consumers bypass record.processing.order in Debezium 3.5.2. */
			props.setProperty("tasks.max", "1");
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
			//props.setProperty("provide.transaction.metadata", "true");
			//props.setProperty("read.only", "true");
			if (myParameters.cdcDelay > 0)
				props.setProperty("streaming.delay.ms", String.valueOf(myParameters.cdcDelay));

			SourceConsistency.configure(props, myParameters.connectorType);
			NativeSourcePreflight.requireArtifactState(props, myParameters.artifactResume);
			if (myParameters.connectorType == TYPE_POSTGRES)
				NativeSourcePreflight.validatePostgres(props, myParameters.artifactResume);
			SnapshotCompletion snapshotCompletion = SnapshotCompletion.configure(props,activeArtifactBase,batches);
			final OlrSnapshotBoundary olrBoundary = myParameters.connectorType == TYPE_OPENLOG_REPLICATOR
				? new OlrSnapshotBoundary() : null;
			olrSnapshotBoundary = olrBoundary;
			DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
					.using(props)
					.using((success, message, error) -> next.completed(success, message, error))
					.using(next.connectorCallback())
					.notifying((records, committer) -> {
						if (olrBoundary != null)
							olrBoundary.observe(records);
						if (snapshotCompletion == null)
							batches.handleBatch(records,committer);
						else
							snapshotCompletion.handleBatch(records,committer);
					})
					.build();
			next.start(engine);
		}
		catch (Exception | Error failure)
		{
			next.startFailed(failure);
			throw failure;
		}
	}

	/* Protocol 3 also requires explicit generation-owned artifact paths.
	 * The batch data frame remains V1. */
	public int getProtocolVersion()
	{
		return 3;
	}

	public void requestStop()
	{
		EngineSession current = session;
		if (current != null)
			current.requestStop();
	}

	public int stopStatus()
	{
		EngineSession current = session;
		return current == null ? 1 : current.stopStatus();
	}

	/* Legacy entry point, asynchronous in protocol 2: also observe stopStatus(). */
	public void stopEngine()
	{
		requestStop();
	}

	private EngineSession activeSession()
	{
		EngineSession current = session;
		if (current == null)
			throw new IllegalStateException("Debezium engine has not been started");
		return current;
	}

	public ByteBuffer getChangeEvents()
	{
		return activeSession().poll();
	}

	/* Return only after the callback has accepted the full batch. Offset persistence
	 * remains subject to Debezium's flush policy. Partial acknowledgement is unsupported.
	 */
	public void markBatchComplete(int batchid, boolean markall, int markfrom, int markto) throws InterruptedException
	{
		activeSession().acknowledge(batchid, markall, markfrom, markto);
	}

	public String getConnectorOffset(int connectorType, String db, String name, String dstdb)
	{
		String base = activeArtifactBase;
		if (base == null)
			throw new IllegalStateException("No active engine artifact generation");
		return OffsetStore.getFile(connectorType == TYPE_OPENLOG_REPLICATOR ? TYPE_ORACLE : connectorType,
			db, java.nio.file.Path.of(base + "_offsets.dat"));
	}

	private void requireWritableOffsets()
	{
		EngineSession current = session;
		if (current != null && !current.offsetWritable())
			throw new IllegalStateException("Debezium offsets require a confirmed healthy engine shutdown");
	}

	public synchronized void setConnectorOffset(String filename, int type, String db, String newvalue)
	{
		requireWritableOffsets();
		OffsetStore.set(filename, type, db, newvalue);
	}

	public Map<ByteBuffer, ByteBuffer> readOffsetFile(File inputFile)
	{
		return OffsetStore.read(inputFile);
	}

	public synchronized void writeOffsetFile(File outputFile, Map<byte[], byte[]> rawData)
	{
		requireWritableOffsets();
		OffsetStore.write(outputFile, rawData);
	}

	public synchronized void createOffsetFile(String filename, int type, String db, String value)
	{
		requireWritableOffsets();
		if (activeArtifactBase != null && java.nio.file.Path.of(filename).toAbsolutePath().normalize().equals(
			java.nio.file.Path.of(activeArtifactBase + "_offsets.dat").toAbsolutePath().normalize()))
			throw new IllegalStateException("An engine-owned generation cannot accept a bootstrap seed");
		OffsetStore.create(filename, type, db, value);
	}

	public void jvmMemDump()
	{
		checkMemoryStatus();
	}

	public void changeLogLevel(int level)
	{
      switch (level)
      {
          case LOG_LEVEL_ALL:   Configurator.setRootLevel(Level.ALL);   break;
          case LOG_LEVEL_DEBUG: Configurator.setRootLevel(Level.DEBUG); break;
          case LOG_LEVEL_INFO:  Configurator.setRootLevel(Level.INFO);  break;
          case LOG_LEVEL_ERROR: Configurator.setRootLevel(Level.ERROR); break;
          case LOG_LEVEL_FATAL: Configurator.setRootLevel(Level.FATAL); break;
          case LOG_LEVEL_OFF:   Configurator.setRootLevel(Level.OFF);   break;
          case LOG_LEVEL_TRACE: Configurator.setRootLevel(Level.TRACE); break;
          default:
          case LOG_LEVEL_UNDEF:
          case LOG_LEVEL_WARN:  Configurator.setRootLevel(Level.WARN);  break;
      }
      logger.warn("DBZ log level changed to " + logger.getLevel());
	}

	public static void main(String[] args)
	{
		/* testing code can be put here */
    }
}
