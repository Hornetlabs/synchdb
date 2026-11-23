package com.example;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.buffered.infinispan.EmbeddedInfinispanCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.infinispan.InfinispanTransaction;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.RedoSqlDmlEvent;
import io.debezium.relational.TableId;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.TestCase;

public class InfinispanBuffersTest extends TestCase
{
    private Properties properties(Path directory)
    {
        Properties properties = new Properties();
        properties.setProperty("topic.prefix", "fixture");
        properties.setProperty("database.hostname", "localhost");
        properties.setProperty("database.user", "fixture");
        properties.setProperty("database.password", "unused");
        properties.setProperty("database.dbname", "ORCLCDB");
        InfinispanBuffers.configure(properties, directory, "heap", 1);
        return properties;
    }

    private EmbeddedInfinispanCacheProvider open(Properties properties)
    {
        return new EmbeddedInfinispanCacheProvider(new OracleConnectorConfig(Configuration.from(properties)));
    }

    public void testConfigurationKeepsLegacyNamesAndRetainsCachesOnStop()
    {
        Properties properties = properties(Path.of("pg_synchdb/new-generation_ispn"));
        assertEquals("false", properties.getProperty("log.mining.buffer.drop.on.stop"));
        assertTrue(properties.getProperty("log.mining.buffer.infinispan.cache.processed_transactions")
            .contains("name=\"processed-transactions\""));
        assertTrue(properties.getProperty("log.mining.buffer.infinispan.cache.processed_transactions")
            .contains("new-generation_ispn/processed/data"));
        assertTrue(properties.getProperty("log.mining.buffer.infinispan.cache.schema_changes")
            .contains("new-generation_ispn/schema/index"));
        assertNotNull(properties.getProperty("log.mining.buffer.infinispan.cache.rollbacks"));
    }

    public void testOffHeapOptionRetainsTheOriginalInterpretation()
    {
        Properties properties = new Properties();
        InfinispanBuffers.configure(properties, Path.of("cache"), "off heap", 1);
        assertTrue(properties.getProperty("log.mining.buffer.infinispan.cache.events").contains("OFF_HEAP"));
        InfinispanBuffers.configure(properties, Path.of("cache"), "OFF HEAP", 1);
        assertTrue(properties.getProperty("log.mining.buffer.infinispan.cache.events").contains("HEAP"));
        assertFalse(properties.getProperty("log.mining.buffer.infinispan.cache.events").contains("OFF_HEAP"));
    }

    public void testXmlPathsEscapeSpecialCharacters()
    {
        Properties properties = properties(Path.of("cache&a\"b"));
        assertTrue(properties.getProperty("log.mining.buffer.infinispan.cache.events").contains("cache&amp;a&quot;b"));
    }

    public void testInvalidMemorySizeIsRejected()
    {
        try
        {
            InfinispanBuffers.configure(new Properties(), Path.of("cache"), "heap", 0);
            fail("Expected invalid memory size");
        }
        catch (IllegalArgumentException expected) {}
    }

    public void testOldProviderHasNoDefaultRollbackConfiguration()
    {
        Properties properties = properties(Path.of("cache"));
        properties.remove("log.mining.buffer.infinispan.cache.rollbacks");
        OracleConnectorConfig config = new OracleConnectorConfig(Configuration.from(properties));
        assertNull(config.getConfig().getString(OracleConnectorConfig.LOG_MINING_BUFFER_INFINISPAN_CACHE_ROLLBACKS));
    }

    public void testLegacyFourCacheConfigurationFailsInTheRealProvider() throws Exception
    {
        Path work = Files.createTempDirectory("synchdb-infinispan-old-config-");
        try
        {
            Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), LegacyProbe.class.getName(), work.toString())
                .redirectErrorStream(true).start();
            assertTrue("Legacy provider probe must terminate", process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS));
            String output = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            assertEquals(output, 0, process.exitValue());
            assertTrue(output, output.contains("LEGACY_PROVIDER_FAILURE=java.lang.NullPointerException"));
            assertTrue(output, output.contains("EmbeddedInfinispanCacheProvider.createCache"));
        }
        finally
        {
            try (var paths = Files.walk(work))
            {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                    try { Files.delete(path); }
                    catch (java.io.IOException error) { throw new RuntimeException(error); }
                });
            }
        }
    }

    public static final class LegacyProbe
    {
        public static void main(String[] arguments) throws Exception
        {
            Properties properties = new InfinispanBuffersTest().properties(Path.of(arguments[0]));
            properties.remove("log.mining.buffer.infinispan.cache.rollbacks");
            // A failed constructor owns a partially started cache manager.
            // Isolate it in this child JVM, exactly as a failed old worker was.
            try (EmbeddedInfinispanCacheProvider ignored = new InfinispanBuffersTest().open(properties))
            {
                System.out.println("Legacy provider unexpectedly accepted the incomplete configuration");
                System.exit(1);
            }
            catch (NullPointerException expected)
            {
                System.out.println("LEGACY_PROVIDER_FAILURE=" + expected.getClass().getName());
                expected.printStackTrace(System.out);
                System.exit(0);
            }
        }
    }

    public void testClosedDiskStoresReopenAfterColdDirectoryCopy() throws Exception
    {
        Path work = Files.createTempDirectory("synchdb-infinispan-test-");
        Path old = work.resolve("ispn_legacy_target");
        Path migrated = work.resolve("database-connector-generation_ispn");
        String xid = "0001.002.00000003";
        Instant timestamp = Instant.ofEpochMilli(1700000000123L);
        String firstRow = "AAASo8AAEAAAAF2AAA";
        String secondRow = "AAASo8AAEAAAAF2AAB";
        try
        {
            try (EmbeddedInfinispanCacheProvider provider = open(properties(old)))
            {
                LogMinerTransactionCache<InfinispanTransaction> transactions = provider.getTransactionCache();
                InfinispanTransaction transaction = new InfinispanTransaction(xid, Scn.valueOf(100), timestamp, "FIXTURE", 1, "client");
                transactions.addTransaction(transaction);
                for (String row : new String[]{firstRow, secondRow})
                {
                    RedoSqlDmlEvent event = new RedoSqlDmlEvent(EventType.INSERT, Scn.valueOf(101),
                        new TableId("ORCLCDB", "DBZUSER", "ITEMS"), row, "0x0000.00000000.0000",
                        timestamp, new Object[0], new Object[]{"payload"}, "INSERT INTO ITEMS VALUES ('payload')");
                    transactions.addTransactionEvent(transaction, transaction.getNextEventId(), event);
                }
                transactions.syncTransaction(transaction);
                assertTrue(transactions.rollbackTransactionEventWithRowId(transaction, secondRow));
                provider.getProcessedTransactionsCache().put("already-committed", "99");
                provider.getSchemaChangesCache().put("schema-event", "98");
            }
            for (String store : InfinispanBuffers.LEGACY_STORES)
            {
                assertTrue(store + " data directory", Files.isDirectory(old.resolve(store).resolve("data")));
                assertTrue(store + " index directory", Files.isDirectory(old.resolve(store).resolve("index")));
            }
            List<Path> original = new ArrayList<>();
            try (var paths = Files.walk(old)) { paths.forEach(original::add); }
            for (Path path : original)
            {
                Path target = migrated.resolve(old.relativize(path));
                if (Files.isDirectory(path)) Files.createDirectories(target);
                else Files.copy(path, target, StandardCopyOption.COPY_ATTRIBUTES);
            }
            try (EmbeddedInfinispanCacheProvider provider = open(properties(migrated)))
            {
                LogMinerTransactionCache<InfinispanTransaction> transactions = provider.getTransactionCache();
                assertTrue(transactions.containsTransaction(xid));
                InfinispanTransaction transaction = transactions.getTransaction(xid);
                assertEquals(Scn.valueOf(100), transaction.getStartScn());
                assertEquals(2, transaction.getNumberOfEvents());
                RedoSqlDmlEvent event = (RedoSqlDmlEvent) transactions.getTransactionEvent(transaction, 0);
                assertEquals("payload", event.getNewValues()[0]);
                AtomicInteger applied = new AtomicInteger();
                AtomicInteger rolledBack = new AtomicInteger();
                transactions.forEachEvent(transaction, (change, rollback) -> {
                    if (rollback) rolledBack.incrementAndGet();
                    else applied.incrementAndGet();
                    return true;
                });
                assertEquals("Rolled-back row must remain excluded after restart", 1, applied.get());
                assertEquals("Rollback marker must remain persisted after restart", 1, rolledBack.get());
                assertEquals("99", provider.getProcessedTransactionsCache().get("already-committed"));
                assertEquals("98", provider.getSchemaChangesCache().get("schema-event"));
                transactions.removeTransactionEvents(transaction);
                transactions.removeTransaction(transaction);
            }
            try (EmbeddedInfinispanCacheProvider provider = open(properties(migrated)))
            {
                assertFalse(provider.getTransactionCache().containsTransaction(xid));
                assertEquals("99", provider.getProcessedTransactionsCache().get("already-committed"));
            }
        }
        finally
        {
            try (var paths = Files.walk(work))
            {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                    try { Files.delete(path); }
                    catch (java.io.IOException error) { throw new RuntimeException(error); }
                });
            }
        }
    }
}
