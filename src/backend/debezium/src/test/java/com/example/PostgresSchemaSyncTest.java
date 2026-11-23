package com.example;

import java.util.Properties;
import junit.framework.TestCase;

public final class PostgresSchemaSyncTest extends TestCase
{
    private static Properties schemaSyncProperties()
    {
        Properties properties = new Properties();
        properties.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        properties.setProperty("snapshot.mode", "no_data");
        properties.setProperty("topic.prefix", "synchdb-connector");
        return properties;
    }

    public void testFiniteProducerSkipsAllHistoricalTables()
    {
        Properties properties = schemaSyncProperties();
        properties.setProperty("table.include.list", "public.orders");
        PostgresSchemaSync.configure(properties, true);
        assertEquals("initial_only", properties.getProperty("snapshot.mode"));
        assertEquals("public.orders", properties.getProperty("table.include.list"));
        assertFalse("public.orders".matches(properties.getProperty("snapshot.include.collection.list")));
        assertFalse("quoted.\"(?!)\"".matches(properties.getProperty("snapshot.include.collection.list")));
    }

    public void testPinnedProducerInitializesFreshOffsetAndRetainsCompletedOffset()
    {
        Properties properties = schemaSyncProperties();
        PostgresSchemaSync.configure(properties, true);
        io.debezium.snapshot.mode.InitialOnlySnapshotter producer =
            new io.debezium.snapshot.mode.InitialOnlySnapshotter();
        assertEquals(producer.name(), properties.getProperty("snapshot.mode"));
        // A fresh position is captured by the native snapshot transaction;
        // an existing completed offset instead emits SKIPPED without replacing it.
        assertTrue(producer.shouldSnapshotData(false, false));
        assertFalse(producer.shouldSnapshotData(true, false));
        assertFalse(producer.shouldStream());
    }

    public void testFiniteProducerRequiresGenerationOwnedCompletionNotification()
    {
        Properties properties = schemaSyncProperties();
        PostgresSchemaSync.configure(properties, true);
        SnapshotCompletion completion = SnapshotCompletion.configure(properties,
            "pg_synchdb/1_11111111-1111-1111-1111-111111111111_22222222-2222-2222-2222-222222222222",
            new BatchBridge(() -> 1));
        assertNotNull(completion);
        assertEquals("sink", properties.getProperty("notification.enabled.channels"));
        assertTrue(properties.getProperty("notification.sink.topic.name")
            .matches("__synchdb_snapshot_22222222222222222222222222222222_[0-9a-f]{32}"));
    }

    public void testOrdinaryNoDataStillStreams()
    {
        Properties properties = new Properties();
        properties.setProperty("snapshot.mode", "no_data");
        PostgresSchemaSync.configure(properties, false);
        assertEquals("no_data", properties.getProperty("snapshot.mode"));
        assertNull(properties.getProperty("snapshot.include.collection.list"));
    }

    public void testRejectsAnUnrelatedNativeMode()
    {
        Properties properties = new Properties();
        properties.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        properties.setProperty("snapshot.mode", "initial");
        try
        {
            PostgresSchemaSync.configure(properties, true);
            fail("schema-only initialization cannot admit a data snapshot mode");
        }
        catch (IllegalArgumentException expected) {}
    }
}
