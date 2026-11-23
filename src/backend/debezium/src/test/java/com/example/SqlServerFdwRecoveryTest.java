package com.example;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.relational.TableId;
import java.util.Properties;
import java.util.regex.Pattern;
import junit.framework.TestCase;

public class SqlServerFdwRecoveryTest extends TestCase
{
    private Properties selectedTable(String mode)
    {
        Properties properties = new Properties();
        properties.setProperty("snapshot.mode", mode);
        properties.setProperty("database.names", "testDB");
        properties.setProperty("database.include.list", "testDB");
        properties.setProperty("schema.include.list", "dbo");
        properties.setProperty("table.include.list", "dbo.fdw_cdc_test");
        properties.setProperty("snapshot.include.collection.list", "dbo.fdw_cdc_test");
        properties.setProperty("schema.history.internal.store.only.captured.tables.ddl", "true");
        return properties;
    }

    public void testFdwRecoveryRebuildsCapturedSchemaWithoutADataSnapshotSubset()
    {
        Properties properties = selectedTable("recovery");
        TableId selected = new TableId("testDB", "dbo", "fdw_cdc_test");
        SqlServerConnectorConfig old = new SqlServerConnectorConfig(Configuration.from(properties));
        // Debezium's data snapshot selection matches the catalog-qualified
        // identifier, while SQL Server's streaming filter uses schema.table.
        // Passing the FDW selection to both leaves the data-snapshot set empty
        // but still asks SQL Server to publish this table's recovered schema.
        assertFalse(Pattern.compile(old.getDataCollectionsToBeSnapshotted().get(0))
            .matcher(selected.identifier()).matches());
        SourceConsistency.configure(properties, 3, true);
        SqlServerConnectorConfig config = new SqlServerConnectorConfig(Configuration.from(properties));

        assertEquals("recovery", properties.getProperty("snapshot.mode"));
        assertTrue(config.getDataCollectionsToBeSnapshotted().isEmpty());
        assertTrue(config.getTableFilters().dataCollectionFilter().isIncluded(
            selected));
        assertTrue(config.getTableFilters().eligibleForSchemaDataCollectionFilter().isIncluded(selected));
        assertFalse(config.getTableFilters().dataCollectionFilter().isIncluded(
            new TableId("testDB", "dbo", "unselected_table")));
        assertFalse(config.getTableFilters().eligibleForSchemaDataCollectionFilter().isIncluded(
            new TableId("testDB", "dbo", "unselected_table")));
        assertFalse(config.getTableFilters().dataCollectionFilter().isIncluded(
            new TableId("testDB", "other_schema", "fdw_cdc_test")));
    }

    public void testOrdinaryNativeConfigurationRetainsItsSnapshotSubset()
    {
        Properties properties = selectedTable("no_data");
        SourceConsistency.configure(properties, 3);
        SqlServerConnectorConfig config = new SqlServerConnectorConfig(Configuration.from(properties));
        assertEquals(1, config.getDataCollectionsToBeSnapshotted().size());
        assertEquals("dbo.fdw_cdc_test", config.getDataCollectionsToBeSnapshotted().get(0));
    }

    public void testNativeInitialAndAlwaysContinueToCdcWithTheKnownBoundaryGap()
    {
        for (String mode : new String[]{"initial", "always"})
        {
            Properties properties = selectedTable(mode);
            // Native snapshots derive their complete baseline from the capture
            // selection. The optional data-only subset uses another ID format.
            properties.remove("snapshot.include.collection.list");
            SourceConsistency.configure(properties, 3);
            SqlServerConnectorConfig config = new SqlServerConnectorConfig(Configuration.from(properties));

            assertEquals(mode, properties.getProperty("snapshot.mode"));
            assertEquals("exclusive", properties.getProperty("snapshot.isolation.mode"));
            assertEquals("exclusive", properties.getProperty("snapshot.locking.mode"));
            assertEquals("1", properties.getProperty("snapshot.max.threads"));
            assertTrue(config.getTableFilters().dataCollectionFilter().isIncluded(
                new TableId("testDB", "dbo", "fdw_cdc_test")));
            assertFalse(config.getTableFilters().dataCollectionFilter().isIncluded(
                new TableId("testDB", "dbo", "unselected_table")));
        }
    }
}
