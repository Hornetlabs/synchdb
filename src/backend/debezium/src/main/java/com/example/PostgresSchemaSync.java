package com.example;

import java.util.Properties;

/** The legacy native PostgreSQL schema-sync workflow assumes prebuilt targets. */
final class PostgresSchemaSync
{
    private PostgresSchemaSync() {}

    static void configure(Properties properties, boolean schemaSync)
    {
        if (!schemaSync)
            return;
        if (!"io.debezium.connector.postgresql.PostgresConnector".equals(properties.getProperty("connector.class"))
            || !"no_data".equals(properties.getProperty("snapshot.mode")))
            throw new IllegalArgumentException("PostgreSQL schema sync requires native no_data initialization");

        // PostgreSQL does not publish a schema image from a no_data snapshot;
        // users must create the target schema and tables before START. A finite
        // native snapshot selecting zero tables still establishes and persists
        // its own LSN. NativeSourcePreflight first creates its generation-owned
        // slot/publication because initial_only does not create a CDC slot.
        // SnapshotCompletion acknowledges its ordered completion
        // notification before the C worker exposes PAUSED. No source business
        // event or artificial delay is needed, and no historical row is copied.
        properties.setProperty("snapshot.mode", "initial_only");
        properties.setProperty("snapshot.include.collection.list", "(?!)");
    }
}
