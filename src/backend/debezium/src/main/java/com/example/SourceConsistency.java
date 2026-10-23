package com.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * The native connector owns its snapshot transaction, log position and history.
 * These settings select the pinned connector's protocol; they are not a receipt
 * proving that a remote snapshot or a source prerequisite has completed.
 */
final class SourceConsistency
{
    static final String ENGINE_VERSION = "3.5.2.Final";
    private static final Set<String> MODES = new HashSet<>(Arrays.asList(
            "initial", "initial_only", "always", "no_data", "never"));

    private SourceConsistency() {}

    private static String provider(int connectorType)
    {
        switch (connectorType)
        {
            case 1: return "mysql";
            case 2: return "oracle";
            case 3: return "sqlserver";
            case 4: return "oracle"; // Snapshot only; native OLR handoff is separate.
            case 5: return "postgresql";
            default: throw new IllegalArgumentException("Unsupported source consistency provider");
        }
    }

    private static String connectorClass(int connectorType)
    {
        String source = provider(connectorType);
        String name;

        switch (connectorType)
        {
            case 1: name = "MySql"; break;
            case 2:
            case 4: name = "Oracle"; break;
            case 3: name = "SqlServer"; break;
            case 5: name = "Postgres"; break;
            default: throw new IllegalArgumentException("Unsupported source consistency provider");
        }
        return "io.debezium.connector." + source + "." + name + "Connector";
    }

    private static void requireVersionResource(String resource)
    {
        Properties build = new Properties();

        try (InputStream input = SourceConsistency.class.getResourceAsStream(resource))
        {
            if (input == null)
                throw new IllegalStateException("Native source consistency connector version is unavailable");
            build.load(input);
        }
        catch (IOException failure)
        {
            throw new IllegalStateException("Native source consistency connector version cannot be read", failure);
        }
        if (!ENGINE_VERSION.equals(build.getProperty("version")))
            throw new IllegalStateException("Native source consistency requires Debezium " + ENGINE_VERSION);
    }

    private static void requireVersion(int connectorType)
    {
        requireVersionResource("/io/debezium/connector/" + provider(connectorType) + "/build.version");
        // The generic JDBC pool/snapshot sequencing is part of this policy.
        requireVersionResource("/io/debezium/build.version");
    }

    private static void require(Properties properties, String key, String expected)
    {
        String current = properties.getProperty(key);

        if (current != null && !expected.equals(current))
            throw new IllegalArgumentException("Unsupported native source consistency property: " + key);
        properties.setProperty(key, expected);
    }

    private static void forbid(Properties properties, String key)
    {
        if (properties.getProperty(key) != null || properties.containsKey(key))
            throw new IllegalArgumentException("Unsupported native source consistency property: " + key);
    }

    /** Call after every property has been assembled, immediately before build(). */
    static void configure(Properties properties, int connectorType)
    {
        if (properties == null)
            throw new IllegalArgumentException("Native source consistency properties are required");
        properties.forEach((key,value) -> {
            if (!(key instanceof String) || !(value instanceof String))
                throw new IllegalArgumentException("Native source consistency properties must be strings");
        });
        requireVersion(connectorType);
        require(properties, "connector.class", connectorClass(connectorType));
        String mode = properties.getProperty("snapshot.mode", "initial");
        if (!MODES.contains(mode))
            throw new IllegalArgumentException("Unsupported native source consistency snapshot mode");
        if ("never".equals(mode) && connectorType != 1 && connectorType != 5)
            throw new IllegalArgumentException("This native connector does not support snapshot mode never");
        if (connectorType == 3 && ("initial".equals(mode) || "always".equals(mode)))
            throw new IllegalArgumentException("SQL Server native snapshot-to-CDC requires an unimplemented capture-drain fence; use initial_only or explicit no_data");
        properties.setProperty("snapshot.mode", mode);

        for (String key : properties.stringPropertyNames())
            if (key.startsWith("snapshot.select.statement.overrides") ||
                key.startsWith("snapshot.mode.configuration.based.") ||
                key.equals("snapshot.mode.custom.name") ||
                key.equals("snapshot.locking.mode.custom.name") ||
                key.equals("snapshot.query.mode.custom.name"))
                throw new IllegalArgumentException("Unsupported native source consistency property: " + key);

        require(properties, "tasks.max", "1");
        require(properties, "snapshot.max.threads", "1");
        require(properties, "snapshot.query.mode", "select_all");
        require(properties, "event.processing.failure.handling.mode", "fail");
        require(properties, "schema.history.internal.skip.unparseable.ddl", "false");
        require(properties, "incremental.snapshot.allow.schema.changes", "false");

        if (("initial".equals(mode) || "initial_only".equals(mode) || "always".equals(mode)) &&
            !(connectorType == 4 && "initial_only".equals(mode)))
        {
            String snapshot = properties.getProperty("snapshot.include.collection.list");
            String capture = properties.getProperty("table.include.list");

            // Oracle/SQL Server use database.schema.table for snapshot filters,
            // but schema.table for capture filters. Equal strings do not prove
            // equal sets. The supported full baseline uses the connector's
            // default snapshot set derived from its captured tables.
            if (snapshot != null && (connectorType == 2 || connectorType == 3 || connectorType == 4))
                throw new IllegalArgumentException("This native source requires the default snapshot table scope");
            if (snapshot != null && !snapshot.equals(capture))
                throw new IllegalArgumentException("Native snapshot and streaming table scopes must match");
        }

        switch (connectorType)
        {
            case 1:
                // The native global/table lock and repeatable-read transaction
                // establish a binlog position. InnoDB and no concurrent DDL are
                // prerequisites; an arbitrary local SQL offset is never a seed.
                require(properties, "snapshot.locking.mode", "minimal");
                require(properties, "inconsistent.schema.handling.mode", "fail");
                break;
            case 2:
            case 4:
                // Oracle 3.5.2 has shared, none and custom locking (no extended
                // mode). Shared locks do not remove the no-concurrent-DDL
                // prerequisite. Never discard a long transaction or replay a
                // partially emitted table snapshot inside the same session.
                require(properties, "snapshot.locking.mode", "shared");
                require(properties, "snapshot.database.errors.max.retries", "0");
                require(properties, "log.mining.transaction.retention.ms", "0");
                require(properties, "log.mining.buffer.transaction.events.threshold", "0");
                require(properties, "log.mining.window.max.ms", "0");
                if (connectorType == 2)
                {
                    require(properties, "database.connection.adapter", "logminer");
                    // Pinned internal API: SKIP loses transactions crossing
                    // the snapshot SCN; VIEW_ONLY has a recent-commit window.
                    // ALL consults V$/GV$TRANSACTION and a LogMiner session,
                    // preserving snapshotPendingTransactions in the offset.
                    require(properties, "internal.log.mining.transaction.snapshot.boundary.mode", "all");
                }
                break;
            case 3:
                // Exclusive keeps a snapshot-only read stable without relying
                // on ALLOW_SNAPSHOT_ISOLATION. It cannot drain the asynchronous
                // CDC job: initial/always handoff is rejected above, rather
                // than turning a declared prerequisite into a false guarantee.
                require(properties, "snapshot.isolation.mode", "exclusive");
                require(properties, "snapshot.locking.mode", "exclusive");
                break;
            case 5:
                require(properties, "plugin.name", "pgoutput");
                require(properties, "snapshot.isolation.mode", "serializable");
                require(properties, "offset.mismatch.strategy", "trust_offset");
                require(properties, "lsn.flush.mode", "connector");
                require(properties, "slot.drop.on.stop", "false");
                // Identity must be validated by the source/target contract;
                // startup must never replace an administrator's FULL setting.
                forbid(properties, "replica.identity.autoset.values");
                forbid(properties, "internal.slot.seek.to.known.offset.on.start");
                forbid(properties, "flush.lsn.source");
                break;
            default:
                throw new IllegalArgumentException("Unsupported source consistency provider");
        }
        if (connectorType == 4)
        {
            // The C worker proves the OLR fence before starting this producer.
            // Java supplies a finite snapshot, never a competing CDC reader.
            require(properties, "database.connection.adapter", "logminer");
            require(properties, "internal.log.mining.transaction.snapshot.boundary.mode", "all");
            // A selected snapshot-only run has no CDC handoff and may limit
            // its tables. Debezium forbids setting both include and exclude.
            if (properties.getProperty("table.include.list") == null)
                properties.setProperty("table.exclude.list", ".*\\.SYNCHDB_OLR_FENCE");
            if ("no_data".equals(mode))
            {
                properties.setProperty("snapshot.mode", "custom");
                properties.setProperty("snapshot.mode.custom.name", "synchdb_olr_schema");
            }
            else
                properties.setProperty("snapshot.mode", "initial_only");
        }
    }
}
