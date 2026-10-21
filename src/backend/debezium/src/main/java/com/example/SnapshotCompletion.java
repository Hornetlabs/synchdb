package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.StopEngineException;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/* initial_only ends its snapshot producer, but the pinned async engine keeps
 * polling. Its sink notification shares the source FIFO and follows snapshot
 * data/heartbeat publication. After the complete containing batch is applied
 * and acknowledged, StopEngineException requests the engine's normal shutdown.
 * EngineSession still waits for successful completion and settled callbacks.
 * Neither snapshot:last nor an idle queue is used as a completion proof.
 */
final class SnapshotCompletion
{
    private static final String NOTIFICATION_SCHEMA = "io.debezium.connector.common.Notification";
    private static final String SINGLE_PARTITION = "single";
    private static final int MAX_NOTIFICATION_CHARS = 64 * 1024 * 1024;
    private static final ObjectMapper JSON = new ObjectMapper()
        .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    private final String topic;
    private final BatchBridge bridge;
    private final String connectorName;
    private final Set<String> expected;
    private final Set<String> completed = new HashSet<>();
    private final boolean multipleDatabases;

    private SnapshotCompletion(String topic, BatchBridge bridge, String connectorName,
                               Set<String> expected, boolean multipleDatabases)
    {
        this.topic = topic;
        this.bridge = bridge;
        this.connectorName = connectorName;
        this.expected = expected;
        this.multipleDatabases = multipleDatabases;
    }

    static SnapshotCompletion configure(Properties properties, String artifactBase, BatchBridge bridge)
    {
        if (!"initial_only".equals(properties.getProperty("snapshot.mode")))
            return null;
        String generation = artifactBase == null ? "" : artifactBase.substring(artifactBase.lastIndexOf('_') + 1);
        if (!generation.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"))
            throw new IllegalArgumentException("Snapshot completion requires a canonical artifact generation");
        String topicPrefix = "__synchdb_snapshot_" + generation.replace("-", "") + "_";
        String topic = properties.getProperty("notification.sink.topic.name");
        if (topic == null)
            topic = topicPrefix + UUID.randomUUID().toString().replace("-", "");
        else if (!topic.matches(topicPrefix + "[0-9a-f]{32}"))
            throw new IllegalArgumentException("Snapshot notification destination must belong to this session generation");
        String connectorName = properties.getProperty("topic.prefix");
        if (connectorName == null || connectorName.isEmpty())
            throw new IllegalArgumentException("Snapshot completion requires the native connector logical name");
        properties.setProperty("notification.enabled.channels", "sink");
        properties.setProperty("notification.sink.topic.name", topic);
        boolean sqlServer = "io.debezium.connector.sqlserver.SqlServerConnector".equals(properties.getProperty("connector.class"));
        Set<String> expected = sqlServer
            ? new HashSet<>(new SqlServerConnectorConfig(Configuration.from(properties)).getDatabaseNames())
            : Set.of(SINGLE_PARTITION);
        if (expected.isEmpty())
            throw new IllegalArgumentException("Snapshot completion requires a source partition");
        return new SnapshotCompletion(topic,bridge,connectorName,expected,sqlServer);
    }

    void handleBatch(List<ChangeEvent<String,String>> records,
                     DebeziumEngine.RecordCommitter<ChangeEvent<String,String>> committer) throws InterruptedException
    {
        Set<String> terminal = new HashSet<>();
        for (ChangeEvent<String,String> record : records)
        {
            if (record == null)
                continue; // BatchBridge reports the invalid raw record.
            if (!topic.equals(record.destination()))
            {
                if ((!terminal.isEmpty() || !completed.isEmpty()) && isData(record.value())
                    && (!multipleDatabases || terminal.contains(databasePartition(record))
                        || completed.contains(databasePartition(record))))
                    throw new IllegalStateException("Native snapshot data followed its terminal FIFO notification");
                continue;
            }
            JsonNode payload = notification(record.value());
            if (!"Initial Snapshot".equals(payload.path("aggregate_type").asText()))
                continue;
            if (!payload.path("additional_data").path("connector_name").isTextual()
                || !connectorName.equals(payload.path("additional_data").path("connector_name").textValue()))
                throw new IllegalStateException("Native snapshot notification belongs to a different connector");
            String type = payload.path("type").asText();
            if ("ABORTED".equals(type) || ("TABLE_SCAN_COMPLETED".equals(type)
                && (!payload.path("additional_data").path("status").isTextual()
                    || !"SUCCEEDED".equals(payload.path("additional_data").path("status").textValue()))))
                throw new IllegalStateException("Native snapshot producer reported an aborted or failed snapshot");
            if ("COMPLETED".equals(type) || "SKIPPED".equals(type))
            {
                String partition = SINGLE_PARTITION;
                if (multipleDatabases)
                    partition = databasePartition(record);
                if (!expected.contains(partition) || completed.contains(partition) || !terminal.add(partition))
                    throw new IllegalStateException("Native snapshot completion partition is unexpected or duplicated");
            }
        }
        // Keep every original record in its original source-committer order.
        // Internal notifications are excluded only from the target wire image.
        bridge.handleBatch(records,committer,record -> !topic.equals(record.destination()));
        completed.addAll(terminal);
        if (completed.equals(expected))
            // Outside BatchBridge's callback finally: this is the official normal
            // async polling stop, never an application/ACK failure in the bridge.
            throw new StopEngineException("Native initial_only snapshot boundary was fully acknowledged");
    }

    private static String databasePartition(ChangeEvent<String,String> record)
    {
        if (!(record instanceof EmbeddedEngineChangeEvent<?,?,?> nativeRecord))
            throw new IllegalStateException("Native snapshot record has no pinned source partition metadata");
        Map<String,?> sourcePartition = nativeRecord.sourceRecord().sourcePartition();
        Object database = sourcePartition.get("database");
        if (!(database instanceof String))
            throw new IllegalStateException("SQL Server snapshot record has no database partition");
        return (String) database;
    }

    private static boolean isData(String value)
    {
        if (value == null)
            return false;
        if (value.length() > MAX_NOTIFICATION_CHARS)
            throw new IllegalStateException("Native record following snapshot completion exceeds the frame limit");
        try
        {
            JsonNode record = JSON.readTree(value);
            return record != null && record.path("payload").path("op").isTextual()
                && !record.path("payload").path("op").textValue().isEmpty();
        }
        catch (IOException failure)
        {
            throw new IllegalStateException("Cannot validate native records after snapshot completion",failure);
        }
    }

    private static JsonNode notification(String value)
    {
        if (value == null || value.length() > MAX_NOTIFICATION_CHARS)
            throw new IllegalStateException("Invalid internal snapshot notification size");
        try
        {
            JsonNode record = JSON.readTree(value);
            if (record == null || !NOTIFICATION_SCHEMA.equals(record.path("schema").path("name").asText())
                || !record.path("schema").path("version").isIntegralNumber()
                || !record.path("schema").path("version").canConvertToInt()
                || record.path("schema").path("version").intValue() != 1
                || !record.path("payload").isObject() || !record.path("payload").path("type").isTextual()
                || !record.path("payload").path("aggregate_type").isTextual())
                throw new IllegalStateException("Internal snapshot notification has an invalid native schema");
            return record.path("payload");
        }
        catch (IOException failure)
        {
            throw new IllegalStateException("Cannot decode internal native snapshot notification",failure);
        }
    }
}
