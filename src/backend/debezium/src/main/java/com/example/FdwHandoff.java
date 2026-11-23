package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** Compatibility bridge from an FDW read to an engine-owned generation.
 * TODO: coordinate the FDW read transaction and schema with the log position.
 * Retaining a slot or writing an offset does not establish that consistency. */
final class FdwHandoff
{
    private static final ObjectMapper JSON = new ObjectMapper()
        .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    private static final String ARTIFACT =
        "pg_synchdb/[1-9][0-9]*_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_"
        + "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    private FdwHandoff() {}

    private static String base(JsonNode request)
    {
        String base = request.path("artifact_base").asText();
        if (!base.matches(ARTIFACT))
            throw new IllegalArgumentException("FDW handoff requires an exact owned artifact generation");
        return base;
    }

    private static int type(JsonNode request)
    {
        int type = request.path("connector_type").asInt();
        if (type != 1 && type != 2 && type != 3 && type != 5)
            throw new IllegalArgumentException("Unsupported FDW handoff provider");
        return type;
    }

    static String prepareSource(String text) throws Exception
    {
        JsonNode request = JSON.readTree(text);
        String base = base(request);
        if (type(request) != 5)
            throw new IllegalArgumentException("Only PostgreSQL needs an FDW source slot");
        Properties properties = new Properties();
        properties.setProperty("name", "engine");
        properties.setProperty("topic.prefix", "synchdb-connector");
        properties.setProperty("database.hostname", request.required("hostname").asText());
        properties.setProperty("database.port", request.required("port").asText());
        properties.setProperty("database.user", request.required("user").asText());
        properties.setProperty("database.password", request.required("password").asText());
        properties.setProperty("database.dbname", request.required("database").asText());
        properties.setProperty("plugin.name", "pgoutput");
        properties.setProperty("database.connection.timeout.ms", "10000");
        properties.setProperty("query.timeout.ms", "120000");
        String schema = request.path("schema").asText("null");
        if (!schema.isEmpty() && !schema.equals("null"))
            properties.setProperty("schema.include.list", schema);
        String filter = request.path("table").asText("null");
        boolean allTables = filter.equals("null");
        if (filter.startsWith("file:"))
        {
            JsonNode tables = JSON.readTree(Files.readString(Path.of(filter.substring(5)), StandardCharsets.UTF_8)).required("table_list");
            List<String> entries = new ArrayList<>();
            if (!tables.isArray()) throw new IllegalArgumentException("Invalid captured-table filter");
            for (JsonNode table : tables)
            {
                if (!table.isTextual()) throw new IllegalArgumentException("Invalid captured-table filter entry");
                entries.add(table.textValue());
            }
            filter = String.join(",", entries);
        }
        if (!allTables)
            properties.setProperty("table.include.list", filter);
        properties.setProperty("publication.autocreate.mode", allTables ? "all_tables" : "filtered");
        NativeSourcePreflight.configurePostgres(properties, base, null, null);
        PostgresConnectorConfig config = new PostgresConnectorConfig(Configuration.from(properties));
        try (PostgresConnection source = new PostgresConnection(JdbcConfiguration.adapt(config.getJdbcConfig().edit()
                 .with("connectTimeout", 10).with("socketTimeout", 120).build()), "synchdb-fdw-compatibility"))
        {
            source.connection().setAutoCommit(true);
            try (PreparedStatement query = source.connection().prepareStatement(
                "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name=?"))
            {
                query.setString(1, config.slotName());
                try (ResultSet rows = query.executeQuery())
                {
                    if (rows.next()) throw new IllegalStateException("Fresh FDW generation refuses an existing replication slot");
                }
            }
            String publication = quote(config.publicationName());
            String command = "CREATE PUBLICATION " + publication;
            if (allTables)
                command += " FOR ALL TABLES";
            else
            {
                List<String> tables = new ArrayList<>();
                try (PreparedStatement query = source.connection().prepareStatement(
                    "SELECT n.nspname,c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                    + "WHERE c.relkind='r' AND c.relpersistence<>'t'"); ResultSet rows = query.executeQuery())
                {
                    while (rows.next())
                    {
                        TableId table = new TableId(null, rows.getString(1), rows.getString(2));
                        if (config.getTableFilters().dataCollectionFilter().isIncluded(table))
                            tables.add(quote(table.schema()) + "." + quote(table.table()));
                    }
                }
                if (!tables.isEmpty()) command += " FOR TABLE " + String.join(",", tables);
            }
            try (PreparedStatement create = source.connection().prepareStatement(command))
            {
                create.setQueryTimeout(120);
                create.execute();
            }
            // The slot is created before SQL takes the FDW candidate LSN. It
            // retains WAL, but its exported snapshot is not imported by FDW.
            try (PreparedStatement create = source.connection().prepareStatement(
                "SELECT slot_name,lsn::text FROM pg_catalog.pg_create_logical_replication_slot(?,'pgoutput')"))
            {
                create.setString(1, config.slotName());
                create.setQueryTimeout(120);
                try (ResultSet row = create.executeQuery())
                {
                    if (!row.next()) throw new IllegalStateException("FDW replication slot was not created");
                }
            }
        }
        return "{\"status\":\"PREPARED\",\"handoff_verified\":false}";
    }

    private static String quote(String identifier)
    {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    static String publish(String text) throws Exception
    {
        JsonNode request = JSON.readTree(text);
        publish(type(request), request.required("database").asText(),
                Path.of(base(request)), request.required("material"));
        return "{\"status\":\"PREPARED\",\"handoff_verified\":false}";
    }

    // Package-private overload lets tests exercise file ownership without a DB.
    static void publish(int type, String database, Path base, JsonNode material) throws Exception
    {
        if (type != 1 && type != 2 && type != 3 && type != 5)
            throw new IllegalArgumentException("Unsupported FDW handoff provider");
        JsonNode offset = material.path("offset");
        JsonNode history = material.path("history");
        if (!material.isObject() || !offset.isObject() || offset.isEmpty() || !history.isArray())
            throw new IllegalArgumentException("FDW handoff requires an offset object and a history array");
        Path offsetPath = Path.of(base + "_offsets.dat");
        Path historyPath = Path.of(base + "_schemahistory.dat");
        if (!Files.notExists(offsetPath, LinkOption.NOFOLLOW_LINKS)
            || !Files.notExists(historyPath, LinkOption.NOFOLLOW_LINKS))
            throw new IllegalStateException("FDW seed publication cannot overwrite an existing engine file");
        List<String> records = new ArrayList<>();
        long historyBytes = 0;
        for (JsonNode record : history)
        {
            String line = record.isTextual() ? record.textValue() : record.toString();
            if (line.indexOf('\n') >= 0 || line.indexOf('\r') >= 0
                || !JSON.readTree(line).isObject()
                || !new HistoryRecord(DocumentReader.defaultReader().read(line)).isValid())
                throw new IllegalArgumentException("Invalid FDW schema history record");
            historyBytes += line.getBytes(StandardCharsets.UTF_8).length + 1L;
            if (historyBytes > 64L * 1024 * 1024)
                throw new IllegalArgumentException("FDW schema history exceeds 64 MiB");
            records.add(line);
        }
        if ((type == 1 || type == 2) && records.isEmpty())
            throw new IllegalArgumentException("FDW handoff requires schema history for this provider");
        if ((type == 3 || type == 5) && !records.isEmpty())
            throw new IllegalArgumentException("This FDW provider rebuilds history or has no history file");
        if (!records.isEmpty())
        {
            try (BufferedWriter output = Files.newBufferedWriter(historyPath, StandardCharsets.UTF_8,
                     StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))
            {
                for (String line : records)
                {
                    output.write(line);
                    output.newLine();
                }
            }
            try (FileChannel historyFile = FileChannel.open(historyPath, StandardOpenOption.WRITE))
            {
                historyFile.force(true);
            }
        }
        // C has committed exclusive UUID ownership; no active engine is allowed
        // to use this generation until every preparation step has succeeded.
        OffsetStore.create(offsetPath.toString(), type, database, offset.toString());
        OffsetStore.requireResumePartition(type, database, offsetPath);
    }
}
