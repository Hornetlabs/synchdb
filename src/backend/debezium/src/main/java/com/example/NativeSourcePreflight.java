package com.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.TableId;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/* Native PG ownership is independent of a display alias. Catalog checks are
 * read-only, precede engine construction, and do not manufacture a CDC seed.
 * A generation that dies before persisting its first offset requires explicit
 * recovery/reset. Neither a dangling slot nor an empty file is a fresh start.
 * Source administrators must preserve exclusive use of the owned names: PG
 * slots have no immutable UUID that could detect an administrator recreating
 * the same name. The connector repeats its native trust_offset check at start.
 */
final class NativeSourcePreflight
{
    private static final Pattern ARTIFACT = Pattern.compile(
        "pg_synchdb/[1-9][0-9]*_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_"
        + "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");
    private static final ObjectMapper JSON = new ObjectMapper()
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .enable(DeserializationFeature.USE_LONG_FOR_INTS)
        .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    private NativeSourcePreflight() {}

    static void configurePostgres(Properties properties, String artifactBase,
                                  String legacySlot, String legacyPublication)
    {
        Matcher artifact = ARTIFACT.matcher(artifactBase == null ? "" : artifactBase);
        if (!artifact.matches())
            throw new IllegalArgumentException("Native source identity requires a canonical artifact generation");
        if ((legacySlot == null) != (legacyPublication == null))
            throw new IllegalArgumentException("Legacy slot and publication identities must be supplied together");
        String generation = artifact.group(1).replace("-", "");
        String slot = legacySlot == null ? "synchdb_" + generation + "_slot" : legacySlot;
        String publication = legacyPublication == null ? "synchdb_" + generation + "_pub" : legacyPublication;
        if (!slot.matches("[a-z0-9_]{1,63}") || publication.isEmpty() || publication.indexOf('\0') >= 0
            || publication.getBytes(StandardCharsets.UTF_8).length > 63)
            throw new IllegalArgumentException("PostgreSQL source object identity is not a valid exact server name");
        properties.setProperty("slot.name", slot);
        properties.setProperty("publication.name", publication);
    }

    static void requireArtifactState(Properties properties, boolean resumeExisting)
    {
        Path offset = Path.of(required(properties, "offset.storage.file.filename"));
        if (resumeExisting == Files.notExists(offset))
            throw new IllegalStateException(resumeExisting
                ? "An existing artifact generation requires its engine-owned offset"
                : "A fresh artifact generation cannot adopt an existing offset file");
    }

    /* Use the pinned Loader, including its snapshot completion normalization.
     * Compare the safe flush/confirmation boundary: lsn_commit, then lsn_proc,
     * then event lsn. The engine's streaming seek has its own position logic.
     * A slot ahead of the local file requires explicit recovery.
     */
    static Lsn postgresResumeLsn(Properties properties)
    {
        Path path = Path.of(required(properties, "offset.storage.file.filename"));
        String database = required(properties, "database.dbname");
        OffsetStore.requireResumePartition(5, database, path);
        try
        {
            String text = OffsetStore.getFile(5, database, path);
            JsonNode json = JSON.readTree(text);
            if (json == null || !json.isObject() || json.isEmpty())
                throw new IllegalStateException("PostgreSQL resume offset must be a non-empty object");
            Map<String,Object> values = JSON.convertValue(json, new TypeReference<Map<String,Object>>() {});
            for (String name : new String[]{"lsn", "lsn_proc", "lsn_commit"})
            {
                JsonNode value = json.get(name);
                if (value != null && !value.isNull() && (!value.isIntegralNumber() || !value.canConvertToLong()))
                    throw new IllegalStateException("PostgreSQL offset " + name + " must be a signed 64-bit JSON integer");
            }
            PostgresOffsetContext offset = new PostgresOffsetContext.Loader(
                new PostgresConnectorConfig(Configuration.from(properties))).load(values);
            if (offset.isInitialSnapshotRunning())
                throw new IllegalStateException("A partial native snapshot requires explicit target/artifact recovery before restart");
            Lsn position = offset.lastCommitLsn();
            if (position == null || !position.isValid())
                position = offset.lastCompletelyProcessedLsn();
            if (position == null || !position.isValid())
            {
                JsonNode event = json.get("lsn");
                position = event == null || event.isNull() ? null : Lsn.valueOf(event.longValue());
            }
            if (position == null || !position.isValid())
                throw new IllegalStateException("PostgreSQL offset has no valid native resume LSN");
            return position;
        }
        catch (java.io.IOException | IllegalArgumentException failure)
        {
            throw new IllegalStateException("Cannot decode PostgreSQL native resume state", failure);
        }
    }

    static void validatePostgres(Properties properties, boolean resumeExisting) throws SQLException
    {
        requireArtifactState(properties, resumeExisting);
        PostgresConnectorConfig config = new PostgresConnectorConfig(Configuration.from(properties));
        if (config.isPublishViaPartitionRoot())
            throw new IllegalStateException("Native source preflight currently requires leaf partition publication identities");
        boolean streaming = !"initial_only".equals(properties.getProperty("snapshot.mode", "initial"));
        Lsn resume = resumeExisting ? postgresResumeLsn(properties) : null;
        try (PostgresConnection source = new PostgresConnection(config.getJdbcConfig(), "synchdb-native-preflight"))
        {
            source.connection().setReadOnly(true);
            boolean slotExists;
            try (PreparedStatement query = prepare(source,
                "SELECT slot_type,plugin,database,active,temporary,restart_lsn::text,confirmed_flush_lsn::text,"
                + "pg_catalog.to_jsonb(s)->>'wal_status' FROM pg_catalog.pg_replication_slots s WHERE slot_name=?"))
            {
                query.setString(1, config.slotName());
                try (ResultSet row = query.executeQuery())
                {
                    slotExists = row.next();
                    if (slotExists && !resumeExisting)
                        throw new IllegalStateException("Fresh native generation refuses an existing PostgreSQL replication slot");
                    if (slotExists && streaming)
                    {
                        if (!"logical".equals(row.getString(1)) || !"pgoutput".equals(row.getString(2))
                            || !config.databaseName().equals(row.getString(3)) || row.getBoolean(4) || row.getBoolean(5))
                            throw new IllegalStateException("Owned PostgreSQL slot is incompatible, temporary, or already active");
                        String restart = row.getString(6);
                        String confirmed = row.getString(7);
                        if (restart == null || confirmed == null || "lost".equals(row.getString(8))
                            || !Lsn.valueOf(restart).isValid() || !Lsn.valueOf(confirmed).isValid())
                            throw new IllegalStateException("Owned PostgreSQL slot has no retained recoverable WAL position");
                        if (Lsn.valueOf(confirmed).compareTo(resume) > 0 || Lsn.valueOf(restart).compareTo(resume) > 0)
                            throw new IllegalStateException("PostgreSQL slot has advanced beyond the persisted engine offset");
                    }
                }
            }
            boolean publicationExists;
            try (PreparedStatement query = prepare(source,
                "SELECT puballtables,pubinsert,pubupdate,pubdelete,pubtruncate,"
                + "COALESCE((pg_catalog.to_jsonb(p)->>'pubviaroot')::boolean,false) "
                + "FROM pg_catalog.pg_publication p WHERE pubname=?"))
            {
                query.setString(1, config.publicationName());
                try (ResultSet row = query.executeQuery())
                {
                    publicationExists = row.next();
                    if (publicationExists && !resumeExisting)
                        throw new IllegalStateException("Fresh native generation refuses an existing PostgreSQL publication");
                    if (publicationExists && streaming)
                    {
                        boolean expectedAll = "all_tables".equals(properties.getProperty("publication.autocreate.mode", "all_tables"));
                        if (row.getBoolean(1) != expectedAll || !row.getBoolean(2) || !row.getBoolean(3)
                            || !row.getBoolean(4) || !row.getBoolean(5) || row.getBoolean(6) != config.isPublishViaPartitionRoot())
                            throw new IllegalStateException("Owned PostgreSQL publication does not match native capture semantics");
                    }
                }
            }
            if (resumeExisting && streaming)
            {
                if (!slotExists || !publicationExists)
                    throw new IllegalStateException("PostgreSQL resume requires both existing owned slot and publication; automatic recreation is unsafe");
                validatePublicationTables(source, config);
            }
        }
    }

    private static void validatePublicationTables(PostgresConnection source, PostgresConnectorConfig config) throws SQLException
    {
        try (PreparedStatement query = prepare(source,
            "SELECT 1 FROM pg_catalog.pg_publication_rel pr JOIN pg_catalog.pg_publication p ON p.oid=pr.prpubid "
            + "WHERE p.pubname=? AND (pg_catalog.to_jsonb(pr)->>'prqual' IS NOT NULL "
            + "OR pg_catalog.to_jsonb(pr)->>'prattrs' IS NOT NULL) LIMIT 1"))
        {
            query.setString(1, config.publicationName());
            try (ResultSet row = query.executeQuery())
            {
                if (row.next())
                    throw new IllegalStateException("Native resume does not accept publication row filters or column lists");
            }
        }
        Set<String> published = new HashSet<>();
        try (PreparedStatement query = prepare(source,
            "SELECT schemaname,tablename FROM pg_catalog.pg_publication_tables WHERE pubname=?"))
        {
            query.setString(1, config.publicationName());
            try (ResultSet rows = query.executeQuery())
            {
                while (rows.next()) published.add(rows.getString(1) + "\0" + rows.getString(2));
            }
        }
        // pg_publication_tables reports actual leaf identities when
        // publish_via_partition_root=false; a partitioned parent is not a
        // missing published relation. Use the same pinned data filter on
        // ordinary/leaf tables, without a JDBC metadata query that has no
        // statement timeout.
        try (PreparedStatement query = prepare(source,
            "SELECT n.nspname,c.relname FROM pg_catalog.pg_class c "
            + "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            + "WHERE c.relkind='r' AND c.relpersistence<>'t'"))
        {
            try (ResultSet rows = query.executeQuery())
            {
                while (rows.next())
                {
                    TableId table = new TableId(null,rows.getString(1),rows.getString(2));
                    if (config.getTableFilters().dataCollectionFilter().isIncluded(table)
                        && !published.contains(table.schema() + "\0" + table.table()))
                        throw new IllegalStateException("Owned PostgreSQL publication omits a captured source table");
                }
            }
        }
    }

    private static PreparedStatement prepare(PostgresConnection source, String sql) throws SQLException
    {
        PreparedStatement query = source.connection().prepareStatement(sql);
        try
        {
            long milliseconds = source.config().getQueryTimeout().toMillis();
            // Preserve the connector's explicit zero/no-limit choice; ceil a
            // positive sub-second value so it cannot accidentally become zero.
            query.setQueryTimeout(milliseconds == 0 ? 0
                : (int) Math.min(Integer.MAX_VALUE,Math.max(1,(milliseconds + 999) / 1000)));
            return query;
        }
        catch (SQLException | RuntimeException failure)
        {
            try { query.close(); } catch (SQLException closeFailure) { failure.addSuppressed(closeFailure); }
            throw failure;
        }
    }

    private static String required(Properties properties, String key)
    {
        String value = properties.getProperty(key);
        if (value == null || value.isEmpty())
            throw new IllegalArgumentException("Missing native preflight property: " + key);
        return value;
    }
}
