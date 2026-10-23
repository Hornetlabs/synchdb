package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TreeSet;
import java.util.UUID;

/** Source evidence for the native OLR worker. The token must be observed on
 * that worker's actual OLR socket before any CDC data may be applied. */
final class OlrHandoff
{
    static final String TABLE = "SYNCHDB_OLR_FENCE";
    private static final ObjectMapper JSON = new ObjectMapper()
        .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
        .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    private OlrHandoff() {}

    static String prepare(String request) throws Exception
    {
        JsonNode input = JSON.readTree(request);
        String owner = input.path("schema").asText();
        if (!owner.matches("[A-Z][A-Z0-9_$#]{0,127}"))
            throw new IllegalArgumentException("OLR handoff requires one exact uppercase Oracle schema");
        String ssl = input.path("ssl_mode").asText("null");
        if (!"null".equals(ssl) && !"disable".equals(ssl))
            throw new IllegalArgumentException("OLR source fence does not yet support Oracle TLS");
        String database = input.path("database").asText();
        String service = database.substring(database.indexOf('/') + 1);
        if (!service.matches("[A-Za-z0-9_.-]+"))
            throw new IllegalArgumentException("Invalid Oracle service for OLR source fence");
        String host = input.path("hostname").asText();
        if (host.indexOf('/') >= 0 || host.indexOf('@') >= 0 || host.indexOf('?') >= 0)
            throw new IllegalArgumentException("Invalid Oracle host for OLR source fence");
        Properties credentials = new Properties();
        credentials.setProperty("user", input.path("user").asText());
        credentials.setProperty("password", input.path("password").asText());
        credentials.setProperty("oracle.net.CONNECT_TIMEOUT", "10000");
        credentials.setProperty("oracle.jdbc.ReadTimeout", "120000");
        String url = "jdbc:oracle:thin:@//" + host + ":" + input.path("port").asInt() + "/" + service;
        JsonNode previous = input.path("previous");
        try (Connection connection = DriverManager.getConnection(url, credentials))
        {
            connection.setAutoCommit(true);
            ObjectNode proof = JSON.createObjectNode();
            proof.put("version", 1);
            proof.put("owner", owner);
            try (PreparedStatement query = query(connection,
                "SELECT DBID, RESETLOGS_CHANGE#, SYS_CONTEXT('USERENV','CON_ID'), CURRENT_SCN FROM V$DATABASE");
                 ResultSet row = query.executeQuery())
            {
                if (!row.next()) throw new IllegalStateException("Missing Oracle database identity");
                proof.put("dbid", row.getString(1));
                proof.put("resetlogs_scn", row.getString(2));
                proof.put("con_id", row.getString(3));
                proof.put("start_scn", row.getString(4));
            }
            if (!previous.isMissingNode() && !previous.isNull())
            {
                for (String field : new String[]{"version", "owner", "dbid", "resetlogs_scn", "con_id"})
                    if (!proof.path(field).equals(previous.path(field)))
                        throw new IllegalStateException("OLR source identity/incarnation changed: " + field);
                for (String field : new String[]{"start_scn", "floor_scn", "redo_sequence",
                    "olr_first_data_scn", "snapshot_pending_xids", "scn"})
                    if (previous.has(field))
                        proof.set(field, previous.required(field));
            }
            else
            {
                // Starting at a log boundary rather than the snapshot SCN
                // reconstructs BEGIN records of transactions crossing it.
                try (PreparedStatement query = query(connection,
                    "SELECT MIN(FIRST_CHANGE#), MIN(SEQUENCE#), COUNT(DISTINCT THREAD#) FROM V$LOG");
                     ResultSet row = query.executeQuery())
                {
                    if (!row.next() || row.getInt(3) != 1)
                        throw new IllegalStateException("OLR handoff currently requires a single redo thread");
                    proof.put("floor_scn", row.getString(1));
                    proof.put("redo_sequence", row.getString(2));
                    proof.put("maximum_start_scn", Long.toString(Long.parseLong(proof.path("start_scn").asText()) - 1));
                    proof.put("start_scn", Long.toString(row.getLong(1) - 1));
                }
                try (PreparedStatement query = query(connection, "SELECT MIN(START_SCN) FROM GV$TRANSACTION");
                     ResultSet row = query.executeQuery())
                {
                    if (!row.next()) throw new IllegalStateException("Missing Oracle active transaction view");
                    String oldest = row.getString(1);
                    if (oldest != null && Long.parseLong(oldest) < Long.parseLong(proof.path("floor_scn").asText()))
                        throw new IllegalStateException("Retained online redo does not cover the oldest active transaction");
                    if (oldest != null)
                        proof.put("maximum_start_scn", Long.toString(Math.min(
                            Long.parseLong(proof.path("maximum_start_scn").asText()), Long.parseLong(oldest) - 1)));
                }
            }
            requireRedo(connection, proof);
            String table = "\"" + owner + "\".\"" + TABLE + "\"";
            boolean exists;
            try (PreparedStatement query = query(connection,
                "SELECT DEPENDENCIES FROM ALL_TABLES WHERE OWNER=? AND TABLE_NAME=?"))
            {
                query.setString(1, owner); query.setString(2, TABLE);
                try (ResultSet row = query.executeQuery())
                {
                    exists = row.next();
                    if (exists && !"ENABLED".equals(row.getString(1)))
                        throw new IllegalStateException("Existing OLR fence table lacks ROWDEPENDENCIES");
                }
            }
            if (!exists)
            {
                try (PreparedStatement create = query(connection, "CREATE TABLE " + table
                    + " (TOKEN VARCHAR2(36) PRIMARY KEY, SOURCE_DBID NUMBER(20) NOT NULL,"
                    + " RESETLOGS_SCN NUMBER(20) NOT NULL, CON_ID NUMBER(10) NOT NULL) ROWDEPENDENCIES"))
                { create.execute(); }
                // Oracle's AS OF metadata cannot safely span a fresh DDL in
                // the same timestamp/SCN mapping interval (ORA-01466).
                Thread.sleep(15000);
            }
            try (PreparedStatement query = query(connection,
                "SELECT COLUMN_NAME,DATA_TYPE FROM ALL_TAB_COLUMNS WHERE OWNER=? AND TABLE_NAME=? ORDER BY COLUMN_ID"))
            {
                query.setString(1, owner); query.setString(2, TABLE);
                String[] names = {"TOKEN", "SOURCE_DBID", "RESETLOGS_SCN", "CON_ID"};
                try (ResultSet rows = query.executeQuery())
                {
                    for (int index = 0; index < names.length; index++)
                        if (!rows.next() || !names[index].equals(rows.getString(1))
                            || !(index == 0 ? "VARCHAR2" : "NUMBER").equals(rows.getString(2)))
                            throw new IllegalStateException("Existing OLR fence table has an incompatible schema");
                    if (rows.next()) throw new IllegalStateException("Existing OLR fence table has unexpected columns");
                }
            }
            proof.put("token", UUID.randomUUID().toString());
            try (PreparedStatement insert = query(connection,
                "INSERT INTO " + table + " (TOKEN,SOURCE_DBID,RESETLOGS_SCN,CON_ID) VALUES (?,?,?,?)"))
            {
                insert.setString(1, proof.path("token").asText());
                insert.setLong(2, Long.parseLong(proof.path("dbid").asText()));
                insert.setLong(3, Long.parseLong(proof.path("resetlogs_scn").asText()));
                insert.setInt(4, Integer.parseInt(proof.path("con_id").asText()));
                if (insert.executeUpdate() != 1) throw new IllegalStateException("OLR fence was not committed");
            }
            try (PreparedStatement query = query(connection, "SELECT ORA_ROWSCN FROM " + table + " WHERE TOKEN=?"))
            {
                query.setString(1, proof.path("token").asText());
                try (ResultSet row = query.executeQuery())
                { row.next(); proof.put("fence_scn_upper_bound", row.getString(1)); }
            }
            return proof.toString();
        }
    }

    private static void requireRedo(Connection connection, JsonNode proof) throws SQLException
    {
        long first = Long.parseLong(proof.path("redo_sequence").asText());
        TreeSet<Long> sequences = new TreeSet<>();
        try (PreparedStatement query = query(connection,
            "SELECT SEQUENCE# FROM V$LOG WHERE THREAD#=1 UNION SELECT SEQUENCE# FROM V$ARCHIVED_LOG"
            + " WHERE THREAD#=1 AND RESETLOGS_CHANGE#=? AND DELETED='NO' AND STATUS='A' AND NAME IS NOT NULL"))
        {
            query.setLong(1, Long.parseLong(proof.path("resetlogs_scn").asText()));
            try (ResultSet rows = query.executeQuery())
            { while (rows.next()) if (rows.getLong(1) >= first) sequences.add(rows.getLong(1)); }
        }
        if (sequences.isEmpty() || sequences.first() != first)
            throw new IllegalStateException("OLR replay floor is no longer retained; explicit baseline recovery is required");
        long expected = first;
        for (long actual : sequences)
            if (actual != expected++) throw new IllegalStateException("Gap in retained OLR redo sequences");
    }

    private static PreparedStatement query(Connection connection, String sql) throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setQueryTimeout(120);
        return statement;
    }
}
