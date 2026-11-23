package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import junit.framework.TestCase;

public class FdwHandoffTest extends TestCase
{
    private static final ObjectMapper JSON = new ObjectMapper();

    private ObjectNode material(String offset) throws Exception
    {
        ObjectNode input = JSON.createObjectNode();
        input.set("offset", JSON.readTree(offset));
        input.putArray("history");
        return input;
    }

    public void testPostgresSeedUsesNativeOffsetEncoding() throws Exception
    {
        Path directory = Files.createTempDirectory("fdw-seed-");
        Path base = directory.resolve("generation");
        try
        {
            FdwHandoff.publish(5, "db", base, material("{\"lsn\":12345}"));
            Path offset = Path.of(base + "_offsets.dat");
            OffsetStore.requireResumePartition(5, "db", offset);
            assertEquals("{\"lsn\":12345}", OffsetStore.getFile(5, "db", offset));
            assertFalse(Files.exists(Path.of(base + "_schemahistory.dat")));
        }
        finally
        {
            Files.deleteIfExists(Path.of(base + "_offsets.dat"));
            Files.delete(directory);
        }
    }

    public void testExistingEngineOffsetIsNeverOverwritten() throws Exception
    {
        Path directory = Files.createTempDirectory("fdw-seed-");
        Path base = directory.resolve("generation");
        Path offset = Path.of(base + "_offsets.dat");
        try
        {
            FdwHandoff.publish(5, "db", base, material("{\"lsn\":12345}"));
            byte[] before = Files.readAllBytes(offset);
            try
            {
                FdwHandoff.publish(5, "db", base, material("{\"lsn\":99999}"));
                fail("Existing engine file was replaced");
            }
            catch (IllegalStateException expected) {}
            assertTrue(java.util.Arrays.equals(before, Files.readAllBytes(offset)));
        }
        finally
        {
            Files.deleteIfExists(offset);
            Files.delete(directory);
        }
    }

    public void testMysqlRequiresCompleteHistoryBeforeWritingOffset() throws Exception
    {
        Path directory = Files.createTempDirectory("fdw-seed-");
        Path base = directory.resolve("generation");
        try
        {
            try
            {
                FdwHandoff.publish(1, "db", base, material("{\"file\":\"bin.000001\",\"pos\":4}"));
                fail("MySQL seed without history was accepted");
            }
            catch (IllegalArgumentException expected) {}
            assertFalse(Files.exists(Path.of(base + "_offsets.dat")));
        }
        finally { Files.delete(directory); }
    }

    public void testSqlServerSeedAllowsRecoveryToBuildHistory() throws Exception
    {
        Path directory = Files.createTempDirectory("fdw-seed-");
        Path base = directory.resolve("generation");
        try
        {
            String offset = "{\"commit_lsn\":\"00000000:00000001:0001\",\"snapshot\":true,\"snapshot_completed\":false}";
            FdwHandoff.publish(3, "db", base, material(offset));
            OffsetStore.requireResumePartition(3, "db", Path.of(base + "_offsets.dat"));
            assertEquals(offset, OffsetStore.getFile(3, "db", Path.of(base + "_offsets.dat")));
            assertFalse(Files.exists(Path.of(base + "_schemahistory.dat")));
        }
        finally
        {
            Files.deleteIfExists(Path.of(base + "_offsets.dat"));
            Files.delete(directory);
        }
    }

    public void testMysqlPublishesCompleteHistoryAlongsideOffset() throws Exception
    {
        Path directory = Files.createTempDirectory("fdw-seed-");
        Path base = directory.resolve("generation");
        try
        {
            ObjectNode input = material("{\"file\":\"bin.000001\",\"pos\":123}");
            input.withArray("history").add(JSON.readTree("{\"source\":{\"server\":\"synchdb-connector\"},"
                + "\"position\":{\"file\":\"bin.000001\",\"pos\":123},\"databaseName\":\"db\","
                + "\"ddl\":\"CREATE TABLE t(id INT)\",\"tableChanges\":[]}"));
            FdwHandoff.publish(1, "db", base, input);
            ArtifactFiles.validateHistory(1, Path.of(base + "_schemahistory.dat"), true);
            assertEquals("{\"file\":\"bin.000001\",\"pos\":123}",
                OffsetStore.getFile(1, "db", Path.of(base + "_offsets.dat")));
        }
        finally
        {
            Files.deleteIfExists(Path.of(base + "_offsets.dat"));
            Files.deleteIfExists(Path.of(base + "_schemahistory.dat"));
            Files.delete(directory);
        }
    }

    public void testRecoveryIsExclusiveToExplicitSqlServerFdwOrigin()
    {
        Properties properties = new Properties();
        properties.setProperty("snapshot.mode", "recovery");
        SourceConsistency.configure(properties, 3, true);
        assertEquals("recovery", properties.getProperty("snapshot.mode"));
        for (int provider : new int[]{1,2,3,5})
        {
            Properties nativeProperties = new Properties();
            nativeProperties.setProperty("snapshot.mode", "recovery");
            try
            {
                SourceConsistency.configure(nativeProperties, provider);
                fail("Ordinary native recovery policy was loosened");
            }
            catch (IllegalArgumentException expected) {}
        }
    }
}
