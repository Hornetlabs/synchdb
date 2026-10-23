package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Properties;
import junit.framework.TestCase;

public class OlrSnapshotBoundaryTest extends TestCase
{
    public void testCompletionPreservesTransactionsOpenAtReadView() throws Exception
    {
        OlrSnapshotBoundary boundary = new OlrSnapshotBoundary();
        boundary.acceptOffset(Map.of("snapshot_scn", "1000",
            "snapshot_pending_tx", "07000d009d070000:850"));
        // Native postSnapshotCompletion drops snapshot_pending_tx. It must
        // not erase the witness needed for late commits with earlier SCNs.
        boundary.acceptOffset(Map.of("snapshot_scn", "1000"));
        var completed = new ObjectMapper().readTree(boundary.completed());
        assertEquals("1000", completed.path("scn").asText());
        assertEquals("0x0007.00d.0000079d", completed.path("snapshot_pending_xids").get(0).asText());
    }

    public void testChangedReadViewFails()
    {
        OlrSnapshotBoundary boundary = new OlrSnapshotBoundary();
        boundary.acceptOffset(Map.of("snapshot_scn", "1000"));
        try
        {
            boundary.acceptOffset(Map.of("snapshot_scn", "1001"));
            fail("two source read views were accepted");
        }
        catch (IllegalStateException expected) {}
    }

    public void testOlrProducerIsFiniteAndTransactionAware()
    {
        Properties properties = new Properties();
        properties.setProperty("snapshot.mode", "initial");
        SourceConsistency.configure(properties, 4);
        assertEquals("initial_only", properties.getProperty("snapshot.mode"));
        assertEquals("all", properties.getProperty("internal.log.mining.transaction.snapshot.boundary.mode"));
        properties = new Properties();
        properties.setProperty("snapshot.mode", "no_data");
        SourceConsistency.configure(properties, 4);
        assertEquals("synchdb_olr_schema", properties.getProperty("snapshot.mode.custom.name"));
        assertFalse(new OlrSchemaSnapshotter().shouldStream());
    }
}
