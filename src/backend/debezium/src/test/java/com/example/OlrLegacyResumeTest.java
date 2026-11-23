package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import junit.framework.TestCase;

public class OlrLegacyResumeTest extends TestCase
{
    private static final ObjectMapper JSON = new ObjectMapper();

    private ObjectNode identity()
    {
        ObjectNode proof = JSON.createObjectNode();
        proof.put("version", 1); proof.put("owner", "DBZUSER");
        proof.put("dbid", "123"); proof.put("resetlogs_scn", "456"); proof.put("con_id", "0");
        return proof;
    }

    private ObjectNode seed()
    {
        ObjectNode seed = JSON.createObjectNode();
        seed.put("protocol", OlrHandoff.LEGACY_PROTOCOL);
        seed.put("identity_verified", false);
        seed.put("legacy_scn", "777"); seed.put("legacy_c_scn", "700"); seed.put("legacy_c_idx", "0");
        return seed;
    }

    public void testImportedFileBindsCurrentIdentityWithoutInventingSnapshotProof()
    {
        ObjectNode proof = identity();
        OlrHandoff.inheritPrevious(proof, seed());
        assertEquals("123", proof.path("dbid").asText());
        assertEquals("777", proof.path("legacy_scn").asText());
        assertEquals("700", proof.path("legacy_c_scn").asText());
        assertEquals("0", proof.path("legacy_c_idx").asText());
        assertEquals(OlrHandoff.LEGACY_PROTOCOL, proof.path("protocol").asText());
        assertFalse(proof.path("handoff_verified").asBoolean());
        assertFalse(proof.path("legacy_source_identity_verified").asBoolean());
        assertFalse(proof.has("snapshot_pending_xids"));
        assertFalse(proof.has("scn"));
    }

    public void testSubsequentLegacyResumeRejectsAnotherDatabase()
    {
        ObjectNode previous = identity();
        OlrHandoff.inheritPrevious(previous, seed());
        previous.put("identity_verified", true);
        ObjectNode changed = identity(); changed.put("dbid", "999");
        try
        {
            OlrHandoff.inheritPrevious(changed, previous);
            fail("Bound legacy cursor accepted another Oracle source");
        }
        catch (IllegalStateException expected) { assertTrue(expected.getMessage().contains("dbid")); }
    }

    public void testSubsequentLegacyResumeRejectsResetlogs()
    {
        ObjectNode previous = identity();
        OlrHandoff.inheritPrevious(previous, seed());
        previous.put("identity_verified", true);
        ObjectNode changed = identity(); changed.put("resetlogs_scn", "999");
        try
        {
            OlrHandoff.inheritPrevious(changed, previous);
            fail("Bound legacy cursor accepted another incarnation");
        }
        catch (IllegalStateException expected) { assertTrue(expected.getMessage().contains("resetlogs_scn")); }
    }

    public void testOnlyExplicitUnboundLegacySeedCanOmitIdentity()
    {
        for (String damaged : new String[]{"protocol", "identity_verified", "legacy_c_scn"})
        {
            ObjectNode seed = seed(); seed.remove(damaged);
            try
            {
                OlrHandoff.inheritPrevious(identity(), seed);
                fail("Incomplete imported state was accepted: " + damaged);
            }
            catch (IllegalStateException | IllegalArgumentException expected) {}
        }
        ObjectNode partiallyBound = seed(); partiallyBound.put("dbid", "123");
        try
        {
            OlrHandoff.inheritPrevious(identity(), partiallyBound);
            fail("A partially bound identity was treated as an unbound seed");
        }
        catch (IllegalStateException expected) {}
    }

    public void testNativeResumeStillPreservesItsSnapshotReadView() throws Exception
    {
        ObjectNode previous = identity();
        previous.put("scn", "888"); previous.putArray("snapshot_pending_xids").add("a.b.c");
        ObjectNode proof = identity();
        OlrHandoff.inheritPrevious(proof, previous);
        assertEquals("888", proof.path("scn").asText());
        assertEquals(previous.path("snapshot_pending_xids"), proof.path("snapshot_pending_xids"));
        assertFalse(proof.has("protocol"));
    }

    /** Model only the two catalog queries used to locate a retained replay log. */
    private Connection redoConnection(int threads, String floor, String sequence, Map<Integer, Long> parameters)
    {
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
            (connection, method, arguments) -> {
                if (method.getName().equals("prepareStatement"))
                {
                    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{PreparedStatement.class},
                        (statement, operation, values) -> {
                            if (operation.getName().equals("setLong"))
                            { parameters.put((Integer) values[0], (Long) values[1]); return null; }
                            if (operation.getName().equals("executeQuery"))
                            {
                                boolean[] seen = {false};
                                return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{ResultSet.class},
                                    (result, getter, column) -> {
                                        if (getter.getName().equals("next"))
                                        { boolean first = !seen[0]; seen[0] = true; return first; }
                                        if (getter.getName().equals("getInt")) return threads;
                                        if (getter.getName().equals("close")) return null;
                                        String value = (Integer) column[0] == 1 ? floor : sequence;
                                        if (getter.getName().equals("getString")) return value;
                                        if (getter.getName().equals("getLong")) return value == null ? 0L : Long.parseLong(value);
                                        return null;
                                    });
                            }
                            return null;
                        });
                }
                return null;
            });
    }

    private ObjectNode resume()
    {
        ObjectNode request = JSON.createObjectNode();
        request.put("resume_scn", "777"); request.put("resume_c_scn", "700"); request.put("resume_c_idx", "0");
        return request;
    }

    public void testResumeLocatesRedoForCurrentCursorIncludingZeroIndex() throws Exception
    {
        ObjectNode proof = identity();
        Map<Integer, Long> parameters = new HashMap<>();
        OlrHandoff.prepareLegacyReplay(redoConnection(1, "500", "9", parameters), proof, resume());
        assertEquals(Long.valueOf(456), parameters.get(1));
        assertEquals(Long.valueOf(700), parameters.get(2));
        assertEquals(Long.valueOf(700), parameters.get(3));
        assertEquals("499", proof.path("start_scn").asText());
        assertEquals("9", proof.path("redo_sequence").asText());
        assertEquals("777", proof.path("olr_first_data_scn").asText());
    }

    public void testMissingLegacyRedoIsRejected() throws Exception
    {
        try
        {
            OlrHandoff.prepareLegacyReplay(redoConnection(1, null, null, new HashMap<>()), identity(), resume());
            fail("A missing historical replay log was accepted");
        }
        catch (IllegalStateException expected) { assertTrue(expected.getMessage().contains("no longer retained")); }
    }

    public void testMultipleRedoThreadsAreRejected() throws Exception
    {
        try
        {
            OlrHandoff.prepareLegacyReplay(redoConnection(2, "500", "9", new HashMap<>()), identity(), resume());
            fail("Multiple redo threads were accepted");
        }
        catch (IllegalStateException expected) { assertTrue(expected.getMessage().contains("single redo thread")); }
    }

    public void testInvalidLegacyCursorCannotBecomeAReplayFloor() throws Exception
    {
        for (String invalid : new String[]{"-1", "0", "9223372036854775808", "abc"})
        {
            ObjectNode request = resume(); request.put("resume_c_scn", invalid);
            try
            {
                OlrHandoff.prepareLegacyReplay(redoConnection(1, "500", "9", new HashMap<>()), identity(), request);
                fail("Invalid cursor was accepted: " + invalid);
            }
            catch (IllegalArgumentException expected) {}
        }
    }
}
