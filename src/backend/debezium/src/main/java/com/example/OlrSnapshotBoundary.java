package com.example;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.Scn;
import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.engine.ChangeEvent;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Preserve the native snapshot's transaction-aware boundary before normal
 * completion removes snapshot_pending_tx from subsequent offset maps. */
final class OlrSnapshotBoundary
{
    private String scn;
    private final Set<String> pending = new LinkedHashSet<>();

    synchronized void observe(List<ChangeEvent<String,String>> records)
    {
        for (ChangeEvent<String,String> record : records)
        {
            if (!(record instanceof EmbeddedEngineChangeEvent<?,?,?> nativeRecord))
                throw new IllegalStateException("OLR snapshot has no native source offset metadata");
            Map<String,?> offset = nativeRecord.sourceRecord().sourceOffset();
            acceptOffset(offset);
        }
    }

    synchronized void acceptOffset(Map<String,?> offset)
    {
            Object view = offset.get("snapshot_scn");
            if (view == null) return;
            String current = view.toString();
            if (!current.matches("[1-9][0-9]*") || (scn != null && !scn.equals(current)))
                throw new IllegalStateException("OLR snapshot changed its native read view");
            scn = current;
            for (Map.Entry<String,Scn> transaction : OracleOffsetContext.loadSnapshotPendingTransactions(offset).entrySet())
                pending.add(olrXid(transaction.getKey()));
    }

    static String olrXid(String raw)
    {
        String hex = raw.startsWith("0x") ? raw.substring(2) : raw;
        if (!hex.matches("[0-9a-fA-F]{16}"))
            throw new IllegalStateException("Invalid Oracle snapshot transaction identity");
        byte[] bytes = java.util.HexFormat.of().parseHex(hex);
        java.nio.ByteBuffer value = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        return String.format(java.util.Locale.ROOT, "0x%04x.%03x.%08x",
            Short.toUnsignedInt(value.getShort()), Short.toUnsignedInt(value.getShort()),
            Integer.toUnsignedLong(value.getInt()));
    }

    synchronized String completed()
    {
        if (scn == null)
            throw new IllegalStateException("OLR snapshot completed without a native read-view witness");
        ObjectNode result = new ObjectMapper().createObjectNode();
        result.put("scn", scn);
        var transactions = result.putArray("snapshot_pending_xids");
        pending.forEach(transactions::add);
        return result.toString();
    }
}
