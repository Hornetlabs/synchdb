package com.example;

import java.nio.file.Path;
import java.util.Properties;
import org.infinispan.commons.util.Version;

/** The on-disk layout used by the pinned Oracle LogMiner cache provider.
 * Keep the four legacy store names stable when the enclosing directory moves.
 * The provider also needs a rollbacks cache; legacy runners omitted that XML.
 */
final class InfinispanBuffers
{
    static final String VERSION = "15.2.1.Final";
    static final String[] LEGACY_STORES = {"events", "transactions", "processed", "schema"};

    private InfinispanBuffers() {}

    static void configure(Properties properties, Path directory, String memoryType, int memorySize)
    {
        if (!VERSION.equals(Version.getVersion()))
            throw new IllegalStateException("Oracle persistent caches require Infinispan " + VERSION);
        if (memorySize <= 0)
            throw new IllegalArgumentException("Infinispan memory size must be positive");
        // Preserve the original runner's interpretation of the public option.
        String storage = "off heap".equals(memoryType) ? "OFF_HEAP" : "HEAP";
        properties.setProperty("log.mining.buffer.type", "infinispan_embedded");
        properties.setProperty("log.mining.buffer.drop.on.stop", "false");
        properties.setProperty("log.mining.buffer.infinispan.cache.global",
            "<infinispan xmlns=\"urn:infinispan:config:14.0\"><threads>"
            + "<blocking-bounded-queue-thread-pool name=\"myexec\" max-threads=\"10\" keepalive-time=\"10000\" queue-length=\"5000\"/>"
            + "</threads></infinispan>");
        configureCache(properties, directory, "events", "events", "events", storage, memorySize);
        configureCache(properties, directory, "transactions", "transactions", "transactions", storage, memorySize);
        configureCache(properties, directory, "processed_transactions", "processed-transactions", "processed", storage, memorySize);
        configureCache(properties, directory, "schema_changes", "schema-changes", "schema", storage, memorySize);
        // A new optional store does not change the four legacy store formats.
        // On migration it starts empty if the old runner never persisted it.
        configureCache(properties, directory, "rollbacks", "rollbacks", "rollbacks", storage, memorySize);
    }

    private static void configureCache(Properties properties, Path directory, String property,
                                       String cache, String store, String storage, int memorySize)
    {
        String base = escape(directory.resolve(store).toString());
        properties.setProperty("log.mining.buffer.infinispan.cache." + property,
            "<local-cache name=\"" + cache + "\"><encoding>"
            + "<key media-type=\"application/x-protostream\"/><value media-type=\"application/x-protostream\"/>"
            + "</encoding><memory storage=\"" + storage + "\" max-size=\"" + memorySize + "MB\" when-full=\"REMOVE\"/>"
            + "<persistence passivation=\"true\"><file-store read-only=\"false\" preload=\"false\" shared=\"false\">"
            + "<data path=\"" + base + "/data\"/><index path=\"" + base + "/index\"/><write-behind/>"
            + "</file-store></persistence></local-cache>");
    }

    private static String escape(String value)
    {
        return value.replace("&", "&amp;").replace("\"", "&quot;")
            .replace("<", "&lt;").replace(">", "&gt;").replace("'", "&apos;");
    }
}
