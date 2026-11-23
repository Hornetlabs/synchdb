package com.example;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.TestCase;

/** Source settlement of rowless TRUNCATE, filtered records and failed batches. */
public class BatchBridgeTest extends TestCase {
    private static final class Event implements ChangeEvent<String, String> {
        private final String value;
        Event(String value) { this.value = value; }
        public String key() { return null; }
        public String value() { return value; }
        public String destination() { return "test.items"; }
        public Integer partition() { return 0; }
    }

    private static final class Committer implements DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> {
        final List<ChangeEvent<String, String>> processed = new ArrayList<>();
        final AtomicInteger calls = new AtomicInteger();
        int finished;
        int failAt = -1;
        Thread owner;

        public void markProcessed(ChangeEvent<String, String> record) {
            owner = Thread.currentThread();
            if (calls.incrementAndGet() == failAt) {
                throw new IllegalStateException("injected source acknowledgement failure");
            }
            processed.add(record);
        }

        public void markProcessed(ChangeEvent<String, String> record, DebeziumEngine.Offsets offsets) {
            markProcessed(record);
        }

        public void markBatchFinished() {
            assertSame(owner, Thread.currentThread());
            finished++;
        }

        public DebeziumEngine.Offsets buildOffsets() {
            throw new UnsupportedOperationException("not used by this source protocol");
        }
    }

    private static final class Callback implements AutoCloseable {
        final BatchBridge bridge;
        final FutureTask<Void> result;
        final Thread thread;

        Callback(BatchBridge bridge, List<ChangeEvent<String, String>> records,
                 Committer committer, boolean include) {
            this.bridge = bridge;
            result = new FutureTask<>(() -> {
                bridge.handleBatch(records, committer, record -> include);
                return null;
            });
            thread = new Thread(result, "test-source-callback");
            thread.setDaemon(true);
            thread.start();
        }

        ByteBuffer poll() throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline) {
                ByteBuffer frame = bridge.poll();
                if (frame != null) {
                    return frame;
                }
                if (result.isDone()) {
                    result.get();
                    fail("source callback ended before delivering its batch");
                }
                Thread.sleep(1);
            }
            throw new AssertionError("source batch was not delivered");
        }

        Throwable failure() throws Exception {
            try {
                result.get(3, TimeUnit.SECONDS);
                throw new AssertionError("source callback unexpectedly succeeded");
            } catch (ExecutionException expected) {
                return expected.getCause();
            }
        }

        public void close() throws Exception {
            bridge.requestStop();
            thread.join(3000);
            assertFalse("source callback did not stop", thread.isAlive());
        }
    }

    private static BatchBridge bridge() {
        return new BatchBridge(new AtomicInteger()::getAndIncrement);
    }

    public void testTruncateAndTombstoneAreSettledOnTheCallbackThread() throws Exception {
        BatchBridge bridge = bridge();
        Committer committer = new Committer();
        List<ChangeEvent<String, String>> records = List.of(
            new Event("{\"payload\":{\"op\":\"t\"}}"), new Event(null));
        try (Callback callback = new Callback(bridge, records, committer, true)) {
            ByteBuffer frame = callback.poll();
            assertEquals('B', frame.get(0));
            assertEquals(1, frame.getInt(5)); // Tombstone has no target event.
            assertEquals(0, committer.calls.get());
            int id = frame.getInt(1);
            bridge.acknowledge(id, true, -1, -1);
            callback.result.get(3, TimeUnit.SECONDS);
            assertEquals(records, committer.processed);
            assertSame(callback.thread, committer.owner);
            assertEquals(1, committer.finished);
            bridge.acknowledge(id, true, -1, -1);
            assertEquals(2, committer.calls.get());
            assertEquals(1, committer.finished);
        }
    }

    public void testRejectedPartialOrWrongIdentityDoesNotSettleTheBatch() throws Exception {
        BatchBridge bridge = bridge();
        Committer committer = new Committer();
        try (Callback callback = new Callback(bridge, List.of(new Event("truncate")), committer, true)) {
            int id = callback.poll().getInt(1);
            try {
                bridge.acknowledge(id, false, 0, 0);
                fail("partial acknowledgement was accepted");
            } catch (IllegalArgumentException expected) { }
            try {
                bridge.acknowledge(id + 1, true, -1, -1);
                fail("wrong batch identity was accepted");
            } catch (IllegalArgumentException expected) { }
            assertEquals(0, committer.calls.get());
            bridge.acknowledge(id, true, -1, -1);
            callback.result.get(3, TimeUnit.SECONDS);
            assertEquals(1, committer.finished);
        }
    }

    public void testStopBeforeTargetCommitDoesNotAcknowledge() throws Exception {
        BatchBridge bridge = bridge();
        Committer committer = new Committer();
        try (Callback callback = new Callback(bridge, List.of(new Event("truncate")), committer, true)) {
            callback.poll();
            bridge.requestStop();
            assertTrue(BatchBridge.causesStopCancellation(callback.failure()));
            assertEquals(0, committer.calls.get());
            assertFalse(bridge.failed());
            assertTrue(bridge.callbacksQuiescent());
        }
    }

    public void testTargetFailureCancelsTheUnacknowledgedTruncate() throws Exception {
        BatchBridge bridge = bridge();
        Committer committer = new Committer();
        try (Callback callback = new Callback(bridge, List.of(new Event("truncate")), committer, true)) {
            callback.poll();
            bridge.fail(new IllegalStateException("target batch rolled back"));
            assertTrue(callback.failure() instanceof IllegalStateException);
            assertEquals(0, committer.calls.get());
            assertTrue(bridge.failed());
            assertFalse(bridge.restartable());
            try {
                bridge.poll();
                fail("failed session delivered another batch");
            } catch (IllegalStateException expected) { }
        }
    }

    public void testSourceAcknowledgementFailureFreezesTheSession() throws Exception {
        BatchBridge bridge = bridge();
        Committer committer = new Committer();
        committer.failAt = 2;
        try (Callback callback = new Callback(bridge,
                List.of(new Event("truncate"), new Event("following insert")), committer, true)) {
            int id = callback.poll().getInt(1);
            try {
                bridge.acknowledge(id, true, -1, -1);
                fail("source acknowledgement failure was hidden");
            } catch (IllegalStateException expected) { }
            assertTrue(callback.failure() instanceof IllegalStateException);
            assertEquals(1, committer.processed.size());
            assertEquals(0, committer.finished);
            assertFalse(bridge.restartable());
            try {
                bridge.acknowledge(id, true, -1, -1);
                fail("partially acknowledged source batch was acknowledged again");
            } catch (IllegalStateException expected) { }
            assertEquals(2, committer.calls.get());
        }
    }

    public void testFilteredBatchStillSettlesEveryOriginalRecord() throws Exception {
        BatchBridge bridge = bridge();
        Committer committer = new Committer();
        List<ChangeEvent<String, String>> records = List.of(new Event("truncate"), new Event(null));
        try (Callback callback = new Callback(bridge, records, committer, false)) {
            ByteBuffer frame = callback.poll();
            assertEquals(0, frame.getInt(5));
            assertEquals(0, committer.calls.get());
            bridge.acknowledge(frame.getInt(1), true, -1, -1);
            callback.result.get(3, TimeUnit.SECONDS);
            assertEquals(records, committer.processed);
            assertEquals(1, committer.finished);
        }
    }
}
