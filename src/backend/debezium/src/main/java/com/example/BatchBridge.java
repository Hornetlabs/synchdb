package com.example;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntSupplier;
import java.util.function.Predicate;

/** One callback owns its records and committer until source settlement ends. */
final class BatchBridge {
    private static final int DEFAULT_MAX_FRAME_BYTES = 64 * 1024 * 1024;
    private static final int DEFAULT_MAX_RECORDS = 1_000_000;
    private static final long DEFAULT_ACK_TIMEOUT_MILLIS = 30_000;
    private static final int HEADER_BYTES = 1 + Integer.BYTES + Integer.BYTES;

    /** Identifies cancellation before any source acknowledgement was accepted. */
    static final class StopCancelledException extends InterruptedException {
        private static final long serialVersionUID = 1L;

        private StopCancelledException(String message) {
            super(message);
        }
    }

    private enum State {
        ENCODING, READY, DELIVERED, ACK_REQUESTED, ACKING, ACKED,
        CANCELLED, FAILED, ACK_UNKNOWN
    }

    private static final class Batch {
        private State state = State.ENCODING;
        private int id = -1;
        private ByteBuffer frame;
    }

    private final Object monitor = new Object();
    private final IntSupplier nextId;
    private final int maxFrameBytes;
    private final int maxRecords;
    private final long ackTimeoutNanos;
    private Batch slot;
    private int callbacks;
    private boolean stopping;
    private Throwable failure;
    private int lastAllocatedId = -1;
    private int lastSuccessfulId = -1;

    BatchBridge(IntSupplier nextId) {
        this(nextId, DEFAULT_MAX_FRAME_BYTES, DEFAULT_MAX_RECORDS,
                DEFAULT_ACK_TIMEOUT_MILLIS);
    }

    BatchBridge(IntSupplier nextId, int maxFrameBytes, int maxRecords,
                long ackTimeoutMillis) {
        this.nextId = Objects.requireNonNull(nextId, "nextId");
        if (maxFrameBytes < HEADER_BYTES || maxRecords <= 0 || ackTimeoutMillis <= 0) {
            throw new IllegalArgumentException("Invalid batch frame, record or acknowledgement limit");
        }
        this.maxFrameBytes = maxFrameBytes;
        this.maxRecords = maxRecords;
        this.ackTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(ackTimeoutMillis);
    }

    void handleBatch(List<ChangeEvent<String, String>> records,
                     DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer)
            throws InterruptedException {
        handleBatch(records,committer,record -> true);
    }

    void handleBatch(List<ChangeEvent<String, String>> records,
                     DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer,
                     Predicate<ChangeEvent<String,String>> includeInFrame)
            throws InterruptedException {
        Objects.requireNonNull(includeInFrame,"includeInFrame");
        Batch batch = null;
        synchronized (monitor) {
            callbacks++;
        }
        try {
            synchronized (monitor) {
                while (slot != null && failure == null && !stopping) {
                    monitor.wait();
                }
                requireHealthy();
                if (stopping) {
                    throw new StopCancelledException("Batch session is stopping");
                }
                batch = new Batch();
                slot = batch;
            }

            if (records == null || committer == null) {
                throw new IllegalArgumentException("Batch records and committer must not be null");
            }
            if (records.size() > maxRecords) {
                throw new IllegalArgumentException("Source batch exceeds the raw record limit");
            }
            // Copy the container, preserving every original object, including tombstones.
            List<ChangeEvent<String, String>> owned = new ArrayList<>(records);
            if (owned.size() > maxRecords) {
                throw new IllegalArgumentException("Source batch exceeds the raw record limit");
            }
            ByteBuffer frame = encode(owned, batch, includeInFrame);
            int id = nextId.getAsInt();
            synchronized (monitor) {
                requireHealthy();
                if (stopping || batch.state == State.CANCELLED) {
                    throw new StopCancelledException("Batch session is stopping");
                }
                if (id < 0 || id <= lastAllocatedId) {
                    throw new IllegalStateException("Batch identifiers must increase without wrapping");
                }
                lastAllocatedId = id;
                batch.id = id;
                frame.putInt(1, id);
                batch.frame = frame;
                batch.state = State.READY;
                monitor.notifyAll();

                while (batch.state == State.READY || batch.state == State.DELIVERED) {
                    monitor.wait();
                }
                requireHealthy();
                if (batch.state == State.CANCELLED) {
                    throw new StopCancelledException("Unacknowledged batch was cancelled during stop");
                }
                if (batch.state != State.ACK_REQUESTED) {
                    throw new IllegalStateException("Batch has no accepted acknowledgement request");
                }
                // An accepted request takes precedence over a concurrent normal stop.
                batch.state = State.ACKING;
            }

            // The callback thread alone touches this committer. Never hold the monitor
            // across connector calls, offset flushes or callback-owned record methods.
            for (ChangeEvent<String, String> record : owned) {
                requireAckInProgress(batch);
                committer.markProcessed(record);
            }
            requireAckInProgress(batch);
            committer.markBatchFinished();
            synchronized (monitor) {
                requireHealthy();
                if (batch.state != State.ACKING) {
                    throw new IllegalStateException("Batch acknowledgement result is unknown");
                }
                batch.state = State.ACKED;
                lastSuccessfulId = batch.id;
                monitor.notifyAll();
            }
        }
        catch (InterruptedException interrupted) {
            synchronized (monitor) {
                boolean cancelledByStop = stopping &&
                        (batch == null || batch.state == State.CANCELLED ||
                                batch.state == State.ENCODING);
                if (!cancelledByStop) {
                    failLocked(interrupted);
                }
                monitor.notifyAll();
            }
            throw interrupted;
        }
        catch (RuntimeException | Error error) {
            fail(error);
            throw error;
        }
        finally {
            synchronized (monitor) {
                if (batch != null && slot == batch) {
                    batch.frame = null;
                    slot = null;
                }
                callbacks--;
                monitor.notifyAll();
            }
        }
    }

    /** The cap limits wire payload, not source records or all transient heap use. */
    private ByteBuffer encode(List<ChangeEvent<String, String>> records, Batch batch,
                              Predicate<ChangeEvent<String,String>> includeInFrame)
            throws InterruptedException {
        List<byte[]> payloads = new ArrayList<>();
        long bytes = HEADER_BYTES;
        for (ChangeEvent<String, String> record : records) {
            checkEncodingStop(batch);
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Interrupted while encoding a source batch");
            }
            if (record == null) {
                throw new IllegalArgumentException("Source batch contains a null record");
            }
            if (!includeInFrame.test(record)) {
                continue;
            }
            String value = record.value();
            checkEncodingStop(batch);
            if (value == null) {
                continue;
            }
            // UTF-16 length is a lower bound for this UTF-8 encoding, including
            // replacement of malformed surrogates. Reject impossible sizes first.
            if (value.length() > maxFrameBytes ||
                    bytes + Integer.BYTES + (long) value.length() + 1 > maxFrameBytes) {
                throw new IllegalArgumentException("Source batch exceeds the wire byte limit");
            }
            byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
            bytes += Integer.BYTES + (long) encoded.length + 1;
            if (bytes > maxFrameBytes) {
                throw new IllegalArgumentException("Source batch exceeds the wire byte limit");
            }
            if (encoded.length == 0) {
                throw new IllegalArgumentException("Source record must not be empty");
            }
            for (byte octet : encoded) {
                if (octet == 0) {
                    throw new IllegalArgumentException("Source record contains an embedded NUL");
                }
            }
            payloads.add(encoded);
        }

        checkEncodingStop(batch);
        ByteBuffer frame = ByteBuffer.allocateDirect((int) bytes);
        frame.put((byte) 'B');
        frame.putInt(-1); // Filled with the allocated source token before publication.
        frame.putInt(payloads.size());
        for (byte[] payload : payloads) {
            frame.putInt(payload.length + 1);
            frame.put(payload);
            frame.put((byte) 0);
        }
        frame.flip();
        return frame;
    }

    private void checkEncodingStop(Batch batch) throws StopCancelledException {
        synchronized (monitor) {
            requireHealthy();
            if (stopping && batch.state == State.CANCELLED) {
                throw new StopCancelledException("Source batch encoding was cancelled during stop");
            }
        }
    }

    static boolean causesStopCancellation(Throwable cause) {
        // A bounded walk also handles arbitrary cyclic cause chains without retaining
        // the engine's Throwable graph beyond this check.
        for (int depth = 0; cause != null && depth < 32; depth++) {
            if (cause instanceof StopCancelledException) {
                return true;
            }
            Throwable next = cause.getCause();
            if (next == cause) {
                break;
            }
            cause = next;
        }
        return false;
    }

    ByteBuffer poll() {
        synchronized (monitor) {
            requireHealthy();
            if (slot == null) {
                return null;
            }
            if (slot.state == State.READY) {
                slot.state = State.DELIVERED;
                return slot.frame;
            }
            if (slot.state == State.DELIVERED || slot.state == State.ACK_REQUESTED ||
                    slot.state == State.ACKING) {
                throw new IllegalStateException("A batch is already delivered and unsettled");
            }
            return null;
        }
    }

    void acknowledge(int id, boolean all, int from, int to) throws InterruptedException {
        synchronized (monitor) {
            requireHealthy();
            if (!all) {
                throw new IllegalArgumentException("Partial source batch acknowledgement is unsupported");
            }
            if (id >= 0 && id == lastSuccessfulId) {
                return;
            }
            if (slot == null || id < 0 || slot.id != id) {
                throw new IllegalArgumentException("Batch id is not the current delivered batch");
            }
            if (stopping || slot.state != State.DELIVERED) {
                throw new IllegalStateException("Batch is not ready for acknowledgement");
            }

            Batch batch = slot;
            batch.state = State.ACK_REQUESTED;
            monitor.notifyAll();
            long started = System.nanoTime();
            while (true) {
                requireHealthy();
                if (batch.state == State.ACKED) {
                    return;
                }
                long remaining = ackTimeoutNanos - (System.nanoTime() - started);
                if (remaining <= 0) {
                    TimeoutException timeout = new TimeoutException("Timed out waiting for source batch acknowledgement");
                    batch.state = State.ACK_UNKNOWN;
                    failLocked(timeout);
                    throw failedException();
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(monitor, remaining);
                }
                catch (InterruptedException interrupted) {
                    batch.state = State.ACK_UNKNOWN;
                    failLocked(interrupted);
                    throw interrupted;
                }
            }
        }
    }

    void requestStop() {
        synchronized (monitor) {
            stopping = true;
            if (slot != null && (slot.state == State.ENCODING ||
                    slot.state == State.READY || slot.state == State.DELIVERED)) {
                slot.state = State.CANCELLED;
            }
            monitor.notifyAll();
        }
    }

    void fail(Throwable cause) {
        Throwable actual = cause == null ?
                new IllegalStateException("Source batch session failed") : cause;
        synchronized (monitor) {
            failLocked(actual);
        }
    }

    private void failLocked(Throwable cause) {
        if (failure == null) {
            failure = cause;
        }
        if (slot != null && slot.state != State.ACKED && slot.state != State.ACK_UNKNOWN) {
            slot.state = slot.state == State.ACKING ? State.ACK_UNKNOWN : State.FAILED;
        }
        monitor.notifyAll();
    }

    private void requireAckInProgress(Batch batch) {
        synchronized (monitor) {
            requireHealthy();
            if (batch.state != State.ACKING) {
                throw new IllegalStateException("Batch acknowledgement is no longer active");
            }
        }
    }

    private void requireHealthy() {
        if (failure != null) {
            throw failedException();
        }
    }

    private IllegalStateException failedException() {
        return new IllegalStateException("Source batch session failed", failure);
    }

    boolean callbacksQuiescent() {
        synchronized (monitor) {
            return callbacks == 0;
        }
    }

    boolean hasPending() {
        synchronized (monitor) {
            return slot != null || callbacks != 0;
        }
    }

    boolean failed() {
        synchronized (monitor) {
            return failure != null;
        }
    }

    String failureMessage() {
        Throwable cause;
        synchronized (monitor) {
            cause = failure;
        }
        return cause == null ? "" : cause.toString();
    }

    boolean restartable() {
        return !failed();
    }
}
