package com.example;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Resources and completion belong to one start, including its asynchronous close. */
final class EngineSession {
    private static final int PENDING = 0;
    private static final int CLOSED = 1;
    private static final int FAILED = 2;
    private static final String ASYNC_ENGINE = "io.debezium.embedded.async.AsyncEmbeddedEngine";
    private static final String STARTING_TASKS =
            "Cannot stop engine while tasks are starting, this may lead to leaked resource. Wait for the tasks to be fully started.";
    private static final String ALREADY_STOPPING = "Engine is already being shutting down.";
    private static final String ALREADY_STOPPED = "Engine has been already shut down.";
    private static final String CHANGE_STOPPING = "Cannot change engine state to 'STOPPING' as the engine is not in expected state '";

    private final BatchBridge bridge;
    private DebeziumEngine<ChangeEvent<String, String>> engine;
    private ExecutorService executor;
    private Future<?> future;
    private boolean startInProgress = true;
    private boolean runEntered;
    private boolean stopRequested;
    private boolean completionSeen;
    private boolean completionSuccess;
    private String completionMessage = "";
    private Throwable failure;
    private int closeStatus = PENDING;
    private boolean lifecycleRequired;
    private boolean connectorStarted;
    private boolean connectorStopped;
    private boolean taskStarted;
    private boolean taskStopped;

    EngineSession(BatchBridge bridge) {
        this.bridge = Objects.requireNonNull(bridge, "bridge");
    }

    /* The pinned async engine can log a failed/timed-out SourceTask.stop()
     * and still report successful engine completion. Its taskStopped callback
     * runs only after that task's stop Future returns successfully. Require
     * that positive observation before admitting a new engine or offset edit.
     * Startup can stop before either resource started; it needs no fabricated
     * stop notification and never waits indefinitely for one. */
    synchronized DebeziumEngine.ConnectorCallback connectorCallback() {
        lifecycleRequired = true;
        return new DebeziumEngine.ConnectorCallback() {
            @Override
            public void connectorStarted() {
                synchronized (EngineSession.this) {
                    connectorStarted = true;
                    connectorStopped = false;
                    EngineSession.this.notifyAll();
                }
            }

            @Override
            public void connectorStopped() {
                synchronized (EngineSession.this) {
                    connectorStopped = true;
                    EngineSession.this.notifyAll();
                }
            }

            @Override
            public void taskStarted() {
                synchronized (EngineSession.this) {
                    taskStarted = true;
                    taskStopped = false;
                    EngineSession.this.notifyAll();
                }
            }

            @Override
            public void taskStopped() {
                synchronized (EngineSession.this) {
                    taskStopped = true;
                    EngineSession.this.notifyAll();
                }
            }
        };
    }

    /* Caller holds this session's monitor. Only inspect this after the engine
     * Future and callbacks have settled; absence while running is ordinary. */
    private IllegalStateException unconfirmedLifecycle() {
        if (lifecycleRequired && taskStarted && !taskStopped) {
            return new IllegalStateException("Native source task shutdown was not confirmed by its lifecycle callback");
        }
        if (lifecycleRequired && connectorStarted && !connectorStopped) {
            return new IllegalStateException("Native source connector shutdown was not confirmed by its lifecycle callback");
        }
        return null;
    }

    void start(DebeziumEngine<ChangeEvent<String, String>> nextEngine) {
        try {
            synchronized (this) {
                if (!startInProgress || engine != null) {
                    throw new IllegalStateException("Engine session has already been started");
                }
                // Retain the engine even if executor creation or submit subsequently fails.
                engine = Objects.requireNonNull(nextEngine, "engine");
                executor = Executors.newSingleThreadExecutor();
                future = executor.submit(this::run);
                startInProgress = false;
                notifyAll();
            }
        }
        catch (RuntimeException | Error error) {
            startFailed(error);
            throw error;
        }
    }

    private void run() {
        try {
            synchronized (this) {
                // A stop that wins admission closes the unstarted engine itself.
                if (stopRequested) {
                    return;
                }
                runEntered = true;
                notifyAll();
            }
            engine.run();
        }
        catch (RuntimeException | Error error) {
            completed(false, error.getMessage(), error);
            throw error;
        }
        finally {
            synchronized (this) {
                // Orderly shutdown never interrupts a callback settling an accepted ACK.
                executor.shutdown();
                notifyAll();
            }
        }
    }

    void startFailed(Throwable error) {
        Throwable cause = error == null ? new IllegalStateException("Engine start failed") : error;
        synchronized (this) {
            rememberFailure(cause.getMessage(), cause);
            startInProgress = false;
            if (executor != null) {
                executor.shutdown();
            }
            notifyAll();
        }
        bridge.fail(cause);
    }

    void completed(boolean success, String message, Throwable error) {
        Throwable cause = error;
        synchronized (this) {
            completionSeen = true;
            boolean cancelledByStop = stopRequested && !bridge.failed() &&
                    BatchBridge.causesStopCancellation(error);
            if (cancelledByStop) {
                // Debezium wraps a normal cancelled callback as a failed Future.
                // It confirms no source ACK and must not poison a clean pause.
                cause = null;
                completionSuccess = false;
            }
            else if (!success || error != null) {
                cause = error == null ? new IllegalStateException(clean(message)) : error;
                rememberFailure(message, cause);
            }
            else if (failure == null) {
                completionSuccess = true;
                completionMessage = clean(message);
            }
            notifyAll();
        }
        if (cause != null) {
            bridge.fail(cause);
        }
    }

    private void rememberFailure(String message, Throwable cause) {
        if (failure == null) {
            failure = cause;
            completionMessage = clean(message == null || message.isEmpty() ? cause.toString() : message);
        }
        completionSuccess = false;
    }

    ByteBuffer poll() {
        synchronized (this) {
            if (stopRequested) {
                throw new IllegalStateException("Engine session is closing");
            }
        }

        // A successful completion callback may precede delivery and ACK of its last batch.
        try {
            ByteBuffer batch = bridge.poll();
            if (batch != null) {
                return batch;
            }
        }
        catch (IllegalStateException error) {
            if (!bridge.failed()) {
                throw error;
            }
        }
        if (bridge.failed()) {
            synchronized (this) {
                rememberFailure(bridge.failureMessage(),
                        new IllegalStateException(bridge.failureMessage()));
            }
        }

        Future<?> currentFuture;
        ExecutorService currentExecutor;
        synchronized (this) {
            currentFuture = future;
            currentExecutor = executor;
        }
        if (currentFuture != null && currentFuture.isDone()) {
            try {
                currentFuture.get();
            }
            catch (ExecutionException | CancellationException error) {
                Throwable cause = error.getCause() == null ? error : error.getCause();
                completed(false, cause.getMessage(), cause);
            }
            catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while checking engine completion", error);
            }
        }

        synchronized (this) {
            if (failure != null) {
                return completionFrame(false, completionMessage);
            }
            if (startInProgress || currentFuture == null || !currentFuture.isDone() ||
                    currentExecutor == null || !currentExecutor.isTerminated() ||
                    !bridge.callbacksQuiescent() || bridge.hasPending()) {
                return null;
            }
            IllegalStateException lifecycleFailure = unconfirmedLifecycle();
            if (lifecycleFailure != null) {
                rememberFailure(lifecycleFailure.getMessage(), lifecycleFailure);
                bridge.fail(lifecycleFailure);
            }
            if (!completionSeen || !completionSuccess) {
                rememberFailure("Engine exited without a successful completion callback",
                        new IllegalStateException("Engine completion was not confirmed"));
                bridge.fail(failure);
            }
            return completionFrame(failure == null, completionMessage);
        }
    }

    void acknowledge(int id, boolean all, int from, int to) throws InterruptedException {
        synchronized (this) {
            if (stopRequested) {
                throw new IllegalStateException("Engine session is closing");
            }
        }
        bridge.acknowledge(id, all, from, to);
    }

    void requestStop() {
        synchronized (this) {
            if (stopRequested) {
                return;
            }
            stopRequested = true;
        }
        try {
            // This cancels undelivered/unacknowledged work, but lets ACKING finish.
            bridge.requestStop();
            Thread closer = new Thread(this::close, "synchdb-engine-close");
            closer.setDaemon(true);
            closer.start();
        }
        catch (RuntimeException | Error error) {
            synchronized (this) {
                rememberFailure(error.getMessage(), error);
                closeStatus = FAILED;
            }
            throw error;
        }
    }

    private void close() {
        ExecutorService currentExecutor = null;
        try {
            DebeziumEngine<ChangeEvent<String, String>> currentEngine;
            synchronized (this) {
                while (startInProgress) {
                    wait();
                }
                currentEngine = engine;
                currentExecutor = executor;
            }
            awaitCallbacks();
            if (currentEngine != null) {
                closeEngine(currentEngine);
            }
            if (currentExecutor != null) {
                currentExecutor.shutdown();
                while (!currentExecutor.awaitTermination(10, TimeUnit.MILLISECONDS)) {
                    // C owns the deadline. Never mistake an interrupt request for termination.
                }
            }
            awaitCallbacks();
            synchronized (this) {
                IllegalStateException lifecycleFailure = unconfirmedLifecycle();
                if (lifecycleFailure != null) {
                    throw lifecycleFailure;
                }
                closeStatus = CLOSED;
                notifyAll();
            }
        }
        catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            synchronized (this) {
                rememberFailure(error.getMessage(), error);
                closeStatus = FAILED;
                notifyAll();
            }
            bridge.fail(error);
        }
        finally {
            if (currentExecutor != null) {
                currentExecutor.shutdown();
            }
        }
    }

    private void awaitCallbacks() throws InterruptedException {
        while (!bridge.callbacksQuiescent()) {
            Thread.sleep(10);
        }
    }

    private void closeEngine(DebeziumEngine<ChangeEvent<String, String>> currentEngine)
            throws Exception {
        for (;;) {
            try {
                currentEngine.close();
                return;
            }
            catch (IllegalStateException error) {
                // Debezium 3.5.2 exposes no public state API. Recognize only its exact
                // own state guards, never a connector's unrelated IllegalStateException.
                int guard = asyncCloseGuard(currentEngine, error);
                if (guard == 0) {
                    throw error;
                }
                if (guard == 1) {
                    awaitCallbacks();
                    Thread.sleep(10);
                    continue;
                }
                // Natural completion owns the close already. Its Future must really
                // return before either state guard can count as a successful close.
                for (;;) {
                    Future<?> currentFuture;
                    boolean entered;
                    synchronized (this) {
                        currentFuture = future;
                        entered = runEntered;
                    }
                    if (!entered || currentFuture == null) {
                        throw error;
                    }
                    if (currentFuture.isDone()) {
                        awaitCallbacks();
                        return;
                    }
                    Thread.sleep(10);
                }
            }
        }
    }

    private static int asyncCloseGuard(DebeziumEngine<?> currentEngine,
                                      IllegalStateException error) {
        StackTraceElement[] trace = error.getStackTrace();
        if (!ASYNC_ENGINE.equals(currentEngine.getClass().getName()) ||
                error.getClass() != IllegalStateException.class || trace.length == 0 ||
                !ASYNC_ENGINE.equals(trace[0].getClassName())) {
            return 0;
        }
        String message = error.getMessage();
        if ("close".equals(trace[0].getMethodName())) {
            if (STARTING_TASKS.equals(message)) {
                return 1;
            }
            return ALREADY_STOPPING.equals(message) || ALREADY_STOPPED.equals(message) ? 2 : 0;
        }
        if (!"setEngineState".equals(trace[0].getMethodName()) || trace.length < 2 ||
                !ASYNC_ENGINE.equals(trace[1].getClassName()) ||
                !"close".equals(trace[1].getMethodName()) || message == null) {
            return 0;
        }
        // close() can lose its state CAS to startup advancing, or to run()'s
        // natural shutdown. Match only the exact 3.5.2 diagnostic and transitions.
        String[] states = {"CREATING", "INITIALIZING", "CREATING_TASKS",
                "STARTING_TASKS", "POLLING_TASKS", "STOPPING", "STOPPED"};
        for (int expected = 0; expected < 5; expected++) {
            for (int current = expected + 1; current < states.length; current++) {
                String known = CHANGE_STOPPING + states[expected] +
                        "', current engine state is '" + states[current] + "'";
                if (known.equals(message)) {
                    return current >= 5 ? 2 : 1;
                }
            }
        }
        return 0;
    }

    synchronized int stopStatus() {
        return closeStatus;
    }

    /* Offset replacement is safe only after confirmed closure of a healthy bridge. */
    boolean offsetWritable() {
        return restartable();
    }

    boolean restartable() {
        synchronized (this) {
            if (closeStatus != CLOSED || unconfirmedLifecycle() != null) {
                return false;
            }
        }
        return bridge.restartable();
    }

    private static String clean(String value) {
        String message = value == null ? "" : value.replace('\n', ' ').replace('\r', ' ').replace('\0', ' ');
        return message.length() > 4096 ? message.substring(0, 4096) : message;
    }

    private static ByteBuffer completionFrame(boolean success, String message) {
        byte[] status = (success ? "true" : "false").getBytes(StandardCharsets.UTF_8);
        byte[] text = clean(message).getBytes(StandardCharsets.UTF_8);
        ByteBuffer result = ByteBuffer.allocateDirect(1 + Integer.BYTES + status.length +
                Integer.BYTES + text.length);
        result.put((byte) 'K').putInt(status.length).put(status).putInt(text.length).put(text).flip();
        return result;
    }
}
