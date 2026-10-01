package dev.huginnlabs.dataflow;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Application log shipping: {@code Dataflow.info/warn/error/debug/log} and
 * the JUL bridge ({@link Dataflow.LogHandler}) buffer log lines in a bounded
 * queue — each stamped with the current span's trace/span ids so logs join
 * the trace timeline — and a background daemon thread POSTs them as JSON
 * batches to {@code POST /api/v1/logs} ({@code X-Api-Key} auth, at most
 * 1000 lines per batch, one flush every 500 ms or 50 buffered lines).
 *
 * <p>The HTTP base reuses the manifest's resolution
 * ({@link Manifest#httpBaseURL}): a bare {@code host:port} gRPC endpoint
 * carries no derivable HTTP base, so logging stays off unless the endpoint
 * is URL-form or {@code DATAFLOW_HTTP_URL} is set.
 *
 * <p>Everything is best-effort: recording never throws or blocks on I/O,
 * the 1024-line queue drops its oldest line under pressure (counted), a
 * failed POST is retried once and the batch then dropped, and
 * {@code Dataflow.flushLogs()} waits at most a few seconds. With the SDK
 * disabled every entry point is a no-op.
 */
final class Logs {

    /** Queue cap — drop-oldest above this, with a dropped-lines counter. */
    static final int QUEUE_CAP = 1024;
    /** Server-side cap: at most 1000 lines per POST. */
    static final int MAX_BATCH = 1000;
    /** Lines buffered before the flusher wakes early. */
    static final int FLUSH_THRESHOLD = 50;
    /** Max age of a partial batch before it is flushed. */
    static final long FLUSH_INTERVAL_MS = 500;
    /** Wire cap on one message (the server clamps to the same). */
    static final int MESSAGE_CAP = 8192;
    /** Wire caps on fields (the server clamps to the same). */
    static final int MAX_FIELDS = 50;
    static final int FIELD_KEY_CAP = 128;
    static final int FIELD_VALUE_CAP = 512;
    /** Per-POST timeout. */
    static final Duration TIMEOUT = Duration.ofSeconds(5);

    /** One buffered log line. */
    static final class Entry {
        final long timestamp;              // unix millis
        final String level;                // debug | info | warn | error
        final String message;
        final String traceId;
        final String spanId;
        final Map<String, String> fields;

        Entry(long timestamp, String level, String message,
              String traceId, String spanId, Map<String, String> fields) {
            this.timestamp = timestamp;
            this.level = level;
            this.message = message;
            this.traceId = traceId;
            this.spanId = spanId;
            this.fields = fields;
        }
    }

    private static final ReentrantLock LOCK = new ReentrantLock();
    private static final Condition WAKE = LOCK.newCondition();
    private static final ArrayDeque<Entry> QUEUE = new ArrayDeque<>();
    private static volatile boolean running;
    /** Batches taken from the queue but not yet POSTed (lock-guarded). */
    private static int inFlight;
    /** Lines evicted from the full queue (lock-guarded). */
    private static long dropped;

    private Logs() {}

    // -- recording (behind Dataflow.info/warn/error/debug/log + JUL) --------

    /**
     * Buffers one line with the current span's trace context. Never throws;
     * a no-op while the SDK is disabled or no HTTP base can be resolved.
     */
    static void record(String level, String message, Map<String, Object> fields) {
        try {
            if (!Dataflow.enabled()) return;
            if (Manifest.httpBaseURL(Dataflow.settings().endpoint) == null) return;

            Span current = Span.current();
            Map<String, String> fs = new LinkedHashMap<>();
            if (fields != null) {
                for (Map.Entry<String, Object> e : fields.entrySet()) {
                    if (fs.size() >= MAX_FIELDS) break;
                    Object v = e.getValue();
                    fs.put(clip(String.valueOf(e.getKey()), FIELD_KEY_CAP),
                            clip(v == null ? "" : String.valueOf(v), FIELD_VALUE_CAP));
                }
            }
            Entry entry = new Entry(
                    System.currentTimeMillis(),
                    normalize(level),
                    clip(message == null ? "" : message, MESSAGE_CAP),
                    current != null ? current.traceId() : "",
                    current != null ? current.spanId() : "",
                    fs);

            LOCK.lock();
            try {
                QUEUE.addLast(entry);
                while (QUEUE.size() > QUEUE_CAP) {
                    QUEUE.pollFirst();
                    dropped++;
                }
                if (QUEUE.size() >= FLUSH_THRESHOLD) WAKE.signal();
            } finally {
                LOCK.unlock();
            }
        } catch (Throwable ignore) {
            // best-effort: logging must never break the caller
        }
    }

    /** JUL bridge: formats the message (parameters included) and records it. */
    static void jul(java.util.logging.LogRecord record) {
        if (record == null || !Dataflow.enabled()) return;
        try {
            String message = new java.util.logging.SimpleFormatter().formatMessage(record);
            record(record.getLevel().getName(), message, null);
        } catch (Throwable ignore) {
            // never propagate into the logging framework
        }
    }

    /**
     * Maps any level label onto the wire's {@code debug|info|warn|error}:
     * JUL names (SEVERE, WARNING, FINE, …) included, unknown labels report
     * as {@code info} — mirroring the server's own normalization.
     */
    static String normalize(String level) {
        String l = level == null ? "" : level.trim().toLowerCase(Locale.ROOT);
        switch (l) {
            case "debug": case "fine": case "finer": case "finest": return "debug";
            case "warn": case "warning": return "warn";
            case "error": case "severe": case "err": case "fatal": return "error";
            default: return "info";
        }
    }

    // -- flusher -------------------------------------------------------------

    /** Starts the background flusher (called once from SDK startup). */
    static void start(Dataflow.Settings s) {
        running = true;
        Thread t = new Thread(Logs::run, "dataflow-logs");
        t.setDaemon(true);
        t.start();
    }

    private static void run() {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
        while (running) {
            List<Entry> batch;
            LOCK.lock();
            try {
                long deadline = System.nanoTime() + FLUSH_INTERVAL_MS * 1_000_000L;
                while (QUEUE.isEmpty() && running) {
                    long remain = deadline - System.nanoTime();
                    if (remain <= 0) break;
                    WAKE.awaitNanos(remain);
                }
                batch = drainLocked();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
                return;
            } finally {
                LOCK.unlock();
            }
            if (batch.isEmpty()) continue;
            postWithRetry(client, batch);
        }
    }

    /** Takes up to {@link #MAX_BATCH} lines and marks the batch in-flight. */
    private static List<Entry> drainLocked() {
        int n = Math.min(QUEUE.size(), MAX_BATCH);
        List<Entry> batch = new ArrayList<>(n);
        for (int i = 0; i < n; i++) batch.add(QUEUE.pollFirst());
        if (!batch.isEmpty()) inFlight++;
        WAKE.signalAll(); // flush() waiters track inFlight too
        return batch;
    }

    /** POSTs the batch (one retry), then releases the in-flight mark. */
    private static void postWithRetry(HttpClient client, List<Entry> batch) {
        try {
            String base = Manifest.httpBaseURL(Dataflow.settings().endpoint);
            if (base == null) return; // logging turned off — batch is dropped
            String url = base + "/api/v1/logs";
            String body = buildBody(batch);
            for (int attempt = 0; attempt < 2; attempt++) {
                try {
                    HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                            .timeout(TIMEOUT)
                            .header("Content-Type", "application/json")
                            .header("X-Api-Key", Dataflow.settings().apiKey)
                            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                            .build();
                    // Response body is discarded; only a 2xx counts as sent.
                    HttpResponse<Void> resp =
                            client.send(req, HttpResponse.BodyHandlers.discarding());
                    if (resp.statusCode() >= 200 && resp.statusCode() < 300) return;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable ignore) {
                    // network failure / bad URL — fall through to the retry
                }
                if (attempt == 0) {
                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
            // both attempts failed: the batch is dropped (best-effort)
        } catch (Throwable ignore) {
            // never let shipping break the flusher thread
        } finally {
            LOCK.lock();
            try {
                inFlight--;
                WAKE.signalAll();
            } finally {
                LOCK.unlock();
            }
        }
    }

    /**
     * Builds the batch body: {@code {"logs":[{timestamp, level, message,
     * trace_id, span_id, service_name, fields}, …]}}.
     */
    static String buildBody(List<Entry> batch) {
        String service = Dataflow.serviceName();
        List<Object> logs = new ArrayList<>(batch.size());
        for (Entry e : batch) {
            Map<String, Object> m = new LinkedHashMap<>(8);
            m.put("timestamp", e.timestamp);
            m.put("level", e.level);
            m.put("message", e.message);
            m.put("trace_id", e.traceId);
            m.put("span_id", e.spanId);
            m.put("service_name", service);
            m.put("fields", e.fields);
            logs.add(m);
        }
        Map<String, Object> body = new LinkedHashMap<>(1);
        body.put("logs", logs);
        return Json.write(body);
    }

    /**
     * Best-effort wait until every buffered line has been POSTed (or the
     * deadline passes). Returns immediately while the flusher is not
     * running; never throws.
     */
    static void flush(long timeoutMs) {
        if (!running) return;
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        LOCK.lock();
        try {
            while (!QUEUE.isEmpty() || inFlight > 0) {
                long remain = deadline - System.nanoTime();
                if (remain <= 0) return;
                WAKE.awaitNanos(remain);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            LOCK.unlock();
        }
    }

    // -- test seams (tests live in the same package) --------------------------

    /** Lines currently buffered. */
    static int pending() {
        LOCK.lock();
        try {
            return QUEUE.size();
        } finally {
            LOCK.unlock();
        }
    }

    /** Lines evicted from the full queue so far. */
    static long droppedCount() {
        LOCK.lock();
        try {
            return dropped;
        } finally {
            LOCK.unlock();
        }
    }

    /** Truncates to the wire cap (null-safe). */
    private static String clip(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max);
    }
}
