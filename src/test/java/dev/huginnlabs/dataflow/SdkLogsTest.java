package dev.huginnlabs.dataflow;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Application log shipping: {@code Dataflow.info/warn/error/debug/log} and
 * the JUL bridge, asserted against a real POST — an in-JVM
 * {@link HttpServer} plays the ingestion API and the batch bodies are
 * parsed and checked for the wire shape, trace/span correlation with the
 * current span, level normalization, field stringification/caps, batching
 * and the drop-oldest overflow behavior. Runs after {@link SdkCrashTest}
 * (class name ordering) so the SDK is already enabled; {@link #beforeAll()}
 * points the endpoint at the local server in URL form — the manifest
 * base-resolution rule — and {@link #afterAll()} restores the dead bare
 * {@code host:port} endpoint (logging off) for the suites that follow.
 */
@TestClassOrder(ClassOrderer.ClassName.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SdkLogsTest {

    /** Every request body received by the fake ingestion API. */
    private static final List<String> BODIES = new CopyOnWriteArrayList<>();
    /** X-Api-Key header of the first received request. */
    private static final AtomicReference<String> apiKeySeen = new AtomicReference<>();
    /** When non-null, the fake API stalls responses (overflow test). */
    private static volatile CountDownLatch hang;

    private static HttpServer server;

    @BeforeAll
    static void beforeAll() throws Exception {
        SdkTestEnv.configure(); // SDK on (flusher thread); bare endpoint = logs off for now
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool(daemons()));
        server.createContext("/api/v1/logs", SdkLogsTest::handle);
        server.start();
        // URL-form endpoint → manifest base resolution hands logging this server.
        Dataflow.configure(new Dataflow.Builder()
                .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .apiKey("test-key")
                .serviceName("sdk-java-test"));
    }

    @AfterAll
    static void afterAll() {
        Dataflow.flushLogs();
        if (server != null) server.stop(0);
        // Back to the dead bare host:port — logging off for the later suites.
        SdkTestEnv.configure();
    }

    private static void handle(HttpExchange exchange) throws IOException {
        try {
            byte[] body = exchange.getRequestBody().readAllBytes();
            BODIES.add(new String(body, StandardCharsets.UTF_8));
            apiKeySeen.compareAndSet(null, exchange.getRequestHeaders().getFirst("X-Api-Key"));
            CountDownLatch latch = hang;
            if (latch != null) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            byte[] resp = "{\"accepted\":1}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        } catch (IOException ignore) {
            // client gave up (5s timeout + retry) — nothing to answer to
        }
    }

    private static ThreadFactory daemons() {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, "sdk-logs-test-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    // -- tests ---------------------------------------------------------------

    @Test
    @Order(1)
    void logLinesCarryTraceContextAndWireShape() {
        int before = totalLogs();
        String traceId;
        String spanId;
        try (Trace t = Dataflow.trace("job.Run")) {
            traceId = t.span().traceId();
            spanId = t.span().spanId();
            Dataflow.info("booking created", mapOf("order", "ord_42", "total", 199));
            Dataflow.warn("low stock", null);
            Dataflow.error("payment failed", mapOf());
            Dataflow.debug("retrying", null);
        }
        Dataflow.flushLogs();
        Dataflow.info("outside span", null); // no current span → empty ids
        Dataflow.flushLogs();

        List<Map<?, ?>> logs = awaitLogs(before, 5);
        assertEquals("test-key", apiKeySeen.get()); // X-Api-Key auth, as the manifest uses

        assertShape(logs.get(0));
        assertEquals("booking created", logs.get(0).get("message"));
        assertEquals("info", logs.get(0).get("level"));
        assertEquals(traceId, logs.get(0).get("trace_id"));
        assertEquals(spanId, logs.get(0).get("span_id"));
        assertEquals(mapOf("order", "ord_42", "total", "199"), logs.get(0).get("fields"));

        assertEquals("low stock", logs.get(1).get("message"));
        assertEquals("warn", logs.get(1).get("level"));
        assertEquals(traceId, logs.get(1).get("trace_id"));
        assertEquals(Map.of(), logs.get(1).get("fields"));

        assertEquals("payment failed", logs.get(2).get("message"));
        assertEquals("error", logs.get(2).get("level"));

        assertEquals("retrying", logs.get(3).get("message"));
        assertEquals("debug", logs.get(3).get("level"));

        assertEquals("outside span", logs.get(4).get("message"));
        assertEquals("", logs.get(4).get("trace_id"));
        assertEquals("", logs.get(4).get("span_id"));
    }

    @Test
    @Order(2)
    void arbitraryLevelsAreNormalized() {
        int before = totalLogs();
        Dataflow.log("WARNING", "w", null);
        Dataflow.log("SEVERE", "e", null);
        Dataflow.log("Fine", "d", null);
        Dataflow.log("totally-custom", "c", null);
        Dataflow.flushLogs();

        List<Map<?, ?>> logs = awaitLogs(before, 4);
        assertEquals("warn", logs.get(0).get("level"));
        assertEquals("error", logs.get(1).get("level"));
        assertEquals("debug", logs.get(2).get("level"));
        assertEquals("info", logs.get(3).get("level"));
    }

    @Test
    @Order(3)
    void fieldsAreStringifiedAndCapped() {
        int before = totalLogs();
        // Insertion order → the first 50 entries survive the cap deterministically.
        Map<String, Object> fat = new java.util.LinkedHashMap<>();
        for (int i = 0; i < 55; i++) fat.put("k" + i, i);
        Dataflow.info("fat fields", fat);
        Dataflow.info("long value", mapOf("v", "x".repeat(600)));
        Dataflow.flushLogs();

        List<Map<?, ?>> logs = awaitLogs(before, 2);
        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) logs.get(0).get("fields");
        assertEquals(50, fields.size()); // capped, extras dropped
        assertEquals("0", fields.get("k0"));
        assertEquals("49", fields.get("k49"));
        assertFalse(fields.containsKey("k50"));

        @SuppressWarnings("unchecked")
        Map<String, Object> longFields = (Map<String, Object>) logs.get(1).get("fields");
        assertEquals(512, ((String) longFields.get("v")).length());
    }

    @Test
    @Order(4)
    void batchingShipsEverythingWithinTheServerCap() {
        int before = totalLogs();
        for (int i = 0; i < 120; i++) {
            Dataflow.info("bulk " + i, null);
        }
        Dataflow.flushLogs();

        List<Map<?, ?>> logs = awaitLogs(before, 120);
        assertEquals(120, logs.size());
        for (int i = 0; i < logs.size(); i++) {
            assertEquals("bulk " + i, logs.get(i).get("message")); // nothing lost, order kept
        }
        // Every POST batch obeys the server's 1000-line cap.
        for (String body : BODIES) {
            Object logsArray = ((Map<?, ?>) parseJson(body)).get("logs");
            assertTrue(((List<?>) logsArray).size() <= Logs.MAX_BATCH);
        }
    }

    @Test
    @Order(5)
    void julHandlerForwardsRecordsWithLevelMappingAndParameters() {
        int before = totalLogs();
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger("sdk.logs.jul");
        logger.setUseParentHandlers(false);
        logger.setLevel(java.util.logging.Level.ALL);
        Dataflow.LogHandler handler = new Dataflow.LogHandler();
        logger.addHandler(handler);
        try {
            logger.log(java.util.logging.Level.SEVERE, "boom {0}", "disk");
            logger.log(java.util.logging.Level.WARNING, "careful");
            logger.log(java.util.logging.Level.FINE, "tiny");
        } finally {
            logger.removeHandler(handler);
        }
        Dataflow.flushLogs();

        List<Map<?, ?>> logs = awaitLogs(before, 3);
        assertEquals("error", logs.get(0).get("level"));
        assertEquals("boom disk", logs.get(0).get("message")); // parameters formatted
        assertEquals("warn", logs.get(1).get("level"));
        assertEquals("careful", logs.get(1).get("message"));
        assertEquals("debug", logs.get(2).get("level"));
        assertEquals("tiny", logs.get(2).get("message"));
    }

    @Test
    @Order(6)
    void overflowDropsOldestAndCounts() {
        long beforeDropped = Logs.droppedCount();
        hang = new CountDownLatch(1); // stall the in-flight POST
        try {
            // The first drain takes at most MAX_BATCH (1000) lines, so >1024
            // lines beyond that are guaranteed to overflow the 1024 queue.
            for (int i = 0; i < Logs.MAX_BATCH + Logs.QUEUE_CAP + 200; i++) {
                Dataflow.info("overflow " + i, null);
            }
            assertTrue(Logs.droppedCount() > beforeDropped);
            assertTrue(Logs.pending() <= Logs.QUEUE_CAP);
        } finally {
            hang.countDown();
            hang = null;
        }
        Dataflow.flushLogs();
        assertTrue(totalLogs() > 0);
    }

    // -- helpers ---------------------------------------------------------------

    /** Total number of log entries received across all batches so far. */
    private static int totalLogs() {
        int n = 0;
        for (String body : BODIES) {
            Object logs = ((Map<?, ?>) parseJson(body)).get("logs");
            if (logs instanceof List<?> l) n += l.size();
        }
        return n;
    }

    /**
     * Waits until {@code count} more log entries (beyond {@code baseline})
     * have been received, then returns exactly that slice.
     */
    private static List<Map<?, ?>> awaitLogs(int baseline, int count) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline && totalLogs() < baseline + count) {
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        List<Map<?, ?>> all = new ArrayList<>();
        for (String body : BODIES) {
            Object logs = ((Map<?, ?>) parseJson(body)).get("logs");
            if (logs instanceof List<?> l) {
                for (Object o : l) all.add((Map<?, ?>) o);
            }
        }
        assertTrue(all.size() >= baseline + count,
                "expected " + (baseline + count) + " logs, saw " + all.size());
        return new ArrayList<>(all.subList(baseline, baseline + count));
    }

    /** Wire shape of one log entry. */
    private static void assertShape(Map<?, ?> entry) {
        Object ts = entry.get("timestamp");
        assertTrue(ts instanceof Number && ((Number) ts).longValue() > 0, "timestamp");
        assertTrue(List.of("debug", "info", "warn", "error").contains(entry.get("level")), "level");
        assertTrue(entry.get("message") instanceof String, "message");
        assertTrue(entry.get("trace_id") instanceof String, "trace_id");
        assertTrue(entry.get("span_id") instanceof String, "span_id");
        assertEquals("sdk-java-test", entry.get("service_name"));
        assertNotNull(entry.get("fields"), "fields");
        for (Object v : ((Map<?, ?>) entry.get("fields")).values()) {
            assertTrue(v instanceof String, "field values are strings");
        }
    }

    private static Map<String, Object> mapOf(Object... kv) {
        Map<String, Object> m = new java.util.LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    // -- minimal JSON parser (no third-party dependencies) ---------------------

    /** Parses batch bodies into Map/List/String/Double/Boolean/null trees. */
    static Object parseJson(String s) {
        return new Parser(s).parseValue();
    }

    private static final class Parser {
        private final String s;
        private int i;

        Parser(String s) { this.s = s; }

        Object parseValue() {
            skipWs();
            char c = s.charAt(i);
            switch (c) {
                case '{': return parseObject();
                case '[': return parseArray();
                case '"': return parseString();
                case 't': expect("true"); return Boolean.TRUE;
                case 'f': expect("false"); return Boolean.FALSE;
                case 'n': expect("null"); return null;
                default: return parseNumber();
            }
        }

        private Map<String, Object> parseObject() {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            i++; // {
            skipWs();
            if (s.charAt(i) == '}') { i++; return m; }
            while (true) {
                skipWs();
                String key = parseString();
                skipWs();
                i++; // :
                m.put(key, parseValue());
                skipWs();
                char c = s.charAt(i);
                i++;
                if (c == '}') return m;
                if (c != ',') throw new IllegalStateException("bad object at " + i);
            }
        }

        private List<Object> parseArray() {
            List<Object> l = new ArrayList<>();
            i++; // [
            skipWs();
            if (s.charAt(i) == ']') { i++; return l; }
            while (true) {
                l.add(parseValue());
                skipWs();
                char c = s.charAt(i);
                i++;
                if (c == ']') return l;
                if (c != ',') throw new IllegalStateException("bad array at " + i);
            }
        }

        private String parseString() {
            StringBuilder sb = new StringBuilder();
            i++; // "
            while (true) {
                char c = s.charAt(i++);
                if (c == '"') return sb.toString();
                if (c != '\\') { sb.append(c); continue; }
                char e = s.charAt(i++);
                switch (e) {
                    case '"': sb.append('"'); break;
                    case '\\': sb.append('\\'); break;
                    case '/': sb.append('/'); break;
                    case 'b': sb.append('\b'); break;
                    case 'f': sb.append('\f'); break;
                    case 'n': sb.append('\n'); break;
                    case 'r': sb.append('\r'); break;
                    case 't': sb.append('\t'); break;
                    case 'u':
                        sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                        i += 4;
                        break;
                    default: throw new IllegalStateException("bad escape \\" + e);
                }
            }
        }

        private Double parseNumber() {
            int start = i;
            while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) i++;
            return Double.parseDouble(s.substring(start, i));
        }

        private void expect(String word) {
            if (!s.startsWith(word, i)) throw new IllegalStateException("expected " + word + " at " + i);
            i += word.length();
        }

        private void skipWs() {
            while (i < s.length() && " \t\r\n".indexOf(s.charAt(i)) >= 0) i++;
        }
    }
}
