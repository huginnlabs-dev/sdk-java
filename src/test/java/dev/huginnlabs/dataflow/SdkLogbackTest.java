package dev.huginnlabs.dataflow;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
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
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The Logback appender end-to-end: a Logback {@link LoggerContext} wired
 * with {@link LogbackAppender} through {@code Dataflow.installLogback} and
 * driven by SLF4J {@code LoggerFactory} loggers, asserted against a real
 * POST — an in-JVM {@link HttpServer} plays the ingestion API (same pattern
 * as {@link SdkLogsTest}). Covers {@code {}}-parameter formatting,
 * trace-id correlation with the current span, level mapping
 * (TRACE/DEBUG→debug … ERROR→error), the throwable stand-in for empty
 * messages, idempotent install / no-op uninstall and the disabled-SDK
 * no-op. Runs between {@link SdkCrashTest} and {@link SdkLogsTest} (class
 * name ordering); {@link #afterAll()} detaches the appender and restores
 * the dead bare endpoint so the later suites see a clean pipeline.
 */
@TestClassOrder(ClassOrderer.ClassName.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SdkLogbackTest {

    /** Every request body received by the fake ingestion API. */
    private static final List<String> BODIES = new CopyOnWriteArrayList<>();

    private static HttpServer server;

    @BeforeAll
    static void beforeAll() throws Exception {
        SdkTestEnv.configure(); // SDK on (flusher thread); bare endpoint = logs off for now
        awaitDefaultLogbackContext();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool(daemons()));
        server.createContext("/api/v1/logs", SdkLogbackTest::handle);
        server.start();
        // URL-form endpoint → manifest base resolution hands logging this server.
        Dataflow.configure(new Dataflow.Builder()
                .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .apiKey("test-key")
                .serviceName("sdk-java-test"));
    }

    @AfterAll
    static void afterAll() {
        Dataflow.uninstallLogback(); // no-op when nothing is installed; must not throw
        if (server != null) server.stop(0);
        // Back to the dead bare host:port — logging off for the later suites.
        SdkTestEnv.configure();
    }

    private static void handle(HttpExchange exchange) throws IOException {
        try {
            byte[] body = exchange.getRequestBody().readAllBytes();
            BODIES.add(new String(body, StandardCharsets.UTF_8));
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
            Thread t = new Thread(r, "sdk-logback-test-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /**
     * slf4j 2.x binds its provider on a background thread; until that lands,
     * {@code LoggerFactory.getILoggerFactory()} hands out a substitute
     * factory. Wait (bounded) for the real Logback context so the
     * default-factory install behaves deterministically — production
     * services call {@code installLogback()} long after startup and never
     * see this window.
     */
    private static void awaitDefaultLogbackContext() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!(LoggerFactory.getILoggerFactory() instanceof LoggerContext)) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("Logback provider did not initialize");
            }
            Thread.sleep(10);
        }
    }

    // -- tests ----------------------------------------------------------------

    @Test
    @Order(1)
    void slf4jLoggingShipsWithFormattingAndTraceCorrelation() {
        Dataflow.uninstallLogback(); // precondition: nothing installed — no-op
        assertEquals(0, dataflowAppenders(defaultRoot()));

        Dataflow.installLogback();   // default LoggerFactory.getILoggerFactory()
        Dataflow.installLogback();   // idempotent: must not double-attach
        assertEquals(1, dataflowAppenders(defaultRoot()));

        // LoggerFactory hands out the slf4j Logger; the Logback type carries
        // the programmatic level gate the tests need.
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("sdk.logback.slf4j");
        logger.setLevel(Level.INFO);
        String traceId;
        try (Trace t = Dataflow.trace("logback.Run")) {
            traceId = t.span().traceId();
            logger.info("lb1-booking {} for {}", "created", "ord_42");
            logger.warn("lb1-cache cold");
        }
        Dataflow.flushLogs();

        List<Map<?, ?>> logs = awaitLogsContaining("lb1-", 2);
        assertEquals("lb1-booking created for ord_42", logs.get(0).get("message")); // {} formatted
        assertEquals("info", logs.get(0).get("level"));
        assertEquals(traceId, logs.get(0).get("trace_id")); // stamped with the current span
        assertEquals("lb1-cache cold", logs.get(1).get("message"));
        assertEquals("warn", logs.get(1).get("level"));
    }

    @Test
    @Order(2)
    void levelsMapOntoTheWireVocabulary() {
        // Explicit context overload — install on a context the test manages.
        LoggerContext ctx = new LoggerContext();
        ch.qos.logback.classic.Logger logger = ctx.getLogger("sdk.logback.levels");
        logger.setLevel(Level.TRACE); // let every level through the logger gate
        Dataflow.installLogback(ctx);

        logger.trace("lb2-t");
        logger.debug("lb2-d");
        logger.info("lb2-i");
        logger.warn("lb2-w");
        logger.error("lb2-e");
        Dataflow.flushLogs();

        List<Map<?, ?>> logs = awaitLogsContaining("lb2-", 5);
        assertEquals("debug", logs.get(0).get("level")); // TRACE → debug
        assertEquals("debug", logs.get(1).get("level"));
        assertEquals("info", logs.get(2).get("level"));
        assertEquals("warn", logs.get(3).get("level"));
        assertEquals("error", logs.get(4).get("level"));
    }

    @Test
    @Order(3)
    void throwablesStandInForEmptyMessagesAndAreSkippedOtherwise() {
        Dataflow.installLogback(); // back on the default LoggerFactory context
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("sdk.logback.throwable");
        logger.setLevel(Level.ERROR);
        logger.error((String) null, new IllegalStateException("lb3 disk full")); // no message
        logger.error("lb3-ctx-kept", new IllegalStateException("not shipped"));  // message kept
        Dataflow.flushLogs();

        List<Map<?, ?>> logs = awaitLogsContaining("lb3", 2);
        assertEquals("java.lang.IllegalStateException: lb3 disk full", logs.get(0).get("message"));
        assertEquals("error", logs.get(0).get("level"));
        assertEquals("lb3-ctx-kept", logs.get(1).get("message")); // v1: throwable skipped
    }

    @Test
    @Order(4)
    void uninstallDetachesAndStopsShipping() {
        Dataflow.flushLogs();
        Dataflow.installLogback();
        assertEquals(1, dataflowAppenders(defaultRoot()));

        Dataflow.uninstallLogback();
        assertEquals(0, dataflowAppenders(defaultRoot()));

        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("sdk.logback.after");
        logger.setLevel(Level.INFO);
        logger.info("lb4 after uninstall"); // no appender attached anymore
        Dataflow.flushLogs();
        assertEquals(0, awaitLogsContaining("lb4", 0).size()); // nothing shipped

        Dataflow.uninstallLogback(); // idempotent: no-op, must not throw
    }

    @Test
    @Order(5)
    void disabledSdkDropsRecordsSilently() {
        Dataflow.flushLogs();
        Dataflow.configure(new Dataflow.Builder()
                .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                .apiKey("test-key")
                .serviceName("sdk-java-test")
                .disabled(true));
        try {
            Dataflow.installLogback();
            ch.qos.logback.classic.Logger logger =
                    (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("sdk.logback.disabled");
            logger.setLevel(Level.INFO);
            logger.error("lb5 while disabled");
            logger.warn("lb5 also disabled");
            assertEquals(0, Logs.pending()); // dropped silently, before the queue
        } finally {
            Dataflow.configure(new Dataflow.Builder()
                    .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
                    .apiKey("test-key")
                    .serviceName("sdk-java-test"));
            Dataflow.uninstallLogback(); // leave the suites that follow untouched
        }
        assertEquals(0, Logs.pending());
    }

    // -- helpers ---------------------------------------------------------------

    /** The root logger of the default SLF4J (Logback) context. */
    private static ch.qos.logback.classic.Logger defaultRoot() {
        return ((LoggerContext) LoggerFactory.getILoggerFactory())
                .getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    }

    /** Number of Dataflow appenders attached to the logger. */
    private static int dataflowAppenders(ch.qos.logback.classic.Logger logger) {
        int n = 0;
        for (var it = logger.iteratorForAppenders(); it.hasNext(); ) {
            if (it.next() instanceof LogbackAppender) n++;
        }
        return n;
    }

    private static int matching(String fragment) {
        int n = 0;
        for (String body : BODIES) {
            Object logs = ((Map<?, ?>) SdkLogsTest.parseJson(body)).get("logs");
            if (logs instanceof List<?> l) {
                for (Object o : l) {
                    if (String.valueOf(((Map<?, ?>) o).get("message")).contains(fragment)) n++;
                }
            }
        }
        return n;
    }

    /**
     * Waits until exactly {@code count} shipped entries whose message
     * contains {@code fragment} have been received, then returns them in
     * ship order. Filtering by fragment keeps the asserts immune to any
     * unrelated framework lines the root-level appender may also forward
     * (Logback is on the test classpath for the whole run).
     */
    private static List<Map<?, ?>> awaitLogsContaining(String fragment, int count) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline && matching(fragment) < count) {
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        List<Map<?, ?>> all = new ArrayList<>();
        for (String body : BODIES) {
            Object logs = ((Map<?, ?>) SdkLogsTest.parseJson(body)).get("logs");
            if (logs instanceof List<?> l) {
                for (Object o : l) {
                    Map<?, ?> entry = (Map<?, ?>) o;
                    if (String.valueOf(entry.get("message")).contains(fragment)) all.add(entry);
                }
            }
        }
        assertEquals(count, all.size(),
                "expected exactly " + count + " entries containing \"" + fragment + "\"");
        return all;
    }
}
