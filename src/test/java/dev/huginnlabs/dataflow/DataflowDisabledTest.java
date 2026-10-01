package dev.huginnlabs.dataflow;

import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import dev.huginnlabs.dataflow.gen.DataflowProto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs before every other class (ClassOrderer.ClassName) and before any
 * configure() call, proving the best-effort contract: with the SDK disabled
 * (no endpoint / API key / DATAFLOW_DISABLED), tracing passes through
 * untouched and the wrappers decide per call — so wrapping before
 * {@code configure()} is safe.
 */
@TestClassOrder(ClassOrderer.ClassName.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DataflowDisabledTest {

    /** Connection fake: records every call, hands out a recording Statement. */
    private static Connection fakeConn(List<String> calls) {
        return (Connection) Proxy.newProxyInstance(DataflowDisabledTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    calls.add(method.getName());
                    if (method.getName().equals("createStatement")) return fakeStatement(calls);
                    Class<?> r = method.getReturnType();
                    if (r == boolean.class) return Boolean.FALSE;
                    if (r == int.class) return 0;
                    return null;
                });
    }

    private static Statement fakeStatement(List<String> calls) {
        return (Statement) Proxy.newProxyInstance(DataflowDisabledTest.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, args) -> {
                    calls.add(method.getName());
                    Class<?> r = method.getReturnType();
                    if (r == boolean.class) return Boolean.FALSE;
                    if (r == int.class) return 0;
                    return null;
                });
    }

    @Test
    @Order(1)
    void httpWrapperPassesThroughWhenDisabled() {
        assertFalse(Dataflow.enabled());
        TracedHttpClient client = TracedHttpClient.create(HttpClient.newHttpClient());
        HttpRequest req = HttpRequest.newBuilder(URI.create("https://api.example.com/v1/items")).GET().build();
        assertNull(client.begin(req));
        assertSame(req, TracedHttpClient.inject(req, null));
    }

    @Test
    @Order(2)
    void jdbcWrapAndQueryPassThroughWhenDisabled() throws Exception {
        assertFalse(Dataflow.enabled());
        List<String> calls = new ArrayList<>();
        Connection conn = Dataflow.wrap(fakeConn(calls), "postgresql");
        assertNotNull(conn);
        conn.createStatement().executeQuery("SELECT 1 FROM orders");
        assertTrue(calls.contains("createStatement"));
        assertTrue(calls.contains("executeQuery"));
        assertEquals(0, SdkTestEnv.events().size());
    }

    @Test
    @Order(3)
    void captureRunsBodyAndPropagatesWhenDisabled() {
        assertFalse(Dataflow.enabled());
        List<String> ran = new ArrayList<>();
        IllegalStateException boom = new IllegalStateException("boom");
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> Dataflow.capture(() -> {
                    ran.add("ran");
                    throw boom;
                }));
        assertSame(boom, thrown);
        assertEquals(List.of("ran"), ran);
    }

    @Test
    @Order(4)
    void captureCallablePassesThroughWhenDisabled() throws Exception {
        assertFalse(Dataflow.enabled());
        assertEquals("ok", Dataflow.captureCallable(() -> "ok"));
        Exception e = assertThrows(java.io.IOException.class,
                () -> Dataflow.captureCallable(() -> { throw new java.io.IOException("io"); }));
        assertEquals("io", e.getMessage());
    }

    @Test
    @Order(5)
    void captureUncaughtNoOpsWhenDisabled() {
        assertFalse(Dataflow.enabled());
        Thread.UncaughtExceptionHandler before = Thread.getDefaultUncaughtExceptionHandler();
        Dataflow.captureUncaught(); // disabled: nothing is installed
        assertSame(before, Thread.getDefaultUncaughtExceptionHandler());
        Dataflow.ignoreUncaught(); // no-op, must not throw
        assertSame(before, Thread.getDefaultUncaughtExceptionHandler());
    }

    @Test
    @Order(6)
    void logsAreNoOpsWhenDisabled() {
        assertFalse(Dataflow.enabled());
        Dataflow.info("nope", java.util.Map.of("k", "v"));
        Dataflow.warn("nope", null);
        Dataflow.error("nope", null);
        Dataflow.debug("nope", null);
        Dataflow.log("warn", "nope", null);
        Dataflow.flushLogs(); // must not throw
        assertEquals(0, Logs.pending());

        // The JUL bridge records nothing while disabled.
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger("sdk.disabled.jul");
        logger.setUseParentHandlers(false);
        Dataflow.LogHandler handler = new Dataflow.LogHandler();
        logger.addHandler(handler);
        try {
            logger.info("disabled record");
        } finally {
            logger.removeHandler(handler);
        }
        assertEquals(0, Logs.pending());
    }

    @Test
    @Order(7)
    void wrappingBeforeConfigureIsSafeAndLaterQueriesAreTraced() throws Exception {
        List<String> calls = new ArrayList<>();
        Connection conn = Dataflow.wrap(fakeConn(calls), "postgresql");
        // Still disabled here — wrapping happened before configure().
        assertFalse(Dataflow.enabled());

        SdkTestEnv.configure();
        assertTrue(Dataflow.enabled());

        SdkTestEnv.startCapture();
        try {
            conn.createStatement().executeQuery("SELECT 1 FROM orders");
        } finally {
            SdkTestEnv.stopCapture();
        }
        assertEquals(1, SdkTestEnv.events().size());
        assertEquals("SELECT orders", SdkTestEnv.events().get(0).getName());
        assertEquals(DataflowProto.EventType.EVENT_TYPE_DB_QUERY, SdkTestEnv.events().get(0).getType());
    }
}
