package dev.huginnlabs.dataflow;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dev.huginnlabs.dataflow.gen.DataflowProto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JDBC proxy behavior over minimal fake java.sql objects (dynamic proxies
 * themselves — no driver, no network): span emission, SQL summarizing,
 * passthrough, and exception propagation.
 */
@TestClassOrder(ClassOrderer.ClassName.class)
class JdbcProxyTest {

    /** Fake driver object: records calls, returns canned values, throws canned errors. */
    static final class Fake implements InvocationHandler {
        final List<String> calls = new ArrayList<>();
        final Map<String, Object> returns = new HashMap<>();
        final Map<String, Throwable> failures = new HashMap<>();

        @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            calls.add(method.getName());
            Throwable t = failures.get(method.getName());
            if (t != null) throw t;
            Object r = returns.get(method.getName());
            if (r != null) return r;
            Class<?> c = method.getReturnType();
            if (c == boolean.class) return Boolean.FALSE;
            if (c == int.class) return 0;
            if (c == long.class) return 0L;
            return null;
        }

        Fake on(String method, Object value) { returns.put(method, value); return this; }
        Fake fails(String method, Throwable t) { failures.put(method, t); return this; }

        Object proxy(Class<?> iface) {
            return Proxy.newProxyInstance(JdbcProxyTest.class.getClassLoader(), new Class<?>[]{iface}, this);
        }
    }

    @BeforeAll
    static void enableSdk() {
        SdkTestEnv.configure();
    }

    private static List<DataflowProto.TraceEvent> traced(Runnable body) {
        SdkTestEnv.startCapture();
        try {
            body.run();
        } finally {
            SdkTestEnv.stopCapture();
        }
        return SdkTestEnv.events();
    }

    private static Connection tracedConnection(Fake connFake) {
        return Dataflow.wrap((Connection) connProxy(connFake), "postgresql");
    }

    private static Object connProxy(Fake connFake) {
        return connFake.proxy(Connection.class);
    }

    @Test
    void executeQueryEmitsOneDbQuerySpan() {
        Fake stmtFake = new Fake();
        Fake connFake = new Fake().on("createStatement", stmtFake.proxy(Statement.class));
        Connection conn = tracedConnection(connFake);

        List<DataflowProto.TraceEvent> events = traced(() -> {
            try (Statement st = conn.createStatement()) {
                st.executeQuery("SELECT id, total\n  FROM orders WHERE id = $1");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(1, events.size());
        DataflowProto.TraceEvent e = events.get(0);
        assertEquals(DataflowProto.EventType.EVENT_TYPE_DB_QUERY, e.getType());
        assertEquals("SELECT orders", e.getName());
        assertEquals("postgresql", e.getCalleePackage());
        assertEquals("postgresql", e.getMetadataOrDefault("db.system", ""));
        assertEquals("SELECT id, total FROM orders WHERE id = $1", e.getMetadataOrDefault("db.statement", ""));
        assertTrue(stmtFake.calls.contains("executeQuery"));
        // close() passes through without an extra span.
        assertTrue(stmtFake.calls.contains("close"));
    }

    @Test
    void preparedStatementsReuseThePreparedSql() {
        Fake psFake = new Fake();
        Fake connFake = new Fake().on("prepareStatement", psFake.proxy(PreparedStatement.class));
        Connection conn = tracedConnection(connFake);

        List<DataflowProto.TraceEvent> events = traced(() -> {
            try {
                PreparedStatement ps = conn.prepareStatement("INSERT INTO public.users (email) VALUES (?)");
                ps.setString(1, "user@example.com"); // bind values must never be captured
                ps.executeUpdate();
                ps.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(1, events.size());
        DataflowProto.TraceEvent e = events.get(0);
        assertEquals("INSERT users", e.getName());
        assertEquals("INSERT INTO public.users (email) VALUES (?)", e.getMetadataOrDefault("db.statement", ""));
        // The bind value appears nowhere in the wire event.
        assertFalse(e.toString().contains("user@example.com"));
        assertTrue(psFake.calls.contains("setString"));
    }

    @Test
    void executeBatchOnPreparedStatementUsesPreparedSql() {
        Fake psFake = new Fake();
        Fake connFake = new Fake().on("prepareStatement", psFake.proxy(PreparedStatement.class));
        Connection conn = tracedConnection(connFake);

        List<DataflowProto.TraceEvent> events = traced(() -> {
            try {
                PreparedStatement ps = conn.prepareStatement("DELETE FROM sessions WHERE id = ?");
                ps.executeBatch();
                ps.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(1, events.size());
        assertEquals("DELETE sessions", events.get(0).getName());
    }

    @Test
    void statementSummaryVectorsThroughTheProxy() {
        Fake stmtFake = new Fake();
        Fake connFake = new Fake().on("createStatement", stmtFake.proxy(Statement.class));
        Connection conn = tracedConnection(connFake);

        List<DataflowProto.TraceEvent> events = traced(() -> {
            try (Statement st = conn.createStatement()) {
                st.executeUpdate("UPDATE public.items SET total = total - 1");
                st.execute("CREATE TABLE IF NOT EXISTS migrations (id int)");
                st.execute("BEGIN");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(3, events.size());
        assertEquals("UPDATE items", events.get(0).getName());
        assertEquals("CREATE migrations", events.get(1).getName());
        assertEquals("BEGIN", events.get(2).getName());
    }

    @Test
    void callableStatementsAreWrappedToo() {
        Fake csFake = new Fake();
        Fake connFake = new Fake().on("prepareCall", csFake.proxy(CallableStatement.class));
        Connection conn = tracedConnection(connFake);

        List<DataflowProto.TraceEvent> events = traced(() -> {
            try {
                CallableStatement cs = conn.prepareCall("CALL archive_old_orders()");
                cs.execute();
                cs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(1, events.size());
        assertEquals("CALL", events.get(0).getName());
    }

    @Test
    void driverSQLExceptionsPropagateUnchangedAndAreRecorded() {
        SQLException boom = new SQLException("boom");
        Fake stmtFake = new Fake().fails("executeQuery", boom);
        Fake connFake = new Fake().on("createStatement", stmtFake.proxy(Statement.class));
        Connection conn = tracedConnection(connFake);

        List<DataflowProto.TraceEvent> events = traced(() -> {
            SQLException thrown = assertThrows(SQLException.class,
                    () -> {
                        try {
                            conn.createStatement().executeQuery("SELECT * FROM orders");
                        } catch (SQLException e) {
                            throw e;
                        }
                    });
            assertSame(boom, thrown);
        });

        assertEquals(1, events.size());
        assertTrue(events.get(0).getErrorMessage().contains("SQLException: boom"));
    }

    @Test
    void passthroughMethodsNeverEmitSpans() throws Exception {
        Fake connFake = new Fake();
        Connection raw = (Connection) connProxy(connFake);
        connFake.on("unwrap", raw).on("isWrapperFor", Boolean.TRUE);
        Connection conn = tracedConnection(connFake);

        SdkTestEnv.startCapture();
        try {
            conn.setAutoCommit(false);
            conn.commit();
            conn.rollback();
            conn.setReadOnly(true);
            Connection unwrapped = conn.unwrap(Connection.class);
            assertSame(raw, unwrapped);
            assertTrue(conn.isWrapperFor(Connection.class));
            conn.close();
        } finally {
            SdkTestEnv.stopCapture();
        }

        assertTrue(connFake.calls.contains("setAutoCommit"));
        assertTrue(connFake.calls.contains("commit"));
        assertTrue(connFake.calls.contains("rollback"));
        assertTrue(connFake.calls.contains("unwrap"));
        assertTrue(connFake.calls.contains("close"));
        assertEquals(0, SdkTestEnv.events().size());
    }

    @Test
    void statementReturnedByWrappedConnectionIsItselfWrapped() throws Exception {
        // A driver Statement handed out through the wrapped Connection (the
        // fake returns the SAME Statement instance twice) still traces.
        Fake stmtFake = new Fake();
        Fake connFake = new Fake().on("createStatement", stmtFake.proxy(Statement.class));
        Connection conn = tracedConnection(connFake);
        Statement st = conn.createStatement();
        Statement st2 = conn.createStatement();
        assertTrue(st instanceof Statement);

        List<DataflowProto.TraceEvent> events = traced(() -> {
            try {
                st2.executeQuery("SELECT * FROM orders");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        assertEquals(1, events.size());
        assertEquals("SELECT orders", events.get(0).getName());
    }
}
