package dev.huginnlabs.dataflow;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JDBC tracing via {@link java.lang.reflect.Proxy}: every executed statement
 * emits one DB_QUERY span joined to the current trace.
 *
 * <pre>{@code
 * try (Connection conn = Dataflow.wrap(driverManager.getConnection(url), "postgresql")) {
 *     try (Statement st = conn.createStatement()) {
 *         st.executeQuery("SELECT id FROM orders WHERE ...");
 *     }
 * }
 * }</pre>
 *
 * <p>Span shape: name {@code "<VERB> <table>"} derived from the SQL
 * ({@link #stmtSummary}), callee package the db system, metadata
 * {@code db.system} and {@code db.statement} (single-spaced, capped at 200
 * chars). Only the statement text is captured — never bind parameter values.
 * {@code close}, {@code unwrap}, transaction control and everything else
 * pass straight through to the driver; SQLExceptions propagate unchanged.
 * Whether a query is traced is decided per execution, so wrapping before
 * {@link Dataflow#configure()} is safe.
 */
final class Jdbc {

    private Jdbc() {}

    /**
     * Wraps an open connection so executed statements emit DB_QUERY spans.
     * {@code system} labels the database engine ("postgresql", "mysql", …)
     * and becomes the span's callee package. Returns {@code conn} unchanged
     * when null; never throws.
     */
    static Connection wrap(Connection conn, String system) {
        if (conn == null) return null;
        try {
            return (Connection) Proxy.newProxyInstance(
                    Jdbc.class.getClassLoader(),
                    new Class<?>[]{Connection.class},
                    new Handler(conn, system, null));
        } catch (RuntimeException e) {
            return conn; // tracing must never break acquisition
        }
    }

    private static final class Handler implements InvocationHandler {
        private final Object target;
        private final String system;
        /** SQL of the prepared statement (or last addBatch(String)), if known. */
        private volatile String preparedSql;

        Handler(Object target, String system, String preparedSql) {
            this.target = target;
            this.system = system == null ? "" : system;
            this.preparedSql = preparedSql;
        }

        @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            // Proxy identity methods never reach the driver.
            if (name.equals("toString")) {
                return "dataflow-traced " + target.getClass().getName() + "@" + target.hashCode();
            }
            if (name.equals("hashCode")) return target.hashCode();
            if (name.equals("equals")) return proxy == args[0];

            String arg0 = args != null && args.length > 0 && args[0] instanceof String
                    ? (String) args[0] : null;

            // prepareStatement/prepareCall: wrap the statement, remembering
            // its SQL so later executeBatch()/executeQuery() name the span.
            if (name.equals("prepareStatement") || name.equals("prepareCall")) {
                return wrapIfStatement(passThrough(method, args), system, arg0);
            }
            // Statement.addBatch(String): batch text for a later executeBatch.
            if (name.equals("addBatch") && arg0 != null) {
                preparedSql = arg0;
            }

            if (isExecute(name)) {
                String sql = arg0 != null ? arg0 : preparedSql;
                Span span = beginDbSpan(sql);
                try {
                    Object result = passThrough(method, args);
                    endQuietly(span, null);
                    return wrapIfStatement(result, system, null);
                } catch (Throwable t) {
                    endQuietly(span, t);
                    throw rethrow(t);
                }
            }

            // Everything else (close, unwrap, transaction control, metadata,
            // createStatement, ...) passes straight through; statement-shaped
            // results come back wrapped so their executes are traced too.
            return wrapIfStatement(passThrough(method, args), system, null);
        }

        private Object passThrough(Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(target, args);
            } catch (InvocationTargetException e) {
                throw rethrow(e.getCause() != null ? e.getCause() : e);
            }
        }

        /** Driver exceptions propagate unchanged; other checked ones re-wrap. */
        private static Throwable rethrow(Throwable cause) throws Throwable {
            if (cause instanceof SQLException || cause instanceof RuntimeException || cause instanceof Error) {
                throw cause;
            }
            throw new SQLException(cause);
        }

        private static boolean isExecute(String name) {
            switch (name) {
                case "executeQuery":
                case "execute":
                case "executeUpdate":
                case "executeLargeUpdate":
                case "executeBatch":
                case "executeLargeBatch":
                    return true;
                default:
                    return false;
            }
        }

        /** DB_QUERY span, or null when the SDK is disabled. Never throws. */
        private Span beginDbSpan(String sql) {
            if (!Dataflow.enabled()) return null;
            try {
                Span span = Dataflow.startSpan(stmtSummary(sql), "DB_QUERY");
                if (!system.isEmpty()) span.callee(system);
                span.attr("db.system", system);
                String stmt = clipStatement(sql);
                if (!stmt.isEmpty()) span.attr("db.statement", stmt);
                return span;
            } catch (RuntimeException e) {
                return null; // tracing must never break the query
            }
        }

        private static void endQuietly(Span span, Throwable error) {
            if (span == null) return;
            try {
                if (error != null) span.recordError(error);
            } catch (RuntimeException ignore) {
                // best-effort bookkeeping
            }
            try {
                span.end();
            } catch (RuntimeException ignore) {
                // best-effort bookkeeping
            }
        }

        /** Re-wraps Statement/PreparedStatement/CallableStatement results. */
        private static Object wrapIfStatement(Object result, String system, String sql) {
            if (!(result instanceof Statement)) return result;
            Class<?> iface = result instanceof CallableStatement ? CallableStatement.class
                    : result instanceof PreparedStatement ? PreparedStatement.class
                    : Statement.class;
            try {
                return Proxy.newProxyInstance(
                        Jdbc.class.getClassLoader(),
                        new Class<?>[]{iface},
                        new Handler(result, system, sql));
            } catch (RuntimeException e) {
                return result; // tracing must never break the call
            }
        }
    }

    // -----------------------------------------------------------------------
    // SQL: statement summarizing (mirrors the Go SDK's sqltrace.go rules and
    // the Kotlin sibling's Transport.kt).
    // -----------------------------------------------------------------------

    private static final Pattern STMT_VERB = Pattern.compile(
            "(?is)^\\s*\\(?\\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|"
                    + "WITH|BEGIN|COMMIT|ROLLBACK|SET|CALL|EXEC|SHOW|EXPLAIN|PRAGMA)\\b");

    private static final Pattern STMT_TABLE = Pattern.compile(
            "(?i)\\b(?:FROM|INTO|UPDATE|TABLE|JOIN)\\s+(?:IF\\s+(?:NOT\\s+)?EXISTS\\s+)?[`\"'\\[]?([A-Za-z_][\\w$.]*)");

    /** Wire cap on the db.statement metadata value. */
    static final int STATEMENT_CAP = 200;

    /**
     * Short human name for a statement: the verb plus the first table
     * reference when one exists ("SELECT orders", "INSERT users",
     * "CREATE migrations" — {@code IF [NOT] EXISTS} skipped,
     * "public.items" reported as "items"); bare verbs and non-SQL fall back
     * to the first word ("PRAGMA") or "QUERY".
     */
    static String stmtSummary(String query) {
        String one = oneLined(query);
        Matcher verb = STMT_VERB.matcher(one);
        if (!verb.find()) {
            int i = firstBreak(one);
            return i > 0 ? one.substring(0, i).toUpperCase(Locale.ROOT) : "QUERY";
        }
        Matcher table = STMT_TABLE.matcher(one);
        if (!table.find()) return verb.group(1).toUpperCase(Locale.ROOT);
        // Schema-qualified names ("public.items") report the bare table.
        String t = table.group(1);
        int dot = t.lastIndexOf('.');
        if (dot >= 0) t = t.substring(dot + 1);
        int dollar = t.lastIndexOf('$');
        if (dollar >= 0) t = t.substring(dollar + 1);
        return verb.group(1).toUpperCase(Locale.ROOT) + " " + t;
    }

    /** The statement text single-spaced and capped at {@link #STATEMENT_CAP} chars. */
    static String clipStatement(String query) {
        String one = oneLined(query);
        return one.length() > STATEMENT_CAP ? one.substring(0, STATEMENT_CAP) : one;
    }

    /** Collapses all whitespace runs to single spaces (Go strings.Fields+Join). */
    private static String oneLined(String query) {
        if (query == null) return "";
        String trimmed = query.trim();
        if (trimmed.isEmpty()) return "";
        return String.join(" ", trimmed.split("\\s+"));
    }

    private static int firstBreak(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '(') return i;
        }
        return -1;
    }
}
