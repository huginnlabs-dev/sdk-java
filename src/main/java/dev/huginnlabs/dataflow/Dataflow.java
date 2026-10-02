package dev.huginnlabs.dataflow;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import dev.huginnlabs.dataflow.gen.DataflowProto;
import java.util.function.Consumer;

/**
 * HuginnLabs Dataflow SDK for Java — entry point.
 *
 * <p>Spans stream over gRPC ({@code StreamEvents}) to the ingestion API; a
 * background sender trims its replay buffer as the server acknowledges
 * durability. Configure once at startup — from environment variables:
 *
 * <pre>{@code
 * public static void main(String[] args) {
 *     Dataflow.configure();                 // reads DATAFLOW_* env
 *     try (Trace t = Dataflow.trace("app.Run")) {
 *         t.span().setData("job", "42");
 *     }
 * }
 * }</pre>
 *
 * <p>Env: {@code DATAFLOW_ENDPOINT} (grpc base, host:port),
 * {@code DATAFLOW_API_KEY}, {@code DATAFLOW_SERVICE_NAME},
 * {@code DATAFLOW_ENCRYPTION_KEY} (payloads stay plaintext when unset),
 * {@code DATAFLOW_SAMPLE_RATIO}, {@code DATAFLOW_BUFFER_SIZE},
 * {@code DATAFLOW_MAX_BODY_BYTES}, {@code DATAFLOW_ENV},
 * {@code DATAFLOW_APP_VERSION}, {@code DATAFLOW_HTTP_URL} (HTTP API base
 * for service-manifest reporting and log shipping), {@code DATAFLOW_DISABLED}.
 */
public final class Dataflow {

    /** SDK version stamped into agent metadata and the startup manifest. */
    public static final String SDK_VERSION = "0.7.0";

    /** Immutable SDK configuration. */
    public static final class Settings {
        public final String endpoint;
        public final String apiKey;
        public final String serviceName;
        public final String encryptionKey;
        public final double sampleRatio;
        public final int bufferSize;
        public final int maxBodyBytes;
        public final boolean disabled;
        public final Consumer<String> logger;

        Settings(Builder b) {
            this.endpoint = b.endpoint;
            this.apiKey = b.apiKey;
            this.serviceName = b.serviceName;
            this.encryptionKey = b.encryptionKey;
            this.sampleRatio = b.sampleRatio;
            this.bufferSize = b.bufferSize;
            this.maxBodyBytes = b.maxBodyBytes;
            this.disabled = b.disabled;
            this.logger = b.logger;
        }
    }

    /** Builder for {@link Settings} (programmatic configuration). */
    public static final class Builder {
        String endpoint;
        String apiKey;
        String serviceName = "";
        String encryptionKey = "";
        double sampleRatio = 1.0;
        int bufferSize = 10000;
        int maxBodyBytes = 8192;
        boolean disabled = false;
        Consumer<String> logger = System.err::println;

        public Builder endpoint(String v) { this.endpoint = v; return this; }
        public Builder apiKey(String v) { this.apiKey = v; return this; }
        public Builder serviceName(String v) { this.serviceName = v; return this; }
        public Builder encryptionKey(String v) { this.encryptionKey = v; return this; }
        public Builder sampleRatio(double v) { this.sampleRatio = v; return this; }
        public Builder bufferSize(int v) { this.bufferSize = v; return this; }
        public Builder maxBodyBytes(int v) { this.maxBodyBytes = v; return this; }
        public Builder disabled(boolean v) { this.disabled = v; return this; }
        public Builder logger(Consumer<String> v) { this.logger = v; return this; }

        public Settings build() { return new Settings(this); }
    }

    private static volatile Settings settings = null;
    private static final AtomicBoolean started = new AtomicBoolean(false);

    private Dataflow() {}

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }

    private static double envNum(String key, double fallback) {
        try {
            return Double.parseDouble(System.getenv().getOrDefault(key, ""));
        } catch (NumberFormatException | NullPointerException e) {
            return fallback;
        }
    }

    /**
     * Configures the SDK from {@code DATAFLOW_*} environment variables and
     * starts the background sender. Safe to call again with a custom builder
     * to override; reconfiguration replaces the pipeline wholesale.
     */
    public static synchronized void configure() {
        configure(new Builder()
                .endpoint(env("DATAFLOW_ENDPOINT", ""))
                .apiKey(env("DATAFLOW_API_KEY", ""))
                .serviceName(env("DATAFLOW_SERVICE_NAME", ""))
                .encryptionKey(env("DATAFLOW_ENCRYPTION_KEY", ""))
                .sampleRatio(envNum("DATAFLOW_SAMPLE_RATIO", 1.0))
                .bufferSize((int) envNum("DATAFLOW_BUFFER_SIZE", 10000))
                .maxBodyBytes((int) envNum("DATAFLOW_MAX_BODY_BYTES", 8192))
                .disabled("true".equalsIgnoreCase(env("DATAFLOW_DISABLED", "false"))));
    }

    public static synchronized void configure(Builder b) {
        // Idempotent: repeated configure() calls (e.g. from DataflowFilter's
        // constructor) must NOT restart the sender — two pipelines over one
        // replay buffer would deliver every span twice.
        settings = b.build();
        startOnce();
    }

    /** Current settings; passive defaults before configure() is called. */
    public static Settings settings() {
        Settings s = settings;
        return s != null ? s : new Builder().disabled(true).build();
    }

    /** True when spans are being collected and shipped. */
    public static boolean enabled() {
        Settings s = settings;
        return s != null && !s.disabled && !s.endpoint.isEmpty() && !s.apiKey.isEmpty();
    }

    /** The configured service name (defaults to the host name). */
    public static String serviceName() {
        String n = settings().serviceName;
        if (!n.isEmpty()) return n;
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static void startOnce() {
        if (!started.compareAndSet(false, true)) return;
        Settings s = settings();
        if (s.disabled || s.endpoint.isEmpty()) {
            s.logger.accept("dataflow: endpoint not configured; SDK stays passive (set DATAFLOW_ENDPOINT)");
            return;
        }
        if (s.apiKey.isEmpty()) {
            s.logger.accept("dataflow: no API key configured; SDK stays passive (set DATAFLOW_API_KEY)");
            return;
        }
        // Report the service manifest (runtime, framework, dependency
        // inventory) once; best-effort, independent of the tracing pipeline.
        Manifest.sendManifest();
        Pipeline.start(s);
        Logs.start(s);
    }

    /**
     * Suppresses the JVM-core manifest report. Language wrappers that send
     * their own manifest (Kotlin) call this before {@link #configure()} so
     * the catalog shows one entry per service, not a race of two.
     */
    public static void skipManifest() {
        Manifest.disableCore();
    }

    /**
     * Wraps a JDK {@link HttpClient} so every {@code send}/{@code sendAsync}
     * emits one HTTP_CLIENT span joined to the current trace and injects
     * {@code X-Dataflow-Trace-Id} (unless the caller set it) so the receiving
     * instrumented service continues the same trace. Best-effort: when the
     * SDK is disabled the wrapper passes through untouched. The wrapper is
     * deliberately not an {@code HttpClient} subtype (see
     * {@link TracedHttpClient}); reach the original client through
     * {@code TracedHttpClient#delegate()}.
     */
    public static TracedHttpClient instrument(java.net.http.HttpClient client) {
        return TracedHttpClient.create(client);
    }

    /**
     * Wraps an open JDBC {@link Connection} so every executed statement emits
     * one DB_QUERY span ({@code "<VERB> <table>"} name, {@code db.system} /
     * {@code db.statement} metadata). Only the statement text is captured —
     * never bind parameter values. {@code system} labels the database engine
     * ("postgresql", "mysql", …) as the span's callee package. Tracing is
     * decided per query, so wrapping before {@link #configure()} is safe.
     */
    public static java.sql.Connection wrap(java.sql.Connection conn, String system) {
        return Jdbc.wrap(conn, system);
    }

    /**
     * Runs {@code body} and records any crash on the current span — status
     * 500, the exception's {@code toString()} (clipped to 500 chars) as
     * error_message and its stack trace as {@code error.stack} metadata —
     * then rethrows it: nothing is swallowed. Checked throwables the
     * {@link Runnable} contract cannot declare are re-wrapped in
     * {@link RuntimeException}. With no span active a synthetic
     * {@code "exception"} span carries the record. When the SDK is disabled
     * the body simply runs.
     */
    public static void capture(Runnable body) {
        if (body == null) throw new NullPointerException("body");
        if (!enabled()) {
            body.run();
            return;
        }
        Crash.run(body);
    }

    /**
     * {@link #capture(Runnable)} for a {@link Callable}: the value comes
     * back on success and any throwable is recorded, then rethrown
     * unchanged — a {@link Callable} declares {@code throws Exception}, so
     * checked exceptions propagate as-is. When the SDK is disabled the body
     * simply runs.
     */
    public static <T> T captureCallable(Callable<T> body) throws Exception {
        if (body == null) throw new NullPointerException("body");
        if (!enabled()) return body.call();
        return Crash.call(body);
    }

    /**
     * Records uncaught exceptions JVM-wide: installs a default
     * uncaught-exception handler that records each thread death on a
     * synthetic {@code "uncaught exception"} span (same wire shape as
     * {@link #capture(Runnable)}) and then chains to the previously
     * installed handler — or reproduces the JVM's standard stderr report
     * when none was set. A hook cannot rethrow, so the crash still ends the
     * way it would have. Idempotent; no-op while the SDK is disabled. Pair
     * with {@link #ignoreUncaught()} to restore the previous handler.
     */
    public static void captureUncaught() {
        Crash.installUncaught();
    }

    /** Restores the uncaught-exception handler wrapped by {@link #captureUncaught()}. */
    public static void ignoreUncaught() {
        Crash.ignoreUncaught();
    }

    // -- application logs -----------------------------------------------------

    /**
     * Ships an {@code info} log line. The line is stamped with the current
     * span's trace/span ids (empty outside a trace), a millisecond timestamp
     * and the {@code fields} map stringified via {@code String.valueOf}
     * (max 50 entries). Buffered in a bounded queue and POSTed to
     * {@code /api/v1/logs} by a background flusher — best-effort: never
     * blocks or throws, drops the oldest line when the queue is full, and
     * is a no-op while the SDK is disabled.
     */
    public static void info(String message, Map<String, Object> fields) {
        Logs.record("info", message, fields);
    }

    /** {@link #info(String, Map)} at {@code warn} level. */
    public static void warn(String message, Map<String, Object> fields) {
        Logs.record("warn", message, fields);
    }

    /** {@link #info(String, Map)} at {@code error} level. */
    public static void error(String message, Map<String, Object> fields) {
        Logs.record("error", message, fields);
    }

    /** {@link #info(String, Map)} at {@code debug} level. */
    public static void debug(String message, Map<String, Object> fields) {
        Logs.record("debug", message, fields);
    }

    /**
     * Ships a log line at an arbitrary level, normalized onto the wire's
     * {@code debug|info|warn|error} (JUL names — {@code SEVERE},
     * {@code WARNING}, {@code FINE}, … — included; unknown labels report as
     * {@code info}). Best-effort like the level helpers.
     */
    public static void log(String level, String message, Map<String, Object> fields) {
        Logs.record(level, message, fields);
    }

    /**
     * Best-effort wait (up to ~5 seconds) until every buffered log line has
     * been POSTed — useful right before JVM exit. Never throws.
     */
    public static void flushLogs() {
        Logs.flush(5000);
    }

    /**
     * A {@link java.util.logging.Handler} that forwards JUL records into
     * the log pipeline: levels map onto the wire's names ({@code SEVERE}→
     * {@code error}, {@code WARNING}→{@code warn}, {@code INFO}→
     * {@code info}, {@code FINE}/ {@code FINER}/ {@code FINEST}→
     * {@code debug}) and message parameters ({@code "{0}"} placeholders)
     * are formatted; logger names, throwables and other extras are skipped.
     * Records nothing while the SDK is disabled. Install on the root
     * logger (or any logger):
     *
     * <pre>{@code
     * java.util.logging.Logger.getLogger("").addHandler(new Dataflow.LogHandler());
     * }</pre>
     */
    public static final class LogHandler extends java.util.logging.Handler {

        @Override public void publish(java.util.logging.LogRecord record) {
            if (record == null) return;
            Logs.jul(record);
        }

        /** No-op: the log flusher batches on its own schedule (or flushLogs()). */
        @Override public void flush() {}

        @Override public void close() {}
    }

    /**
     * Installs the {@link LogbackAppender} on the default SLF4J factory —
     * the Logback {@code LoggerContext} behind {@code LoggerFactory} — so
     * every Logback line (Spring Boot's default logging included) ships to
     * {@code /api/v1/logs} with trace/span correlation, level mapping
     * ({@code TRACE}/{@code DEBUG}→{@code debug}, {@code WARN}→{@code warn},
     * {@code ERROR}→{@code error}) and {@code {}}-parameter formatting.
     * Idempotent: an earlier install is detached first, never doubled. The
     * reverse of {@link #uninstallLogback()}.
     *
     * <p>Logback-classic is an {@code optional} / {@code provided} dependency
     * of the SDK: the appender class only loads when this is called, and the
     * call fails with {@code NoClassDefFoundError} when Logback is not on the
     * classpath (take it from the service's own dependency set — Spring Boot
     * ships it). Skips silently when SLF4J is bound to a non-Logback backend.
     */
    public static void installLogback() {
        LogbackAppender.installDefault();
    }

    /**
     * {@link #installLogback()} for an explicit Logback
     * {@code LoggerContext} (one the application manages itself instead of
     * the {@code LoggerFactory} default). Idempotent: an earlier install is
     * detached first.
     */
    public static void installLogback(ch.qos.logback.classic.LoggerContext ctx) {
        LogbackAppender.install(ctx);
    }

    /**
     * Detaches and stops the appender installed by {@link #installLogback()}
     * / {@link #installLogback(ch.qos.logback.classic.LoggerContext)}. No-op
     * (never throws) when nothing is installed.
     */
    public static void uninstallLogback() {
        LogbackAppender.uninstall();
    }

    /** Opens a child span of the current thread's span (or a new trace). */
    public static Span startSpan(String name) {
        return startSpan(name, "FUNCTION_CALL");
    }

    public static Span startSpan(String name, String type) {
        return new Span(name, type, Span.current());
    }

    /** Opens an entry-point (HTTP_SERVER) span; used by the middleware. */
    public static Span startServerSpan(String name) {
        return new Span(name, "HTTP_SERVER", null);
    }

    /**
     * Runs {@code body} inside a named span, joining the current trace.
     * The span ends (and is queued for delivery) when the scope closes.
     */
    public static Trace trace(String name) {
        return new Trace(name, "FUNCTION_CALL");
    }

    static void enqueue(DataflowProto.TraceEvent event) {
        Pipeline.enqueue(event);
    }
}
