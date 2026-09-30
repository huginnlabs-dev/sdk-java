package dev.huginnlabs.dataflow;

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
 * for service-manifest reporting), {@code DATAFLOW_DISABLED}.
 */
public final class Dataflow {

    /** SDK version stamped into agent metadata and the startup manifest. */
    public static final String SDK_VERSION = "0.2.0";

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
