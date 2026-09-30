package dev.huginnlabs.dataflow;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import dev.huginnlabs.dataflow.gen.DataflowProto;
import java.util.concurrent.ThreadLocalRandom;

/**
 * One measured unit of work. Create via {@link Dataflow#startSpan} /
 * {@link Dataflow#trace}; end() submits the completed event to the delivery
 * pipeline. Payload field names travel as plaintext metadata (lineage +
 * PII categories); values are AES-256-GCM encrypted when a key is set.
 */
public final class Span {

    private static final ThreadLocal<Span> CURRENT = new ThreadLocal<>();

    /** The span active on this thread, or null. */
    public static Span current() { return CURRENT.get(); }

    static String newId() { return UUID.randomUUID().toString().replace("-", ""); }

    private String traceId;
    private final String spanId;
    private final String parentSpanId;
    private final String name;
    private final String type;
    private final String callerPackage;
    private volatile String calleePackage = "";
    private final long startMillis = Instant.now().toEpochMilli();
    private final long startNanos = System.nanoTime();

    private final Object lock = new Object();
    private final Map<String, String> metadata = new LinkedHashMap<>();
    private final Map<String, Object> payload = new LinkedHashMap<>();
    private String errorMessage = "";
    private int statusCode;
    private boolean ended;
    private final boolean sampled;

    Span(String name, String type, Span parent) {
        this.name = name == null ? "" : name;
        this.type = type == null ? "FUNCTION_CALL" : type;
        this.traceId = parent != null ? parent.traceId : newId();
        this.spanId = newId();
        this.parentSpanId = parent != null ? parent.spanId : "";
        // Package attribution mirrors the C++/Python SDKs: the callee comes
        // from the "pkg.Func" name prefix (or an explicit callee() call);
        // the caller is the enclosing span's callee. Roots keep caller
        // empty — caller==callee self-edges break the flow-graph layout.
        if (parent != null) this.callerPackage = parent.calleePackage;
        else this.callerPackage = "";
        if ("FUNCTION_CALL".equals(this.type) && this.calleePackage.isEmpty()) {
            int dot = this.name.indexOf('.');
            if (dot > 0) this.calleePackage = this.name.substring(0, dot);
        }
        double ratio = Dataflow.settings().sampleRatio;
        this.sampled = ratio >= 1.0 || ThreadLocalRandom.current().nextDouble() < ratio;
        if (!sampled) ended = true;
    }

    public String traceId() { return traceId; }
    public String spanId() { return spanId; }

    /** Metric-grade plaintext attribute (metadata entry). */
    public Span attr(String key, String value) {
        synchronized (lock) {
            metadata.put(key, value == null ? "" : value);
        }
        return this;
    }

    /** Payload field with a scalar/JSON-ish value; encrypted at end(). */
    public Span data(String key, Object value) {
        synchronized (lock) {
            payload.put(key, value);
        }
        return this;
    }

    /** Payload field holding an already-serialized JSON value. */
    public Span dataJson(String key, String jsonValue) {
        return data(key, new Json.Raw(jsonValue == null ? "null" : jsonValue));
    }

    public Span recordError(String message) {
        if (message == null) return this;
        synchronized (lock) {
            if (errorMessage.isEmpty()) errorMessage = message;
            else errorMessage = errorMessage + "; " + message;
        }
        return this;
    }

    public Span recordError(Throwable t) {
        return recordError(t == null ? null : t.getClass().getSimpleName() + ": " + t.getMessage());
    }

    /** HTTP status or gRPC code. */
    public Span status(int code) {
        synchronized (lock) { statusCode = code; }
        return this;
    }

    /** Marks this span's package (or host) for the data-flow graph. */
    public Span callee(String pkg) {
        if (pkg != null && !pkg.isEmpty()) this.calleePackage = pkg;
        return this;
    }

    /** Ends the span and enqueues it for delivery. Idempotent. */
    public void end() {
        synchronized (lock) {
            if (ended) return;
            ended = true;
        }
        if (!sampled) return;

        long durationMs = (System.nanoTime() - startNanos) / 1_000_000L;

        DataflowProto.TraceEvent.Builder b = DataflowProto.TraceEvent.newBuilder()
                .setEventId(newId())
                .setSeq(Pipeline.nextSeq())
                .setTimestamp(startMillis)
                .setDurationMs(durationMs)
                .setType(typeOf(type))
                .setServiceName(Dataflow.serviceName())
                .setName(name)
                .setStatusCode(statusCode)
                .setErrorMessage(errorMessage)
                .setTraceId(traceId)
                .setSpanId(spanId)
                .setParentSpanId(parentSpanId);

        Map<String, String> meta;
        Map<String, Object> data;
        synchronized (lock) {
            meta = new LinkedHashMap<>(metadata);
            data = new LinkedHashMap<>(payload);
        }
        b.setCallerPackage(callerPackage);
        if (!calleePackage.isEmpty()) b.setCalleePackage(calleePackage);

        // Field-name lineage + client-side PII classification: key names
        // (never values) travel as plaintext metadata.
        if (!data.isEmpty()) {
            SortedSet<String> keys = new TreeSet<>(data.keySet());
            meta.put("data.fields", String.join(",", keys));
            String pii = Pii.classify(keys);
            if (!pii.isEmpty()) meta.put("data.pii", pii);
        }
        b.putAllMetadata(meta);

        if (!data.isEmpty()) {
            b.setPayload(Crypto.seal(data));
        }
        Dataflow.enqueue(b.build());
    }

    private static DataflowProto.EventType typeOf(String t) {
        switch (t) {
            case "HTTP_SERVER": return DataflowProto.EventType.EVENT_TYPE_HTTP_SERVER;
            case "HTTP_CLIENT": return DataflowProto.EventType.EVENT_TYPE_HTTP_CLIENT;
            case "GRPC": return DataflowProto.EventType.EVENT_TYPE_GRPC;
            case "LLM_CALL": return DataflowProto.EventType.EVENT_TYPE_LLM_CALL;
            case "DB_QUERY": return DataflowProto.EventType.EVENT_TYPE_DB_QUERY;
            default: return DataflowProto.EventType.EVENT_TYPE_FUNCTION_CALL;
        }
    }

    void activate() { CURRENT.set(this); }
    void deactivate() { CURRENT.remove(); }

    /** Adopts an incoming trace id (X-Dataflow-Trace-Id propagation). */
    void joinTrace(String id) { if (id != null && !id.isEmpty()) this.traceId = id; }

    // Package-private test seams (tests live in the same package).
    String name() { return name; }
    String type() { return type; }
    String calleePackage() { return calleePackage; }
    int statusCode() { return statusCode; }
    String errorMessage() { return errorMessage; }
    Map<String, String> metadataCopy() {
        synchronized (lock) { return new LinkedHashMap<>(metadata); }
    }
}
