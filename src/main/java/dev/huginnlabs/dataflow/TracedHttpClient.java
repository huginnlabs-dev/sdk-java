package dev.huginnlabs.dataflow;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

/**
 * Delegating wrapper around a JDK {@link HttpClient} that records every
 * {@code send}/{@code sendAsync} as one HTTP_CLIENT span joined to the
 * current trace and injects {@code X-Dataflow-Trace-Id} so the receiving
 * instrumented service continues the same trace:
 *
 * <pre>{@code
 * TracedHttpClient client = Dataflow.instrument(HttpClient.newHttpClient());
 * client.send(request, HttpResponse.BodyHandlers.ofString());
 * }</pre>
 *
 * <p>Span shape: name {@code "METHOD host[:port]/path"}, callee package the
 * host, {@code status_code} the HTTP status, metadata {@code http.method} /
 * {@code http.url}. The trace header is only added when the caller has not
 * set one (header names compare case-insensitively). Best-effort: when the
 * SDK is disabled the wrapper passes through untouched, and tracing
 * bookkeeping never throws into caller code.
 *
 * <p>Deliberately <em>not</em> an {@code HttpClient} subtype: the JDK
 * runtimes this SDK must build on differ in the {@code java.net.http}
 * type hierarchy (interface vs. abstract class), so the wrapper holds the
 * delegate by composition and mirrors only the stable {@code send} /
 * {@code sendAsync} surface — the same trade-off as the Kotlin SDK's
 * okhttp wrapper.
 */
public final class TracedHttpClient {

    /** Trace-propagation header, shared with the server-side DataflowFilter. */
    static final String TRACE_HEADER = "X-Dataflow-Trace-Id";

    private final HttpClient delegate;

    private TracedHttpClient(HttpClient delegate) {
        this.delegate = delegate;
    }

    /** Wraps {@code delegate}; all requests then emit HTTP_CLIENT spans. */
    public static TracedHttpClient create(HttpClient delegate) {
        if (delegate == null) throw new NullPointerException("delegate");
        return new TracedHttpClient(delegate);
    }

    /**
     * The wrapped client (for callers that need the original instance).
     * Configuration accessors are not mirrored — obtain them from here.
     */
    public HttpClient delegate() {
        return delegate;
    }

    // -- traced paths -------------------------------------------------------

    public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> bodyHandler)
            throws IOException, InterruptedException {
        Span span = begin(request);
        HttpRequest out = inject(request, span);
        try {
            HttpResponse<T> resp = delegate.send(out, bodyHandler);
            finish(span, resp == null ? 0 : resp.statusCode(), null);
            return resp;
        } catch (IOException | InterruptedException | RuntimeException e) {
            finish(span, 0, e);
            throw e;
        }
    }

    public <T> CompletableFuture<HttpResponse<T>> sendAsync(
            HttpRequest request, HttpResponse.BodyHandler<T> bodyHandler) {
        Span span = begin(request);
        HttpRequest out = inject(request, span);
        return delegate.sendAsync(out, bodyHandler)
                .whenComplete((resp, err) -> finish(span, resp == null ? 0 : resp.statusCode(), err));
    }

    public <T> CompletableFuture<HttpResponse<T>> sendAsync(
            HttpRequest request,
            HttpResponse.BodyHandler<T> bodyHandler,
            HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
        Span span = begin(request);
        HttpRequest out = inject(request, span);
        return delegate.sendAsync(out, bodyHandler, pushPromiseHandler)
                .whenComplete((resp, err) -> finish(span, resp == null ? 0 : resp.statusCode(), err));
    }

    // -- package-private seams (unit-tested without a network) --------------

    /** Opens the HTTP_CLIENT span for this request; null when disabled. */
    Span begin(HttpRequest request) {
        if (!Dataflow.enabled()) return null;
        try {
            java.net.URI uri = request.uri();
            String authority = uri.getHost() + (uri.getPort() > 0 ? ":" + uri.getPort() : "");
            String path = uri.getPath() == null || uri.getPath().isEmpty() ? "/" : uri.getPath();
            Span span = Dataflow.startSpan(request.method() + " " + authority + path, "HTTP_CLIENT");
            span.callee(uri.getHost());
            span.attr("http.method", request.method());
            span.attr("http.url", uri.toString());
            return span;
        } catch (RuntimeException e) {
            return null; // tracing must never break the call
        }
    }

    /**
     * Rebuilds the request with the trace header — unless the caller set
     * one already (any case). Copies the request via the filtering
     * {@code newBuilder(request, predicate)} overload, keeping every
     * existing header.
     */
    static HttpRequest inject(HttpRequest request, Span span) {
        if (span == null) return request;
        try {
            if (request.headers().firstValue(TRACE_HEADER).isPresent()) return request;
            return HttpRequest.newBuilder(request, (name, value) -> true)
                    .setHeader(TRACE_HEADER, span.traceId())
                    .build();
        } catch (RuntimeException e) {
            return request;
        }
    }

    /** Records status/error and ends the span. Never throws. */
    static void finish(Span span, int statusCode, Throwable error) {
        if (span == null) return;
        try {
            if (error != null) span.recordError(error);
            else span.status(statusCode);
        } catch (RuntimeException ignore) {
            // best-effort bookkeeping
        }
        try {
            span.end();
        } catch (RuntimeException ignore) {
            // best-effort bookkeeping
        }
    }
}
