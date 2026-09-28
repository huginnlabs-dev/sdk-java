package dev.huginnlabs.dataflow;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.HttpExchange;

/**
 * Instrumentation for the JDK built-in HTTP server
 * ({@code com.sun.net.httpserver}): wraps every request into an
 * HTTP_SERVER span with agent metadata, redacted request headers and an
 * optional body excerpt, then records status and response size.
 *
 * <pre>{@code
 * HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
 * server.createContext("/").getFilters().add(new DataflowFilter());
 * server.createContext("/", exchange -> { ... });
 * }</pre>
 *
 * Handlers inside the exchange join the trace via {@link Dataflow#trace}.
 * For Spring/Servlet stacks, apply the same idea with a servlet Filter
 * (start a server span, {@code span.status(response.getStatus())}).
 */
public class DataflowFilter extends Filter {

    private static final List<String> REDACTED = List.of(
            "authorization", "proxy-authorization", "cookie", "set-cookie", "x-api-key");

    public DataflowFilter() {
        Dataflow.configure();
    }

    @Override public String description() {
        return "huginnlabs dataflow tracing";
    }

    @Override public void doFilter(HttpExchange exchange, Chain chain) throws IOException {
        if (!Dataflow.enabled()) {
            chain.doFilter(exchange);
            return;
        }
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        String query = exchange.getRequestURI().getRawQuery();

        Span span = Dataflow.startServerSpan(method + " " + path);
        String incoming = exchange.getRequestHeaders().getFirst("X-Dataflow-Trace-Id");
        if (incoming != null && !incoming.isEmpty()) {
            span.joinTrace(incoming);
        }
        Agent.stamp(span);
        span.attr("http.method", method);
        span.attr("http.path", path);
        if (query != null && !query.isEmpty()) span.attr("http.query", query);
        span.attr("http.remote_addr", String.valueOf(exchange.getRemoteAddress()));
        for (Map.Entry<String, List<String>> h : exchange.getRequestHeaders().entrySet()) {
            String lower = h.getKey() == null ? "" : h.getKey().toLowerCase();
            if (lower.isEmpty()) continue;
            String value = REDACTED.contains(lower) ? "[REDACTED]" : String.join(", ", h.getValue());
            span.attr("http.header." + lower, value);
        }
        captureBody(exchange, span);

        RecordingExchange wrapped = new RecordingExchange(exchange);
        span.activate();
        try {
            chain.doFilter(wrapped);
        } catch (IOException | RuntimeException e) {
            span.recordError(e);
            throw e;
        } finally {
            int status = wrapped.statusCode;
            span.status(status);
            span.attr("http.status_code", String.valueOf(status));
            span.attr("http.response_bytes", String.valueOf(wrapped.bytesSent));
            if (status >= 500) span.recordError("http " + status);
            span.end();
            span.deactivate();
        }
    }

    /** Captures up to maxBodyBytes of the request body into the span payload. */
    private static void captureBody(HttpExchange exchange, Span span) {
        int limit = Dataflow.settings().maxBodyBytes;
        if (limit <= 0) return;
        try {
            InputStream in = exchange.getRequestBody();
            byte[] excerpt = in.readNBytes(limit);
            if (excerpt.length == 0) return;
            exchange.setStreams(
                    new java.io.SequenceInputStream(new java.io.ByteArrayInputStream(excerpt), in),
                    exchange.getResponseBody());
            span.data("request", java.util.Map.of(
                    "body_excerpt", new String(excerpt, StandardCharsets.UTF_8),
                    "truncated", excerpt.length >= limit));
        } catch (IOException e) {
            // keep serving; tracing must never break the request
        }
    }

    /** Records the response status code and byte count for the span. */
    static final class RecordingExchange extends HttpExchange {
        private final HttpExchange delegate;
        int statusCode = 200;
        long bytesSent;

        RecordingExchange(HttpExchange delegate) { this.delegate = delegate; }

        @Override public void sendResponseHeaders(int rCode, long responseLength) throws IOException {
            statusCode = rCode;
            delegate.sendResponseHeaders(rCode, responseLength);
        }

        @Override public int getResponseCode() { return statusCode; }

        @Override public OutputStream getResponseBody() {
            OutputStream out = delegate.getResponseBody();
            return new FilterOutputStream(out) {
                @Override public void write(byte[] b, int off, int len) throws IOException {
                    out.write(b, off, len);
                    bytesSent += len;
                }
                @Override public void write(int b) throws IOException {
                    out.write(b);
                    bytesSent++;
                }
            };
        }

        @Override public void close() { delegate.close(); }
        @Override public com.sun.net.httpserver.Headers getRequestHeaders() { return delegate.getRequestHeaders(); }
        @Override public com.sun.net.httpserver.Headers getResponseHeaders() { return delegate.getResponseHeaders(); }
        @Override public java.net.URI getRequestURI() { return delegate.getRequestURI(); }
        @Override public String getRequestMethod() { return delegate.getRequestMethod(); }
        @Override public com.sun.net.httpserver.HttpContext getHttpContext() { return delegate.getHttpContext(); }
        @Override public void setStreams(InputStream i, OutputStream o) { delegate.setStreams(i, o); }
        @Override public InputStream getRequestBody() { return delegate.getRequestBody(); }
        @Override public java.net.InetSocketAddress getRemoteAddress() { return delegate.getRemoteAddress(); }
        @Override public java.net.InetSocketAddress getLocalAddress() { return delegate.getLocalAddress(); }
        @Override public String getProtocol() { return delegate.getProtocol(); }
        @Override public Object getAttribute(String name) { return delegate.getAttribute(name); }
        @Override public void setAttribute(String name, Object value) { delegate.setAttribute(name, value); }
        @Override public com.sun.net.httpserver.HttpPrincipal getPrincipal() { return delegate.getPrincipal(); }
    }
}
