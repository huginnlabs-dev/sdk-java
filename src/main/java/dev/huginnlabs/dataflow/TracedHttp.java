package dev.huginnlabs.dataflow;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Outgoing HTTP calls recorded as HTTP_CLIENT spans joined to the current
 * trace, carrying the {@code X-Dataflow-Trace-Id} header so the receiving
 * instrumented service continues the same trace.
 */
public final class TracedHttp {

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private TracedHttp() {}

    public static HttpResponse<String> get(String url) throws Exception {
        return send("GET", url, null, null);
    }

    public static HttpResponse<String> post(String url, String jsonBody) throws Exception {
        return send("POST", url, jsonBody, "application/json");
    }

    public static HttpResponse<String> send(String method, String url, String body, String contentType) throws Exception {
        URI uri = URI.create(url);
        String authority = uri.getHost() + (uri.getPort() > 0 ? ":" + uri.getPort() : "");

        try (Trace t = new Trace(method + " " + authority + uri.getPath(), "HTTP_CLIENT")) {
            Span span = t.span();
            span.callee(authority);
            span.attr("http.method", method);
            span.attr("http.url", url);

            HttpRequest.Builder rb = HttpRequest.newBuilder()
                    .uri(uri)
                    .timeout(Duration.ofSeconds(30))
                    .header("X-Dataflow-Trace-Id", span.traceId());
            if (contentType != null) rb.header("Content-Type", contentType);
            HttpRequest request = finish(rb, method, body);
            try {
                HttpResponse<String> resp = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
                span.status(resp.statusCode());
                span.attr("http.status_code", String.valueOf(resp.statusCode()));
                return resp;
            } catch (java.io.IOException e) {
                span.recordError(e);
                throw e;
            } catch (InterruptedException e) {
                span.recordError(e);
                Thread.currentThread().interrupt();
                throw e;
            }
        }
    }

    private static HttpRequest finish(HttpRequest.Builder rb, String method, String body) {
        if (body == null) {
            if ("GET".equals(method)) return rb.GET().build();
            return rb.method(method, HttpRequest.BodyPublishers.noBody()).build();
        }
        return rb.method(method, HttpRequest.BodyPublishers.ofString(body)).build();
    }
}
