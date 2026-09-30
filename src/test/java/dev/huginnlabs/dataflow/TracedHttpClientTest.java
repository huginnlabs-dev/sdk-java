package dev.huginnlabs.dataflow;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;

import dev.huginnlabs.dataflow.gen.DataflowProto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HTTP wrapper span logic, exercised through package-private seams so no
 * network is involved.
 */
@TestClassOrder(ClassOrderer.ClassName.class)
class TracedHttpClientTest {

    @BeforeAll
    static void enableSdk() {
        SdkTestEnv.configure();
    }

    private static HttpRequest request(String url) {
        return HttpRequest.newBuilder(URI.create(url)).GET().build();
    }

    @Test
    void beginCreatesHttpClientSpanJoinedToCurrentTrace() {
        TracedHttpClient client = TracedHttpClient.create(HttpClient.newHttpClient());
        try (Trace parent = Dataflow.trace("job.Run")) {
            SdkTestEnv.startCapture();
            try {
                Span span = client.begin(request("https://api.example.com/v1/items?id=3"));
                assertEquals("GET api.example.com/v1/items", span.name());
                assertEquals("HTTP_CLIENT", span.type());
                assertEquals("api.example.com", span.calleePackage());
                assertEquals("GET", span.metadataCopy().get("http.method"));
                assertEquals("https://api.example.com/v1/items?id=3", span.metadataCopy().get("http.url"));
                assertEquals(parent.span().traceId(), span.traceId());
                span.end();
            } finally {
                SdkTestEnv.stopCapture();
            }
        }
        assertEquals(1, SdkTestEnv.events().size());
        assertEquals(DataflowProto.EventType.EVENT_TYPE_HTTP_CLIENT,
                SdkTestEnv.events().get(0).getType());
    }

    @Test
    void spanNameKeepsNonDefaultPortAndDefaultsRootPath() {
        TracedHttpClient client = TracedHttpClient.create(HttpClient.newHttpClient());
        Span span = client.begin(request("http://api.example.com:8443"));
        assertEquals("GET api.example.com:8443/", span.name());
        assertEquals("api.example.com", span.calleePackage());
        span.end();
    }

    @Test
    void spanIsCreatedWhenSdkEnabled() {
        // (The disabled passthrough case lives in DataflowDisabledTest, which
        // runs before any configure() call.)
        TracedHttpClient client = TracedHttpClient.create(HttpClient.newHttpClient());
        Span span = client.begin(request("https://api.example.com/x"));
        assertTrue(span != null && !span.traceId().isEmpty());
        span.end();
    }

    @Test
    void injectAddsTraceHeaderOnlyWhenAbsent() {
        TracedHttpClient client = TracedHttpClient.create(HttpClient.newHttpClient());
        HttpRequest req = request("https://api.example.com/v1/items");
        Span span = client.begin(req);

        HttpRequest out = TracedHttpClient.inject(req, span);
        assertEquals(span.traceId(), out.headers().firstValue("X-Dataflow-Trace-Id").orElse(""));

        // Header already present (any case): untouched.
        HttpRequest preset = HttpRequest.newBuilder(URI.create("https://api.example.com/v1/items"))
                .header("x-dataflow-trace-id", "trace-from-upstream").GET().build();
        HttpRequest out2 = TracedHttpClient.inject(preset, span);
        assertSame(preset, out2);
        assertEquals("trace-from-upstream", out2.headers().firstValue("X-Dataflow-Trace-Id").orElse(""));

        span.end();
    }

    @Test
    void injectWithNullSpanIsIdentity() {
        HttpRequest req = request("https://api.example.com/v1/items");
        assertSame(req, TracedHttpClient.inject(req, null));
    }

    @Test
    void finishRecordsStatusThenEndsOnce() {
        TracedHttpClient client = TracedHttpClient.create(HttpClient.newHttpClient());
        Span span = client.begin(request("https://api.example.com/v1/items"));
        SdkTestEnv.startCapture();
        try {
            TracedHttpClient.finish(span, 201, null);
        } finally {
            SdkTestEnv.stopCapture();
        }
        assertEquals(1, SdkTestEnv.events().size());
        assertEquals(201, SdkTestEnv.events().get(0).getStatusCode());
        assertEquals("", SdkTestEnv.events().get(0).getErrorMessage());
    }

    @Test
    void finishRecordsErrors() {
        TracedHttpClient client = TracedHttpClient.create(HttpClient.newHttpClient());
        Span span = client.begin(request("https://api.example.com/v1/items"));
        SdkTestEnv.startCapture();
        try {
            TracedHttpClient.finish(span, 0, new java.io.IOException("no route to host"));
        } finally {
            SdkTestEnv.stopCapture();
        }
        assertEquals(1, SdkTestEnv.events().size());
        assertTrue(SdkTestEnv.events().get(0).getErrorMessage().contains("IOException: no route to host"));
    }

    @Test
    void delegateRoundTrips() {
        HttpClient plain = HttpClient.newHttpClient();
        TracedHttpClient client = TracedHttpClient.create(plain);
        assertEquals(plain, client.delegate());
    }
}
