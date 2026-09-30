# Dataflow Java SDK

Runtime tracing for JVM services. Streams completed spans over **gRPC**
(`StreamEvents` + ack-watermark replay buffer) — the same wire contract as
the Go and Python SDKs.

- Java 17+, zero third-party dependencies besides `grpc-java` / `protobuf-java`
- AES-256-GCM payload encryption (PBKDF2-SHA256, 10k iterations) — payload
  values never leave the host unencrypted
- Field-name lineage (`data.fields`) + client-side PII classification
  (`data.pii`) in span metadata
- Agent metadata (OS, JVM, pid, CPU) on entry-point spans
- Service manifest reported once at startup (framework, runtime, fat-jar
  dependency inventory) for the project's service catalog
- `DataflowFilter` for the JDK built-in HTTP server; one `Dataflow.trace()`
  scope per measurement point for everything else
- `Dataflow.instrument(HttpClient)` — outgoing HTTP calls become HTTP_CLIENT
  spans and propagate `X-Dataflow-Trace-Id`
- `Dataflow.wrap(Connection, system)` — JDBC statements become DB_QUERY spans
  (`<VERB> <table>` names, statement text only — never bind parameter values)

## Quick start

```java
import dev.huginnlabs.dataflow.*;

public static void main(String[] args) throws Exception {
    Dataflow.configure();                      // reads DATAFLOW_* env

    try (Trace t = Dataflow.trace("booking.Create")) {
        t.span().setData("order", "ord_42");   // encrypted on the wire
    }
}
```

JDK built-in HTTP server instrumentation:

```java
HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
server.createContext("/", handler).getFilters().add(new DataflowFilter());
```

## Outgoing HTTP

Wrap a JDK `HttpClient` once; every `send`/`sendAsync` then emits one
`HTTP_CLIENT` span (`"METHOD host/path"`, host as callee, HTTP status,
`http.method`/`http.url` metadata) and injects `X-Dataflow-Trace-Id` — only
when the caller has not set it — so the receiving instrumented service
continues the same trace:

```java
TracedHttpClient client = Dataflow.instrument(HttpClient.newHttpClient());
client.send(HttpRequest.newBuilder(URI.create("https://payments.example/charge"))
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build(), HttpResponse.BodyHandlers.ofString());
```

## JDBC

Wrap an open connection with the database system's name; every executed
statement emits one `DB_QUERY` span named `"<VERB> <table>"` (e.g.
`SELECT orders`, `INSERT users`, `CREATE migrations` — `IF [NOT] EXISTS`
skipped, `public.items` reported as `items`). Metadata carries `db.system`
and `db.statement` (single-spaced, capped at 200 chars). Only statement
text is captured — never bind parameter values. `close`, `unwrap` and
transaction calls pass through; driver SQLExceptions propagate unchanged.
Tracing is decided per query, so wrapping before `configure()` is safe.

```java
try (Connection conn = Dataflow.wrap(
        DriverManager.getConnection(url, user, password), "postgresql");
     PreparedStatement ps = conn.prepareStatement(
        "SELECT id, total FROM orders WHERE id = ?")) {
    ps.setLong(1, orderId);                     // values never leave the host
    try (ResultSet rs = ps.executeQuery()) { ... }
}
```

## Environment

| Variable | Meaning |
| --- | --- |
| `DATAFLOW_ENDPOINT` | gRPC ingest, `host:port` (e.g. `localhost:25090`) |
| `DATAFLOW_API_KEY` | project API key (`df_…`) |
| `DATAFLOW_SERVICE_NAME` | service label in the dashboard |
| `DATAFLOW_ENCRYPTION_KEY` | payload encryption secret (plaintext when unset) |
| `DATAFLOW_SAMPLE_RATIO` | 0..1 sampling (default 1.0) |
| `DATAFLOW_BUFFER_SIZE` | replay buffer cap (default 10000) |
| `DATAFLOW_ENV` / `DATAFLOW_APP_VERSION` | deployment tags |
| `DATAFLOW_HTTP_URL` | HTTP API base for manifest reporting (override when `DATAFLOW_ENDPOINT` is a bare `host:port`) |

## Maven

```xml
<dependency>
  <groupId>dev.huginnlabs.dataflow</groupId>
  <artifactId>dataflow-sdk</artifactId>
  <version>0.3.0</version>
</dependency>
```

Build from this directory (generates stubs from `proto/dataflow.proto`, a
vendored copy of the canonical `dataflow-go/proto/dataflow.proto` with Java
codegen options — update both together):

```
mvn install
```

Live example: `example-java/` (booking flow, gRPC ingest, load generator).
