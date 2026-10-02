# Dataflow Java SDK

Runtime tracing for JVM services. Streams completed spans over **gRPC**
(`StreamEvents` + ack-watermark replay buffer) — the same wire contract as
the Go and Python SDKs.

- Java 17+, no hard dependencies besides `grpc-java` / `protobuf-java`
  (the Logback appender's `logback-classic` is optional/provided — supplied
  by your service)
- AES-256-GCM payload encryption (PBKDF2-SHA256, 10k iterations) — payload
  values never leave the host unencrypted
- Field-name lineage (`data.fields`) + client-side PII classification
  (`data.pii`) in span metadata
- Agent metadata (OS, JVM, pid, CPU) on entry-point spans
- Service manifest reported once at startup (framework, runtime, fat-jar
  dependency inventory) for the project's service catalog
- Static route scanner CLI (`dev.huginnlabs.dataflow.scan.ScanCli`) feeding
  the server's route catalog from Spring / JAX-RS annotations
- `DataflowFilter` for the JDK built-in HTTP server; one `Dataflow.trace()`
  scope per measurement point for everything else
- `Dataflow.instrument(HttpClient)` — outgoing HTTP calls become HTTP_CLIENT
  spans and propagate `X-Dataflow-Trace-Id`
- `Dataflow.wrap(Connection, system)` — JDBC statements become DB_QUERY spans
  (`<VERB> <table>` names, statement text only — never bind parameter values)
- `Dataflow.capture` / `Dataflow.captureUncaught` — crash capture: crashes are
  recorded (status 500, `error_message`, `error.stack`) and then rethrown or
  chained, never swallowed
- `Dataflow.info/warn/error/debug` + `Dataflow.LogHandler` (JUL bridge) /
  `Dataflow.installLogback()` (SLF4J/Logback appender) — application log
  shipping batched to `POST /api/v1/logs` with trace/span correlation

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

## Crash capture

`Dataflow.capture` / `Dataflow.captureCallable` run a body and record any
crash on the current span — status `500`, `error_message` = the exception's
`toString()` clipped to 500 chars, `error.stack` metadata carrying the stack
trace capped at 8192 chars from the top — and then rethrow it: recording is
best-effort, the exception itself is never swallowed. A checked throwable the
`Runnable` contract cannot declare is re-wrapped in `RuntimeException`; a
`Callable`'s checked exceptions propagate as declared. With no span active a
synthetic `exception` span carries the record.

```java
Dataflow.capture(() -> processOrder(orderId));          // crash -> span + rethrow
String rendered = Dataflow.captureCallable(() -> render(orderId));
```

`Dataflow.captureUncaught()` installs a JVM-wide default uncaught-exception
handler: every thread death is recorded on a synthetic `uncaught exception`
span (same wire shape) and then chained to the previously installed handler —
or the standard stderr report when none was set — so existing hooks keep
working. A hook cannot rethrow, and neither does this one. The install is
idempotent; `Dataflow.ignoreUncaught()` restores the previous handler. While
the SDK is disabled everything passes through: bodies run, the handler is
never installed.

## Log capture

`Dataflow.info` / `warn` / `error` / `debug` ship application logs to
`POST /api/v1/logs`, batched by a background daemon thread (~500 ms per
flush, or immediately at 50 buffered lines, ≤1000 lines per POST). Each
line carries the current span's `trace_id` / `span_id` (empty outside a
trace), `service_name`, a millisecond `timestamp` and the `fields` map
stringified via `String.valueOf` (max 50 entries) — so dashboard logs join
the trace timeline. Sending is best-effort: recording never blocks or
throws, the 1024-line buffer drops its oldest line under pressure (counted),
a failed POST retries once and the batch is then dropped, and
`Dataflow.flushLogs()` waits at most ~5 seconds (useful before JVM exit).

```java
Dataflow.info("booking created", Map.of("order", "ord_42"));
Dataflow.warn("cache cold", null);
Dataflow.flushLogs();                       // best-effort drain before exit
```

For `java.util.logging` users, `Dataflow.LogHandler` forwards records with
level mapping (`SEVERE`→`error`, `WARNING`→`warn`, `INFO`→`info`,
`FINE`/`FINER`/`FINEST`→`debug`) and `{0}`-style parameter formatting —
logger names, throwables and other extras are skipped:

```java
java.util.logging.Logger.getLogger("").addHandler(new Dataflow.LogHandler());
```

For SLF4J/Logback — the default logging stack of Spring Boot and most Java
services — `Dataflow.installLogback()` attaches a `LogbackAppender` to the
root logger of the default `LoggerContext` (`LoggerFactory.getILoggerFactory()`).
Every Logback line is then forwarded into the same pipeline with level
mapping (`TRACE`/`DEBUG`→`debug`, `INFO`→`info`, `WARN`→`warn`,
`ERROR`→`error`), `{}`-parameter formatting and, when an event carries only
a throwable, the throwable's `ClassName: message` standing in as the line.
Key-value pairs and markers are skipped in v1. The install is idempotent (an
earlier install is detached first, never doubled);
`Dataflow.uninstallLogback()` detaches and stops it again. While the SDK is
disabled the appender records nothing. An explicit context works too:
`Dataflow.installLogback(loggerContext)`.

```java
Dataflow.installLogback();          // right after Dataflow.configure()
```

Spring Boot — a one-bean install at startup:

```java
@Bean
ApplicationRunner dataflowLogs() {
    return args -> Dataflow.installLogback();
}
```

`logback-classic` is an **optional/provided** dependency of the SDK: services
already running Logback get the appender for free, and the SDK never pulls
Logback into a build that doesn't have it (`LogbackAppender` only loads when
installed).

The HTTP base resolves like the startup manifest's
(`DATAFLOW_HTTP_URL` > URL-form `DATAFLOW_ENDPOINT`; a bare `host:port`
gRPC endpoint ships no logs). While the SDK is disabled every call is a
no-op and the handler records nothing.

## Route scanning

`dev.huginnlabs.dataflow.scan.ScanCli` is a static route scanner: it walks a
source tree, extracts HTTP endpoints from Java files (Spring `@GetMapping` /
`@PostMapping` / `@PutMapping` / `@DeleteMapping` / `@PatchMapping` /
`@RequestMapping` — including multi-line annotations and class-level path
prefixes — and JAX-RS `@Path` + `@GET`/`@POST`/… pairs) and posts them to the
server catalog (`POST /api/v1/catalog`) so declared routes can be correlated
with observed traffic. Extraction is regex-based over source lines — the SDK
stays dependency-free. Build directories (`target/`, `build/`, `.git/`) and
`*Test.java` files are skipped; `{param}` templates are kept as written.

```bash
mvn -q compile exec:java -Dexec.mainClass=dev.huginnlabs.dataflow.scan.ScanCli \
    -Dexec.args="--dir /path/to/service --service my-service \
                 --api-key df_... --url https://dataflow.example"
```

Flags: `--dir` (source root, required), `--service` (or
`DATAFLOW_SERVICE_NAME`), `--url`, `--api-key` (or `DATAFLOW_API_KEY`), and
`--print` to write the catalog JSON to stdout without posting. Base URL
resolution matches the startup manifest: `--url` > `DATAFLOW_HTTP_URL` >
URL-form `DATAFLOW_ENDPOINT` (a bare `host:port` endpoint is gRPC-only and is
skipped with a clear message).

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
  <version>0.7.0</version>
</dependency>
```

Build from this directory (generates stubs from `proto/dataflow.proto`, a
vendored copy of the canonical `dataflow-go/proto/dataflow.proto` with Java
codegen options — update both together):

```
mvn install
```

Live example: `example-java/` (booking flow, gRPC ingest, load generator).
