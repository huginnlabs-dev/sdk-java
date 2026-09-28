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
- `DataflowFilter` for the JDK built-in HTTP server; one `Dataflow.trace()`
  scope per measurement point for everything else

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

## Maven

```xml
<dependency>
  <groupId>dev.huginnlabs.dataflow</groupId>
  <artifactId>dataflow-sdk</artifactId>
  <version>0.1.0</version>
</dependency>
```

Build from the repo root (generates stubs from `proto/dataflow.proto`):

```
mvn -f sdk-java/pom.xml install
```

Live example: `example-java/` (booking flow, gRPC ingest, load generator).
