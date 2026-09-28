package dev.huginnlabs.dataflow;

import java.util.ArrayDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import dev.huginnlabs.dataflow.gen.DataflowProto;
import dev.huginnlabs.dataflow.gen.DataflowServiceGrpc;
import dev.huginnlabs.dataflow.gen.DataflowProto.TraceEvent;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;

/**
 * The event path from Span.end() to the ingestion API: completed events
 * land in a bounded replay buffer and a background sender streams them
 * over the bidirectional {@code StreamEvents} RPC, trimming the buffer as
 * the server acknowledges durability (ack watermark). Failed batches stay
 * in the buffer and are retried with backoff.
 */
final class Pipeline {

    private static final long FLUSH_INTERVAL_MS = 300;
    private static final int MAX_BATCH = 500;
    private static final long ACK_TIMEOUT_MS = 30_000;

    private static final AtomicLong SEQ = new AtomicLong();
    private static final ReentrantLock LOCK = new ReentrantLock();
    private static final Condition WAKE = LOCK.newCondition();
    private static final ArrayDeque<TraceEvent> BUFFER = new ArrayDeque<>();
    private static long bufferBase = 1; // seq of BUFFER.peekFirst()
    private static volatile boolean running;

    private Pipeline() {}

    static long nextSeq() { return SEQ.incrementAndGet(); }

    static void start(Dataflow.Settings s) {
        if (!Crypto.init(s.encryptionKey) && s.encryptionKey.isEmpty()) {
            s.logger.accept("dataflow: warning: no encryption key set; captured payloads are sent as plaintext");
        }
        running = true;
        Thread t = new Thread(Pipeline::run, "dataflow-sender");
        t.setDaemon(true);
        t.start();
    }

    /** Single entry point from Span.end() into the delivery path. */
    static void enqueue(TraceEvent event) {
        if (!running) return;
        LOCK.lock();
        try {
            BUFFER.addLast(event);
            while (BUFFER.size() > Dataflow.settings().bufferSize) {
                BUFFER.pollFirst();
                bufferBase++;
            }
            WAKE.signalAll();
        } finally {
            LOCK.unlock();
        }
    }

    private static void run() {
        Dataflow.Settings cfg = Dataflow.settings();
        // Endpoint may be a bare host:port or an http(s):// URL; gRPC needs
        // the authority form (plaintext unless https).
        String target = cfg.endpoint.replaceFirst("^https?://", "");
        ManagedChannel channel = io.grpc.ManagedChannelBuilder
                .forTarget(target)
                .usePlaintext()
                .maxInboundMessageSize(16 * 1024 * 1024)
                .build();

        long backoff = 500;
        while (running) {
            boolean hasWork;
            LOCK.lock();
            try {
                long deadline = System.nanoTime() + FLUSH_INTERVAL_MS * 1_000_000L;
                while (BUFFER.isEmpty() && running) {
                    long remain = deadline - System.nanoTime();
                    if (remain <= 0) break;
                    WAKE.awaitNanos(remain);
                }
                hasWork = !BUFFER.isEmpty();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdown();
                return;
            } finally {
                LOCK.unlock();
            }
            if (!hasWork) continue;

            try {
                flush(channel);
                backoff = 500;
            } catch (Exception e) {
                Dataflow.settings().logger.accept("dataflow: send failed, retrying: " + e.getMessage());
                try {
                    Thread.sleep(Math.min(backoff, 10_000));
                } catch (InterruptedException ie) {
                    channel.shutdown();
                    return;
                }
                backoff *= 2;
            }
        }
        channel.shutdown();
    }

    /**
     * Streams every unacked event over a fresh bidirectional call and
     * trims the replay buffer up to the highest acked seq. The lock must
     * NOT be held here: flush blocks on network I/O.
     */
    private static void flush(ManagedChannel channel) {
        long[] acked = {0};
        CountDownLatch done = new CountDownLatch(1);

        DataflowServiceGrpc.DataflowServiceStub stub = withApiKey(channel);
        StreamObserver<TraceEvent> requests = stub.streamEvents(new StreamObserver<DataflowProto.AckResponse>() {
            @Override public void onNext(DataflowProto.AckResponse value) {
                if (value.getLastSeq() > acked[0]) acked[0] = value.getLastSeq();
            }
            @Override public void onError(Throwable t) {
                done.countDown();
            }
            @Override public void onCompleted() {
                done.countDown();
            }
        });

        TraceEvent[] snapshot;
        LOCK.lock();
        try {
            snapshot = BUFFER.toArray(new TraceEvent[0]);
        } finally {
            LOCK.unlock();
        }
        for (int i = 0; i < snapshot.length && i < MAX_BATCH; i++) {
            requests.onNext(snapshot[i]);
        }
        requests.onCompleted();

        try {
            // The server acks as it persists; wait for the stream to close.
            done.await(ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (acked[0] > 0) trim(acked[0]);
    }

    /** Drops buffer entries the server has durably accepted. */
    private static void trim(long ackedSeq) {
        LOCK.lock();
        try {
            long drop = Math.max(ackedSeq + 1 - bufferBase, 0);
            drop = Math.min(drop, BUFFER.size());
            for (long i = 0; i < drop; i++) BUFFER.pollFirst();
            bufferBase += drop;
        } finally {
            LOCK.unlock();
        }
    }

    /** Attaches the project API key as x-api-key metadata on every call. */
    private static DataflowServiceGrpc.DataflowServiceStub withApiKey(ManagedChannel channel) {
        String key = Dataflow.settings().apiKey;
        ClientInterceptor apiKeyInterceptor = new ClientInterceptor() {
            @Override public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, io.grpc.Channel next) {
                ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
                return new ClientCall<ReqT, RespT>() {
                    @Override public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER), key);
                        call.start(responseListener, headers);
                    }
                    @Override public void sendMessage(ReqT message) { call.sendMessage(message); }
                    @Override public void halfClose() { call.halfClose(); }
                    @Override public void cancel(String message, Throwable cause) { call.cancel(message, cause); }
                    @Override public boolean isReady() { return call.isReady(); }
                    @Override public void request(int numMessages) { call.request(numMessages); }
                };
            }
        };
        return DataflowServiceGrpc.newStub(ClientInterceptors.intercept(channel, apiKeyInterceptor));
    }
}
