package dev.huginnlabs.dataflow;

import dev.huginnlabs.dataflow.gen.DataflowProto;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared test plumbing: configures the SDK against a dead endpoint (the
 * sender thread only dials when the replay buffer is non-empty, which the
 * capture sink prevents) and routes completed events into a list instead of
 * onto the wire.
 */
final class SdkTestEnv {

    private static List<DataflowProto.TraceEvent> captured;

    private SdkTestEnv() {}

    /** Idempotent: enables the SDK pointing at a port nothing listens on. */
    static void configure() {
        Dataflow.configure(new Dataflow.Builder()
                .endpoint("127.0.0.1:1")
                .apiKey("test-key")
                .serviceName("sdk-java-test"));
    }

    /** Routes every completed span into {@link #events()}. */
    static void startCapture() {
        captured = new ArrayList<>();
        Pipeline.testSink = e -> {
            List<DataflowProto.TraceEvent> list = captured;
            if (list != null) list.add(e);
        };
    }

    static void stopCapture() {
        Pipeline.testSink = null; // the captured list stays readable for asserts
    }

    static List<DataflowProto.TraceEvent> events() {
        List<DataflowProto.TraceEvent> list = captured;
        return list != null ? list : List.of();
    }
}
