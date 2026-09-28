package dev.huginnlabs.dataflow;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Host/process descriptor stamped onto entry-point spans: OS, arch, JVM
 * runtime, CPU budget, pid, process start, plus the optional DATAFLOW_ENV
 * and DATAFLOW_APP_VERSION deployment tags.
 */
final class Agent {

    private static final long STARTED_MILLIS = System.currentTimeMillis();
    private static volatile Map<String, String> cached;

    private Agent() {}

    static Map<String, String> attrs() {
        Map<String, String> m = cached;
        if (m != null) return m;
        synchronized (Agent.class) {
            if (cached != null) return cached;
            m = new LinkedHashMap<>();
            String os = System.getProperty("os.name", "unknown").toLowerCase();
            String arch = System.getProperty("os.arch", "unknown");
            m.put("agent.os", shortenOs(os) + "/" + arch);
            m.put("agent.runtime", "java " + System.getProperty("java.version", "unknown"));
            m.put("agent.sdk", "java-sdk/" + Dataflow.SDK_VERSION);
            m.put("agent.cpu", String.valueOf(Runtime.getRuntime().availableProcessors()));
            m.put("agent.pid", pid());
            m.put("agent.started", String.valueOf(STARTED_MILLIS));
            String env = System.getenv("DATAFLOW_ENV");
            if (env != null && !env.isEmpty()) m.put("agent.env", env);
            String ver = System.getenv("DATAFLOW_APP_VERSION");
            if (ver != null && !ver.isEmpty()) m.put("agent.app_version", ver);
            cached = m;
            return m;
        }
    }

    /** Stamps the descriptor onto a root span. */
    static void stamp(Span span) {
        for (Map.Entry<String, String> e : attrs().entrySet()) {
            span.attr(e.getKey(), e.getValue());
        }
    }

    private static String shortenOs(String os) {
        if (os.contains("windows")) return "windows";
        if (os.contains("mac")) return "darwin";
        if (os.contains("linux")) return "linux";
        return os.replace(' ', '-');
    }

    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static String pid() {
        try {
            String jvm = ManagementFactory.getRuntimeMXBean().getName(); // "pid@host"
            return jvm.split("@")[0];
        } catch (Throwable t) {
            return hostName();
        }
    }
}
