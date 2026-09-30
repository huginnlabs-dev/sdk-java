package dev.huginnlabs.dataflow;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Service manifest: one best-effort HTTP POST at startup describing this
 * service (runtime, framework, dependency inventory from the running fat
 * jar or classpath). The server turns it into the project's service
 * catalog. Failures are silent — tracing never depends on the manifest
 * reaching the server.
 *
 * <p>Sent once from {@link Dataflow} startup on a daemon thread with a 5s
 * timeout; never blocks startup or the tracing pipeline.
 */
final class Manifest {

    /** Server-side cap on the reported dependency list. */
    private static final int MAX_DEPS = 500;
    /** Wire-contract string caps (service_name/others 128, dep name 256, version 64). */
    private static final int CAP_DEFAULT = 128;
    private static final int CAP_DEP_NAME = 256;
    private static final int CAP_DEP_VERSION = 64;

    /**
     * Well-known framework markers, checked in order; the first dependency
     * name containing a marker wins. Everything else reports as "".
     */
    private static final String[][] FRAMEWORKS = {
            {"spring-boot", "spring-boot"},
            {"spring-web", "spring"},
            {"micronaut", "micronaut"},
            {"quarkus", "quarkus"},
            {"vertx", "vertx"},
            {"jersey", "jersey"},
            {"play-server", "play"},
    };

    private Manifest() {}

    /**
     * Builds the JSON manifest body for {@code POST /api/v1/manifest}.
     * Never throws; every field is derived defensively from the process.
     */
    static String buildManifest(String serviceName, String sdkVersion) {
        return buildManifest(serviceName, sdkVersion,
                System.getProperty("java.class.path", ""), codeSourcePath());
    }

    /** Testable seam: classpath and code-source location injected. */
    static String buildManifest(String serviceName, String sdkVersion,
                                String classPath, String codeSourcePath) {
        List<String[]> deps = new ArrayList<>();
        // Prefer the running fat jar's bundled libraries; a plain (non-fat)
        // jar or exploded class dir falls back to the -cp entries.
        if (!fatJarDeps(codeSourcePath, deps)) {
            classPathDeps(classPath, deps);
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("service_name", clip(serviceName, CAP_DEFAULT));
        body.put("language", "java");
        body.put("sdk_version", clip(sdkVersion, CAP_DEFAULT));
        body.put("runtime_version", clip(System.getProperty("java.version", ""), CAP_DEFAULT));
        body.put("framework", clip(detectFramework(deps), CAP_DEFAULT));
        body.put("os_arch", clip(osArch(), CAP_DEFAULT));
        body.put("app_version", clip(env("DATAFLOW_APP_VERSION"), CAP_DEFAULT));

        List<Object> depsJson = new ArrayList<>(deps.size());
        for (String[] d : deps) {
            Map<String, Object> dep = new LinkedHashMap<>(2);
            dep.put("name", clip(d[0], CAP_DEP_NAME));
            dep.put("version", clip(d[1], CAP_DEP_VERSION));
            depsJson.add(dep);
        }
        body.put("dependencies", depsJson);
        return Json.write(body);
    }

    /**
     * Resolves the HTTP API base for manifest reporting: an explicit
     * {@code DATAFLOW_HTTP_URL} wins (needed when the gRPC
     * {@code DATAFLOW_ENDPOINT} is a bare host:port); URL-form endpoints
     * map directly; otherwise there is no derivable HTTP base and
     * reporting is skipped (returns {@code null}).
     */
    static String httpBaseURL(String endpoint) {
        try {
            String v = env("DATAFLOW_HTTP_URL");
            if (!v.isEmpty()) return trimTrailingSlash(v.trim());
            if (endpoint != null
                    && (endpoint.startsWith("http://") || endpoint.startsWith("https://"))) {
                return trimTrailingSlash(endpoint);
            }
        } catch (Throwable ignore) {
            // never propagate
        }
        return null;
    }

    /**
     * Reports the manifest once per process; call at pipeline startup.
     * Best-effort: daemon thread, short timeout, silent failures — never
     * blocks startup or tracing.
     */
    static void sendManifest() {
        try {
            Dataflow.Settings s = Dataflow.settings();
            if (s.disabled || s.apiKey.isEmpty()) return;
            String base = httpBaseURL(s.endpoint);
            if (base == null) return;
            String url = base + "/api/v1/manifest";
            Thread t = new Thread(() -> post(url, s.apiKey), "dataflow-manifest");
            t.setDaemon(true);
            t.start();
        } catch (Throwable ignore) {
            // Manifest reporting must never break startup.
        }
    }

    /** The daemon-thread body: build + POST; every failure is swallowed. */
    private static void post(String url, String apiKey) {
        try {
            String body = buildManifest(Dataflow.serviceName(), Dataflow.SDK_VERSION);
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(3))
                    .build();
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .header("X-Api-Key", apiKey)
                    .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                    .build();
            // Response body is discarded; non-2xx is fine to ignore.
            client.send(req, HttpResponse.BodyHandlers.discarding());
        } catch (Throwable ignore) {
            // Any failure (bad URL, DNS, timeout, 4xx/5xx) is silently ignored.
        }
    }

    /**
     * Appends dependencies bundled in the running fat jar (entries under
     * {@code BOOT-INF/lib/} ending {@code .jar}). Returns true only when at
     * least one such entry was found (i.e. this really is a fat jar).
     */
    private static boolean fatJarDeps(String jarPath, List<String[]> out) {
        if (jarPath == null || jarPath.isEmpty()) return false;
        try (JarFile jar = new JarFile(jarPath)) {
            Enumeration<JarEntry> entries = jar.entries();
            boolean found = false;
            while (entries.hasMoreElements() && out.size() < MAX_DEPS) {
                JarEntry e = entries.nextElement();
                String name = e.getName();
                if (!name.startsWith("BOOT-INF/lib/") || !name.endsWith(".jar")) continue;
                found = true;
                out.add(splitDep(name.substring(name.lastIndexOf('/') + 1)));
            }
            return found;
        } catch (Throwable t) {
            return false; // not a jar / unreadable — fall back to the classpath
        }
    }

    /** Appends {@code .jar} entries from a java.class.path string, capped. */
    private static void classPathDeps(String classPath, List<String[]> out) {
        if (classPath == null || classPath.isEmpty()) return;
        for (String entry : classPath.split(File.pathSeparator)) {
            if (out.size() >= MAX_DEPS) return;
            if (entry == null || entry.isEmpty()) continue;
            if (entry.startsWith("file:")) {
                try {
                    entry = new File(URI.create(entry)).getPath();
                } catch (Throwable t) {
                    continue;
                }
            }
            if (!entry.toLowerCase(Locale.ROOT).endsWith(".jar")) continue;
            out.add(splitDep(entry));
        }
    }

    /**
     * Splits a jar filename into {name, version}: strips directories and
     * the {@code .jar} suffix, then splits at the LAST {@code -}. A name
     * with no {@code -} reports an empty version.
     */
    static String[] splitDep(String fileName) {
        String s = fileName == null ? "" : fileName;
        if (s.endsWith(".jar")) s = s.substring(0, s.length() - 4);
        int slash = Math.max(s.lastIndexOf('/'), s.lastIndexOf('\\'));
        if (slash >= 0) s = s.substring(slash + 1);
        int cut = s.lastIndexOf('-');
        if (cut <= 0) return new String[] {s, ""};
        return new String[] {s.substring(0, cut), s.substring(cut + 1)};
    }

    /** First dependency name matching a known framework marker, else "". */
    private static String detectFramework(List<String[]> deps) {
        for (String[] d : deps) {
            String name = d[0] == null ? "" : d[0].toLowerCase(Locale.ROOT);
            for (String[] f : FRAMEWORKS) {
                if (name.contains(f[0])) return f[1];
            }
        }
        return "";
    }

    /** "linux/amd64"-style descriptor: normalized OS name + raw os.arch. */
    private static String osArch() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String kind;
        if (os.contains("linux") || os.contains("nix") || os.contains("nux")) kind = "linux";
        else if (os.contains("win")) kind = "windows";
        else if (os.contains("mac")) kind = "mac";
        else kind = os.replace(' ', '-');
        return kind + "/" + System.getProperty("os.arch", "");
    }

    /** The jar this class was loaded from, as a filesystem path (or null). */
    private static String codeSourcePath() {
        try {
            java.security.CodeSource cs = Manifest.class.getProtectionDomain().getCodeSource();
            if (cs == null || cs.getLocation() == null) return null;
            try {
                return new File(cs.getLocation().toURI()).getAbsolutePath();
            } catch (Exception e) {
                return cs.getLocation().getPath();
            }
        } catch (Throwable t) {
            return null;
        }
    }

    private static String env(String key) {
        try {
            String v = System.getenv(key);
            return v == null ? "" : v;
        } catch (Throwable t) {
            return "";
        }
    }

    private static String trimTrailingSlash(String s) {
        while (s.endsWith("/")) s = s.substring(0, s.length() - 1);
        return s;
    }

    /** Truncates to the server-side wire cap (null-safe). */
    private static String clip(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max);
    }
}
