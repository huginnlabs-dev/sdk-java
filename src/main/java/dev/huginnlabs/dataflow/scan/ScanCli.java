package dev.huginnlabs.dataflow.scan;

import dev.huginnlabs.dataflow.Json;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Static route scanner CLI: extracts HTTP endpoints from Java source files
 * (Spring request mappings, JAX-RS {@code @Path} + verb annotations) and
 * posts them to the server catalog so declared routes can be correlated
 * with observed traffic. Regex extraction over source lines only — no
 * parser dependency, the SDK stays dependency-free.
 *
 * <p>Usage:
 * <pre>{@code
 * mvn -q compile exec:java -Dexec.mainClass=dev.huginnlabs.dataflow.scan.ScanCli \
 *     -Dexec.args="--dir /path/to/service --service my-service \
 *                  --url https://dataflow.example --api-key df_..."
 * }</pre>
 *
 * <p>{@code --print} writes the catalog JSON to stdout instead of posting.
 * Base URL resolution: {@code --url} > {@code DATAFLOW_HTTP_URL} > URL-form
 * {@code DATAFLOW_ENDPOINT}; a bare {@code host:port} endpoint (gRPC only)
 * is skipped with a clear message. API key: {@code --api-key} or
 * {@code DATAFLOW_API_KEY}.
 */
public final class ScanCli {

    /** Server-side cap: at most 1000 routes are accepted per catalog post. */
    static final int MAX_ROUTES = 1000;
    /** How many following lines a multi-line annotation may span. */
    private static final int JOIN_WINDOW = 5;

    /** Wire-contract string caps (path/handler/source_file 256, service 128). */
    private static final int CAP_DEFAULT = 128;
    private static final int CAP_ROUTE_FIELD = 256;

    private static final Pattern ANNOTATION_NAME = Pattern.compile("^@([\\w.]+)");
    private static final Pattern STRING_LITERAL =
            Pattern.compile("\"((?:[^\"\\\\]|\\\\.)*)\"");
    private static final Pattern REQUEST_METHOD =
            Pattern.compile("RequestMethod\\.(\\w+)");
    private static final Pattern CLASS_DECL = Pattern.compile(
            "^\\s*(?:(?:public|protected|private|abstract|final|sealed|non-sealed|static|strictfp)\\s+)*"
                    + "(?:class|interface|enum|record)\\s+([\\w$]+)");
    /** Method or constructor declaration: a name followed by '(' near the line start. */
    private static final Pattern METHOD_DECL = Pattern.compile("([\\w$]+)\\s*\\(");
    private static final Pattern SKIP_DIRS = Pattern.compile("target|build|\\.git");

    /** One extracted endpoint. */
    public static final class Route {
        public final String method;
        public final String path;
        public final String handler;
        public final String sourceFile;

        Route(String method, String path, String handler, String sourceFile) {
            this.method = method;
            this.path = path;
            this.handler = handler;
            this.sourceFile = sourceFile;
        }
    }

    /** Scan outcome: routes plus how many files were inspected. */
    public static final class ScanResult {
        public final List<Route> routes;
        public final int files;

        ScanResult(List<Route> routes, int files) {
            this.routes = routes;
            this.files = files;
        }
    }

    /** Annotation kinds we care about; everything else is ignored. */
    private enum Kind { MAPPING, PATH, OTHER }

    /** One annotation awaiting the declaration it belongs to. */
    private static final class Anno {
        final Kind kind;
        final String name;
        final List<String> methods; // upper-case HTTP verbs; empty → caller decides
        final String path;

        Anno(Kind kind, String name, List<String> methods, String path) {
            this.kind = kind;
            this.name = name;
            this.methods = methods;
            this.path = path;
        }
    }

    private ScanCli() {}

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    /** CLI body, injectable streams for tests. Returns the process exit code. */
    static int run(String[] args, Appendable out, Appendable err) {
        String dir = null;
        String service = null;
        String url = null;
        String apiKey = null;
        boolean print = false;
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if ("--dir".equals(a) || "--service".equals(a) || "--url".equals(a)
                    || "--api-key".equals(a)) {
                if (i + 1 >= args.length) {
                    msg(err, "missing value for " + a);
                    return usage(err);
                }
                String v = args[++i];
                if ("--dir".equals(a)) dir = v;
                else if ("--service".equals(a)) service = v;
                else if ("--url".equals(a)) url = v;
                else apiKey = v;
            } else if ("--print".equals(a)) {
                print = true;
            } else {
                msg(err, "unknown argument: " + a);
                return usage(err);
            }
        }
        if (dir == null || dir.isEmpty()) {
            msg(err, "--dir is required");
            return usage(err);
        }
        if (service == null || service.isEmpty()) {
            service = env("DATAFLOW_SERVICE_NAME");
        }
        if (service == null || service.isEmpty()) {
            msg(err, "--service is required (or set DATAFLOW_SERVICE_NAME)");
            return usage(err);
        }

        Path root = Paths.get(dir);
        if (!Files.isDirectory(root)) {
            msg(err, "not a directory: " + dir);
            return 2;
        }

        ScanResult result;
        try {
            result = scanDirectory(root);
        } catch (IOException e) {
            msg(err, "scan failed: " + e);
            return 1;
        }
        String body = buildBody(service, result.routes);

        if (print) {
            println(out, body);
            return 0;
        }

        String base = resolveBaseURL(url, env("DATAFLOW_HTTP_URL"), env("DATAFLOW_ENDPOINT"));
        if (base == null) {
            msg(err, "no HTTP base URL: pass --url, or set DATAFLOW_HTTP_URL;"
                    + " a bare host:port DATAFLOW_ENDPOINT is gRPC-only and cannot be used");
            return 1;
        }
        if (apiKey == null || apiKey.isEmpty()) {
            apiKey = env("DATAFLOW_API_KEY");
        }
        if (apiKey == null || apiKey.isEmpty()) {
            msg(err, "no API key: pass --api-key or set DATAFLOW_API_KEY");
            return 2;
        }

        msg(err, "dataflow-scan: " + result.routes.size() + " route(s) from "
                + result.files + " file(s) -> " + base + "/api/v1/catalog");
        int status = postCatalog(base + "/api/v1/catalog", apiKey, body, err);
        if (status < 200 || status >= 300) {
            msg(err, "catalog post failed with HTTP " + status);
            return 1;
        }
        msg(err, "dataflow-scan: accepted (HTTP " + status + ")");
        return 0;
    }

    private static int usage(Appendable err) {
        msg(err, "usage: ScanCli --dir <source root> --service <name>"
                + " [--url <http base>] [--api-key <key>] [--print]");
        return 2;
    }

    // ------------------------------------------------------------------
    // Extraction
    // ------------------------------------------------------------------

    /** Walks {@code root} for *.java sources, skipping build dirs and tests. */
    static ScanResult scanDirectory(Path root) throws IOException {
        List<Route> routes = new ArrayList<>();
        int[] files = {0};
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                return SKIP_DIRS.matcher(dir.getFileName().toString()).matches()
                        ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String name = file.getFileName().toString();
                if (name.endsWith(".java") && !name.endsWith("Test.java")) {
                    files[0]++;
                    try {
                        String source = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
                        String rel = root.relativize(file).toString().replace('\\', '/');
                        extractRoutes(source, rel, routes);
                    } catch (IOException ignored) {
                        // unreadable file: skip, the rest still scans
                    }
                }
                return routes.size() >= MAX_ROUTES
                        ? FileVisitResult.TERMINATE : FileVisitResult.CONTINUE;
            }
        });
        return new ScanResult(routes, files[0]);
    }

    /**
     * Extracts routes from one source file into {@code out}. The state
     * machine tracks the enclosing class, annotations pending a declaration
     * and class-level path prefixes ({@code @RequestMapping} / JAX-RS
     * {@code @Path} on the class).
     */
    static void extractRoutes(String source, String sourceFile, List<Route> out) {
        String[] lines = source.replace("\r\n", "\n").replace('\r', '\n').split("\n", -1);
        String className = null;
        String classPrefix = "";
        List<Anno> pending = new ArrayList<>();

        for (int i = 0; i < lines.length; i++) {
            String text = stripLineComment(lines[i]).trim();
            if (text.isEmpty()) continue;

            // Split leading annotations off the line; a multi-line annotation
            // (unclosed paren) pulls in the next few lines.
            String rest = text;
            int guard = 0;
            while (rest.startsWith("@") && guard++ < 32) {
                String annoText = null;
                int end = annotationEnd(rest, 0);
                if (end < 0) {
                    String joined = rest;
                    int taken = 0;
                    while (end < 0 && taken < JOIN_WINDOW && i + taken + 1 < lines.length) {
                        joined = joined + " " + stripLineComment(lines[i + taken + 1]).trim();
                        taken++;
                        end = annotationEnd(joined, 0);
                    }
                    if (end < 0) end = joined.length();
                    i += taken;
                    annoText = joined.substring(0, end);
                    rest = joined.substring(end).trim();
                } else {
                    annoText = rest.substring(0, end);
                    rest = rest.substring(end).trim();
                }
                pending.add(parseAnnotation(annoText));
            }

            if (rest.isEmpty()) continue;
            Matcher cls = CLASS_DECL.matcher(rest); // ^-anchored: matches at 0 only
            if (cls.find()) {
                className = cls.group(1);
                String prefix = classPrefixFrom(pending);
                if (prefix != null) classPrefix = prefix;
                pending.clear();
                continue;
            }
            if (!pending.isEmpty()) {
                Matcher method = METHOD_DECL.matcher(rest);
                if (method.find() && !isKeyword(method.group(1))
                        && method.start() > 0 && rest.charAt(method.start() - 1) != '.') {
                    emit(out, pending, className, classPrefix, method.group(1), sourceFile);
                    pending.clear();
                    continue;
                }
            }
            // Any other statement breaks the annotation -> declaration chain.
            pending.clear();
        }
    }

    /** Splits one parsed annotation into kind / methods / path. */
    static Anno parseAnnotation(String annoText) {
        Matcher name = ANNOTATION_NAME.matcher(annoText);
        String simple = "";
        if (name.find()) {
            String fqn = name.group(1);
            simple = fqn.substring(fqn.lastIndexOf('.') + 1);
        }
        String path = firstStringLiteral(annoText);
        switch (simple) {
            case "GetMapping": return new Anno(Kind.MAPPING, simple, List.of("GET"), path);
            case "PostMapping": return new Anno(Kind.MAPPING, simple, List.of("POST"), path);
            case "PutMapping": return new Anno(Kind.MAPPING, simple, List.of("PUT"), path);
            case "DeleteMapping": return new Anno(Kind.MAPPING, simple, List.of("DELETE"), path);
            case "PatchMapping": return new Anno(Kind.MAPPING, simple, List.of("PATCH"), path);
            case "RequestMapping":
                List<String> methods = new ArrayList<>();
                Matcher rm = REQUEST_METHOD.matcher(annoText);
                while (rm.find()) methods.add(rm.group(1).toUpperCase());
                return new Anno(Kind.MAPPING, simple, methods, path);
            case "Path": return new Anno(Kind.PATH, simple, List.of(), path);
            case "GET": case "POST": case "PUT": case "DELETE":
            case "PATCH": case "HEAD": case "OPTIONS":
                return new Anno(Kind.MAPPING, simple, List.of(simple), "");
            default: return new Anno(Kind.OTHER, simple, List.of(), path);
        }
    }

    /** Class-level path prefix from the pending annotations, or null to keep. */
    private static String classPrefixFrom(List<Anno> pending) {
        for (Anno a : pending) {
            if (a.kind == Kind.MAPPING && "RequestMapping".equals(a.name) && !a.path.isEmpty()) {
                return a.path;
            }
        }
        for (Anno a : pending) {
            if (a.kind == Kind.PATH && !a.path.isEmpty()) return a.path;
        }
        return null;
    }

    /** Turns the annotations collected before a method declaration into routes. */
    private static void emit(List<Route> out, List<Anno> pending, String className,
                             String classPrefix, String method, String sourceFile) {
        String fromMapping = "";
        String fromPath = "";
        List<String> methods = new ArrayList<>();
        for (Anno a : pending) {
            if (a.kind == Kind.MAPPING) {
                if (fromMapping.isEmpty()) fromMapping = a.path;
                methods.addAll(a.methods);
            } else if (a.kind == Kind.PATH && fromPath.isEmpty()) {
                fromPath = a.path;
            }
        }
        if (methods.isEmpty()) {
            if (!fromMapping.isEmpty()) {
                methods.add("ANY"); // @RequestMapping without method
            } else {
                return; // JAX-RS @Path without a verb: sub-resource locator, skip
            }
        }
        String path = joinPath(classPrefix, fromMapping.isEmpty() ? fromPath : fromMapping);
        String handler = className == null ? method : className + "." + method;
        for (String m : methods) {
            if (out.size() < MAX_ROUTES) {
                out.add(new Route(m, path, handler, sourceFile));
            }
        }
    }

    /** Joins class + method path segments, always starting with '/'. */
    static String joinPath(String prefix, String suffix) {
        String p = prefix == null ? "" : prefix.trim();
        String s = suffix == null ? "" : suffix.trim();
        if (!p.isEmpty() && !p.startsWith("/")) p = "/" + p;
        if (!s.isEmpty() && !s.startsWith("/")) s = "/" + s;
        while (p.endsWith("/")) p = p.substring(0, p.length() - 1);
        if (s.equals("/")) s = "";
        String joined = p.isEmpty() ? s : (s.isEmpty() ? p : p + s);
        return joined.isEmpty() ? "/" : joined;
    }

    // ------------------------------------------------------------------
    // Line-level helpers
    // ------------------------------------------------------------------

    /**
     * End index (exclusive) of the annotation starting at {@code at}, or -1
     * when its parentheses are still open (multi-line annotation). Bare
     * annotations like {@code @GET} end at the next whitespace.
     */
    static int annotationEnd(String s, int at) {
        int i = at + 1;
        while (i < s.length() && (Character.isJavaIdentifierPart(s.charAt(i)) || s.charAt(i) == '.')) {
            i++;
        }
        int depth = 0;
        boolean inParens = false;
        boolean inStr = false;
        for (; i < s.length(); i++) {
            char c = s.charAt(i);
            if (inStr) {
                if (c == '\\' && i + 1 < s.length()) i++;
                else if (c == '"') inStr = false;
                continue;
            }
            if (c == '"') {
                inStr = true;
            } else if (c == '(') {
                depth++;
                inParens = true;
            } else if (c == ')') {
                depth--;
                if (depth <= 0) return i + 1;
            } else if (!inParens && Character.isWhitespace(c)) {
                return i;
            }
        }
        return inParens ? -1 : s.length();
    }

    /** Cuts a {@code //} line comment (string-literal aware). */
    static String stripLineComment(String line) {
        boolean inStr = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inStr) {
                if (c == '\\' && i + 1 < line.length()) i++;
                else if (c == '"') inStr = false;
                continue;
            }
            if (c == '"') {
                inStr = true;
            } else if (c == '/' && i + 1 < line.length() && line.charAt(i + 1) == '/') {
                return line.substring(0, i);
            }
        }
        return line;
    }

    /** First string literal's content (minimal unescaping), or "". */
    static String firstStringLiteral(String s) {
        Matcher m = STRING_LITERAL.matcher(s);
        if (!m.find()) return "";
        String v = m.group(1);
        return v.replace("\\\"", "\"").replace("\\\\", "\\");
    }

    private static boolean isKeyword(String word) {
        switch (word) {
            case "if": case "for": case "while": case "switch": case "catch":
            case "return": case "new": case "do": case "try": case "synchronized":
            case "throw": case "assert": case "lock": case "equals":
                return true;
            default:
                return false;
        }
    }

    // ------------------------------------------------------------------
    // Wire format + HTTP
    // ------------------------------------------------------------------

    /** Builds the {@code POST /api/v1/catalog} JSON body. */
    static String buildBody(String service, List<Route> routes) {
        List<Object> arr = new ArrayList<>(Math.min(routes.size(), MAX_ROUTES));
        for (Route r : routes) {
            if (arr.size() >= MAX_ROUTES) break;
            Map<String, Object> route = new LinkedHashMap<>(4);
            route.put("method", clip(r.method, 16));
            route.put("path", clip(r.path, CAP_ROUTE_FIELD));
            route.put("handler", clip(r.handler, CAP_ROUTE_FIELD));
            route.put("source_file", clip(r.sourceFile, CAP_ROUTE_FIELD));
            arr.add(route);
        }
        Map<String, Object> body = new LinkedHashMap<>(2);
        body.put("service_name", clip(service, CAP_DEFAULT));
        body.put("routes", arr);
        return Json.write(body);
    }

    /**
     * Resolves the HTTP API base: explicit {@code --url} wins, then
     * {@code DATAFLOW_HTTP_URL}, then a URL-form {@code DATAFLOW_ENDPOINT}.
     * A bare host:port endpoint (gRPC) yields null — the caller reports the
     * skip. Same precedence as the startup manifest.
     */
    static String resolveBaseURL(String urlFlag, String httpUrlEnv, String endpointEnv) {
        String v = urlFlag == null ? "" : urlFlag.trim();
        if (!v.isEmpty()) return trimTrailingSlash(v);
        v = httpUrlEnv == null ? "" : httpUrlEnv.trim();
        if (!v.isEmpty()) return trimTrailingSlash(v);
        v = endpointEnv == null ? "" : endpointEnv.trim();
        if (v.startsWith("http://") || v.startsWith("https://")) return trimTrailingSlash(v);
        return null;
    }

    /** Synchronous POST; returns the HTTP status, body excerpt on error. */
    static int postCatalog(String url, String apiKey, String body, Appendable err) {
        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .build();
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(15))
                    .header("Content-Type", "application/json")
                    .header("X-Api-Key", apiKey)
                    .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> resp =
                    client.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                String excerpt = resp.body() == null ? "" : resp.body().trim();
                if (excerpt.length() > 200) excerpt = excerpt.substring(0, 200);
                msg(err, "server said: " + excerpt);
            }
            return resp.statusCode();
        } catch (Exception e) {
            msg(err, "post failed: " + e);
            return 0;
        }
    }

    private static String trimTrailingSlash(String s) {
        while (s.endsWith("/")) s = s.substring(0, s.length() - 1);
        return s;
    }

    private static String clip(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max);
    }

    private static String env(String key) {
        try {
            String v = System.getenv(key);
            return v == null ? "" : v;
        } catch (Throwable t) {
            return "";
        }
    }

    private static void msg(Appendable err, String text) {
        try {
            err.append("dataflow-scan: " + text + "\n");
        } catch (IOException ignored) {
            // nothing sensible to do
        }
    }

    private static void println(Appendable out, String text) {
        try {
            out.append(text + "\n");
        } catch (IOException ignored) {
            // nothing sensible to do
        }
    }
}
