package dev.huginnlabs.dataflow.scan;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import dev.huginnlabs.dataflow.scan.ScanCli.Route;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regex route extraction over fixture sources written to a temp dir, the
 * catalog JSON shape and the CLI seams (print mode, base URL resolution).
 * No network is involved.
 */
class ScanCliTest {

    @TempDir
    Path tmp;

    private static String find(List<Route> routes, String method, String path) {
        for (Route r : routes) {
            if (r.method.equals(method) && r.path.equals(path)) return r.handler;
        }
        return null;
    }

    // ------------------------------------------------------------------
    // Spring
    // ------------------------------------------------------------------

    @Test
    void extractsSpringMappingsWithClassPrefix() {
        String src = """
                package demo;

                import org.springframework.web.bind.annotation.*;

                @RestController
                @RequestMapping("/api/orders")
                public class OrderController {

                    // @GetMapping("/never") — a comment must not produce a route
                    @GetMapping("/{id}")
                    public Order get(@PathVariable Long id) { return null; }

                    @PostMapping
                    public Order create() { return null; }

                    @PutMapping(value = "/{id}", consumes = "application/json")
                    public Order update() { return null; }

                    @DeleteMapping("/{id}")
                    public void remove() { }

                    @PatchMapping("/{id}")
                    public Order patch() { return null; }

                    @RequestMapping("/health")
                    public String health() { return null; }

                    @RequestMapping(value = "/batch", method = RequestMethod.POST)
                    public String batch() { return null; }

                    @RequestMapping(value = "/multi", method = {RequestMethod.GET, RequestMethod.POST})
                    public String multi() { return null; }
                }
                """;
        List<Route> routes = new ArrayList<>();
        ScanCli.extractRoutes(src, "src/main/java/demo/OrderController.java", routes);

        assertEquals("OrderController.get", find(routes, "GET", "/api/orders/{id}"));
        assertEquals("OrderController.create", find(routes, "POST", "/api/orders"));
        assertEquals("OrderController.update", find(routes, "PUT", "/api/orders/{id}"));
        assertEquals("OrderController.remove", find(routes, "DELETE", "/api/orders/{id}"));
        assertEquals("OrderController.patch", find(routes, "PATCH", "/api/orders/{id}"));
        Route health = null;
        for (Route r : routes) {
            if (r.path.equals("/api/orders/health")) health = r;
        }
        assertNotNull(health);
        assertEquals("ANY", health.method);
        assertEquals("OrderController.health", health.handler);
        assertEquals("OrderController.batch", find(routes, "POST", "/api/orders/batch"));
        assertEquals("OrderController.multi", find(routes, "GET", "/api/orders/multi"));
        assertEquals("OrderController.multi", find(routes, "POST", "/api/orders/multi"));
        assertNull(find(routes, "GET", "/never"));
        for (Route r : routes) {
            assertEquals("src/main/java/demo/OrderController.java", r.sourceFile);
            assertTrue(r.path.startsWith("/"), r.path);
        }
    }

    @Test
    void extractsMultiLineRequestMapping() {
        String src = """
                @RestController
                public class ReportController {

                    @RequestMapping(
                            value = "/reports",
                            method = RequestMethod.GET)
                    public Report get(Long id) { return null; }

                    @PostMapping(
                            value = "/reports",
                            consumes = "application/json")
                    public Report create() { return null; }
                }
                """;
        List<Route> routes = new ArrayList<>();
        ScanCli.extractRoutes(src, "ReportController.java", routes);

        assertEquals(2, routes.size());
        assertEquals("ReportController.get", find(routes, "GET", "/reports"));
        assertEquals("ReportController.create", find(routes, "POST", "/reports"));
    }

    @Test
    void mappingWithoutPathDefaultsToRoot() {
        String src = """
                @RestController
                public class PingController {
                    @PostMapping
                    public void ping() { }
                }
                """;
        List<Route> routes = new ArrayList<>();
        ScanCli.extractRoutes(src, "PingController.java", routes);
        assertEquals(1, routes.size());
        assertEquals("POST", routes.get(0).method);
        assertEquals("/", routes.get(0).path);
        assertEquals("PingController.ping", routes.get(0).handler);
    }

    // ------------------------------------------------------------------
    // JAX-RS
    // ------------------------------------------------------------------

    @Test
    void extractsJaxRsClassAndMethodPaths() {
        String src = """
                @Path("/users")
                public class UserResource {

                    @GET
                    public List<String> list() { return null; }

                    @POST
                    @Path("find")
                    public String find() { return null; }

                    @PUT
                    @Path("/{id}")
                    public String update() { return null; }

                    @DELETE
                    @Path("/{id}")
                    public void delete() { }

                    @Path("/sub")
                    public Object subResource() { return null; }
                }
                """;
        List<Route> routes = new ArrayList<>();
        ScanCli.extractRoutes(src, "UserResource.java", routes);

        assertEquals(4, routes.size());
        assertEquals("UserResource.list", find(routes, "GET", "/users"));
        assertEquals("UserResource.find", find(routes, "POST", "/users/find"));
        assertEquals("UserResource.update", find(routes, "PUT", "/users/{id}"));
        assertEquals("UserResource.delete", find(routes, "DELETE", "/users/{id}"));
        // @Path without a verb is a sub-resource locator: no route.
        assertNull(find(routes, "ANY", "/users/sub"));
    }

    // ------------------------------------------------------------------
    // Directory walk
    // ------------------------------------------------------------------

    private Path write(String rel, String content) throws IOException {
        Path p = tmp.resolve(rel);
        Files.createDirectories(p.getParent());
        Files.writeString(p, content);
        return p;
    }

    @Test
    void scansDirectorySkippingBuildDirsAndTestFiles() throws IOException {
        String controller = """
                @RestController
                public class ThingController {
                    @GetMapping("/ok")
                    public String ok() { return null; }
                }
                """;
        write("app/target/gen/A.java", controller);
        write("app/build/gen/B.java", controller);
        write("app/.git/hooks/C.java", controller);
        write("app/src/ThingTest.java", controller);
        write("app/src/Thing.java", controller);

        ScanCli.ScanResult result = ScanCli.scanDirectory(tmp.resolve("app"));

        assertEquals(1, result.files);
        assertEquals(1, result.routes.size());
        assertEquals("GET", result.routes.get(0).method);
        assertEquals("/ok", result.routes.get(0).path);
        assertEquals("src/Thing.java", result.routes.get(0).sourceFile);
    }

    // ------------------------------------------------------------------
    // Wire format + CLI seams
    // ------------------------------------------------------------------

    @Test
    void buildsCatalogJsonBody() {
        String body = ScanCli.buildBody("my-service", List.of(
                new Route("GET", "/api/orders/{id}", "OrderController.get", "src/OrderController.java")));
        assertEquals("{\"service_name\":\"my-service\",\"routes\":["
                + "{\"method\":\"GET\",\"path\":\"/api/orders/{id}\","
                + "\"handler\":\"OrderController.get\","
                + "\"source_file\":\"src/OrderController.java\"}]}", body);
    }

    @Test
    void bodyIsCappedAtThousandRoutes() {
        List<Route> many = new ArrayList<>();
        for (int i = 0; i < 1500; i++) {
            many.add(new Route("GET", "/p" + i, "H.m", "f" + i + ".java"));
        }
        String body = ScanCli.buildBody("svc", many);
        int count = body.split("\"source_file\":", -1).length - 1;
        assertEquals(ScanCli.MAX_ROUTES, count);
    }

    @Test
    void printWritesJsonToStdoutWithoutPosting() throws IOException {
        write("svc/PingController.java", """
                @RestController
                public class PingController {
                    @GetMapping("/ping")
                    public String ping() { return null; }
                }
                """);
        StringBuilder out = new StringBuilder();
        StringBuilder err = new StringBuilder();
        int code = ScanCli.run(
                new String[] {"--dir", tmp.resolve("svc").toString(),
                        "--service", "svc", "--print"},
                out, err);

        assertEquals(0, code);
        String json = out.toString().trim();
        assertTrue(json.startsWith("{\"service_name\":\"svc\""), json);
        assertTrue(json.contains("\"path\":\"/ping\""), json);
        assertTrue(json.contains("\"handler\":\"PingController.ping\""), json);
        assertTrue(json.contains("\"source_file\":\"PingController.java\""), json);
    }

    @Test
    void resolvesBaseURLWithFlagOverEnvAndSkipsBareEndpoints() {
        assertEquals("https://flag.example",
                ScanCli.resolveBaseURL("https://flag.example/", "http://http.example", "http://end.example"));
        assertEquals("http://http.example",
                ScanCli.resolveBaseURL(null, "http://http.example", "http://end.example"));
        assertEquals("http://end.example",
                ScanCli.resolveBaseURL(null, "", "http://end.example"));
        assertNull(ScanCli.resolveBaseURL(null, "", "localhost:25090"));
        assertNull(ScanCli.resolveBaseURL(null, null, null));
    }

    @Test
    void usageErrorsExitNonZero() {
        StringBuilder out = new StringBuilder();
        StringBuilder err = new StringBuilder();
        assertEquals(2, ScanCli.run(new String[] {}, out, err));
        assertEquals(2, ScanCli.run(new String[] {"--bogus"}, out, err));
        assertEquals(2, ScanCli.run(new String[] {"--dir"}, out, err));
        assertTrue(out.isEmpty());
    }
}
