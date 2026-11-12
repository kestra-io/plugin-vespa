package io.kestra.plugin.vespa;

import com.sun.net.httpserver.HttpServer;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.serializers.JacksonMapper;
import jakarta.inject.Inject;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VespaConnectionTest extends VespaContainer {
    @Inject
    private ModelValidator modelValidator;

    @Test
    void request() throws Exception {
        var response = probe(endpoint).request(runContextFactory.of(), get("/search/?yql=" + encode("select * from music where true") + "&hits=0"));

        assertThat(response.getStatus().getCode(), is(200));
        assertThat(JacksonMapper.ofJson().readTree(response.getBody()).at("/root/fields/totalCount").asLong(), greaterThan(0L));
    }

    @Test
    void endpointWithTrailingSlash() throws Exception {
        try (var stub = Stub.start()) {
            probe(stub.endpoint() + "//").request(runContextFactory.of(), get("/search/"));

            assertThat(stub.seen().getFirst().path(), is("/search/"));
        }
    }

    @Test
    void bearerToken() throws Exception {
        try (var stub = Stub.start()) {
            var task = Probe.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .token(Property.ofExpression("{{ 'tok' ~ 'en' }}"))
                .build();

            task.request(runContextFactory.of(), get("/search/"));

            assertThat(stub.seen().getFirst().authorization(), contains("Bearer token"));
        }
    }

    @Test
    void noTokenNoAuthorization() throws Exception {
        try (var stub = Stub.start()) {
            probe(stub.endpoint()).request(runContextFactory.of(), get("/search/"));

            assertThat(stub.seen().getFirst().authorization(), is(nullValue()));
        }
    }

    @Test
    void blankTokenNoAuthorization() throws Exception {
        try (var stub = Stub.start()) {
            var task = Probe.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .token(Property.ofExpression("{{ '' }}"))
                .build();

            task.request(runContextFactory.of(), get("/search/"));

            assertThat(stub.seen().getFirst().authorization(), is(nullValue()));
        }
    }

    @Test
    void redirectIsRefusedAndKeepsTheToken() throws Exception {
        try (var target = Stub.start(); var redirecting = Stub.start(Duration.ZERO, 302, target.endpoint() + "/search/")) {
            var task = Probe.builder()
                .endpoint(Property.ofValue(redirecting.endpoint()))
                .token(Property.ofValue("token"))
                .build();

            var e = assertThrows(Exception.class, () -> task.request(runContextFactory.of(), get("/search/")));

            assertThat(e.getMessage(), containsString("response code '302'"));
            assertThat(target.seen(), is(empty()));
        }
    }

    @Test
    void endpointNotAnHttpUrl() {
        for (var invalid : List.of("", "localhost", "localhost:18080", "http://", "http:/search", "http://localhost:18080?x=1", "http://:8080", "http://localhost:abc", "http://u:pw@localhost", "http://u@localhost")) {
            var e = assertThrows(IllegalArgumentException.class, () -> probe(invalid).request(runContextFactory.of(), get("/search/")), invalid);

            assertThat(e.getMessage(), containsString("got: '" + invalid + "'"));
        }
    }

    @Test
    void requestKeepsMethodHeadersAndBody() throws Exception {
        try (var stub = Stub.start()) {
            var request = HttpRequest.builder()
                .uri(URI.create("/document/v1/music/music/docid/1"))
                .method("POST")
                .addHeader("X-Probe", "kept")
                .body(HttpRequest.JsonRequestBody.builder().content(Map.of("fields", Map.of("album", "Kind of Blue"))).build())
                .build();

            probe(stub.endpoint().replace("http://", "HTTP://")).request(runContextFactory.of(), request);

            assertThat(stub.seen().getFirst().method(), is("POST"));
            assertThat(stub.seen().getFirst().probeHeader(), is("kept"));
            assertThat(stub.seen().getFirst().body(), is("""
                {"fields":{"album":"Kind of Blue"}}"""));
        }
    }

    @Test
    void readTimeout() throws Exception {
        try (var stub = Stub.start(Duration.ofSeconds(2), 200, null)) {
            var task = Probe.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .readTimeout(Property.ofValue(Duration.ofMillis(500)))
                .build();
            var start = System.nanoTime();

            var e = assertThrows(Exception.class, () -> task.request(runContextFactory.of(), get("/search/")));

            assertThat(Duration.ofNanos(System.nanoTime() - start), lessThan(Duration.ofSeconds(2)));
            assertThat(e.getMessage(), containsStringIgnoringCase("timed out"));
        }
    }

    @Test
    void tokenNotInToString() {
        var task = Probe.builder().endpoint(Property.ofValue(endpoint)).token(Property.ofValue("s3cr3t-token")).build();

        assertThat(task.toString(), not(containsString("s3cr3t-token")));
    }

    @Test
    void endpointRequired() {
        var task = Probe.builder().id("probe").type(Probe.class.getName()).build();

        var violations = modelValidator.isValid(task);

        assertThat(violations.isPresent(), is(true));
        assertThat(violations.get().getMessage(), containsString("endpoint"));
    }

    private static Probe probe(String endpoint) {
        return Probe.builder().endpoint(Property.ofValue(endpoint)).build();
    }

    private static HttpRequest get(String pathAndQuery) {
        return HttpRequest.builder().uri(URI.create(pathAndQuery)).method("GET").build();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    // For what the test container can't show: data-plane tokens are Vespa Cloud only, a slow answer, a redirect.
    private record Stub(HttpServer server, List<Seen> seen) implements AutoCloseable {
        static Stub start() throws IOException {
            return start(Duration.ZERO, 200, null);
        }

        static Stub start(Duration delay, int status, String location) throws IOException {
            var seen = new CopyOnWriteArrayList<Seen>();
            var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.createContext("/", exchange -> {
                seen.add(new Seen(
                    exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(),
                    exchange.getRequestHeaders().get("Authorization"),
                    exchange.getRequestHeaders().getFirst("X-Probe"),
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)
                ));
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (location != null) {
                    exchange.getResponseHeaders().add("Location", location);
                }
                var body = "{}".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.start();
            return new Stub(server, seen);
        }

        String endpoint() {
            return "http://localhost:" + server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    private record Seen(String method, String path, List<String> authorization, String probeHeader, String body) {
    }

    @SuperBuilder
    @NoArgsConstructor
    public static class Probe extends AbstractVespaTask {
    }
}
