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

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VespaConnectionTest extends VespaContainer {
    @Inject
    private ModelValidator modelValidator;

    @Test
    void request() throws Exception {
        var catalogSize = Files.readAllLines(Path.of(getClass().getClassLoader().getResource("vespa/catalog.jsonl").toURI())).stream()
            .filter(line -> !line.isBlank())
            .count();

        var response = probe(endpoint).request(runContextFactory.of(), get("/search/?yql=" + encode("select * from music where true") + "&hits=0"));

        assertThat(response.getStatus().getCode(), is(200));
        assertThat(JacksonMapper.ofJson().readTree(response.getBody()).at("/root/fields/totalCount").asLong(), is(catalogSize));
    }

    @Test
    void endpointWithTrailingSlash() throws Exception {
        var seen = new ArrayList<Seen>();
        var stub = stub(Duration.ZERO, seen);
        try {
            // a stub, not Vespa, because Vespa answers "//search/" like "/search/"
            probe("http://localhost:" + stub.getAddress().getPort() + "/").request(runContextFactory.of(), get("/search/"));

            assertThat(seen.getFirst().path(), is("/search/"));
        } finally {
            stub.stop(0);
        }
    }

    @Test
    void bearerToken() throws Exception {
        var seen = new ArrayList<Seen>();
        var stub = stub(Duration.ZERO, seen);
        try {
            var task = Probe.builder()
                .endpoint(Property.ofValue("http://localhost:" + stub.getAddress().getPort()))
                .token(Property.ofExpression("{{ 'tok' ~ 'en' }}"))
                .build();

            task.request(runContextFactory.of(), get("/search/"));

            assertThat(seen.getFirst().authorization(), contains("Bearer token"));
        } finally {
            stub.stop(0);
        }
    }

    @Test
    void noTokenNoAuthorization() throws Exception {
        var seen = new ArrayList<Seen>();
        var stub = stub(Duration.ZERO, seen);
        try {
            probe("http://localhost:" + stub.getAddress().getPort()).request(runContextFactory.of(), get("/search/"));

            assertThat(seen.getFirst().authorization(), is(nullValue()));
        } finally {
            stub.stop(0);
        }
    }

    @Test
    void failedRequestHasVespaError() {
        var e = assertThrows(Exception.class, () -> probe(endpoint).request(runContextFactory.of(), get("/search/?yql=" + encode("select from where"))));

        assertThat(e.getMessage(), containsString("400"));
        assertThat(e.getMessage(), containsString("Could not create query from YQL"));
    }

    @Test
    void unreachableEndpoint() throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        var unreachable = "http://localhost:" + port;

        var e = assertThrows(Exception.class, () -> probe(unreachable).request(runContextFactory.of(), get("/search/")));

        assertThat(e.getMessage(), containsString(unreachable));
    }

    @Test
    void readTimeout() throws Exception {
        var stub = stub(Duration.ofSeconds(3), new ArrayList<>());
        try {
            var task = Probe.builder()
                .endpoint(Property.ofValue("http://localhost:" + stub.getAddress().getPort()))
                .readTimeout(Property.ofValue(Duration.ofSeconds(1)))
                .build();
            var start = System.nanoTime();

            var e = assertThrows(Exception.class, () -> task.request(runContextFactory.of(), get("/search/")));

            assertThat(Duration.ofNanos(System.nanoTime() - start), lessThan(Duration.ofSeconds(3)));
            assertThat(e.getMessage(), containsStringIgnoringCase("timed out"));
        } finally {
            stub.stop(0);
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

    private static HttpRequest.HttpRequestBuilder get(String pathAndQuery) {
        return HttpRequest.builder().uri(URI.create(pathAndQuery)).method("GET");
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    // For what the test container can't show: data-plane tokens are Vespa Cloud only, and a slow answer.
    private static HttpServer stub(Duration delay, List<Seen> seen) throws Exception {
        var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", exchange -> {
            seen.add(new Seen(exchange.getRequestURI().getPath(), exchange.getRequestHeaders().get("Authorization")));
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            var body = "{}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        return server;
    }

    private record Seen(String path, List<String> authorization) {
    }

    @SuperBuilder
    @NoArgsConstructor
    public static class Probe extends AbstractVespaTask {
    }
}
