package io.kestra.plugin.vespa;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.serializers.JacksonMapper;
import jakarta.inject.Inject;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class VespaConnectionTest extends VespaContainer {
    @Inject
    private ModelValidator modelValidator;

    @Test
    void request() throws Exception {
        var response = send(probe(endpoint()), get("/search/?yql=" + encode("select * from music where true") + "&hits=0"));

        assertThat(response.getStatus().getCode(), is(200));
        assertThat(JacksonMapper.ofJson().readTree(response.getBody()).at("/root/fields/totalCount").asLong(), greaterThan(0L));
    }

    @Test
    void endpointWithTrailingSlash() throws Exception {
        try (var stub = VespaStub.start()) {
            send(probe(stub.endpoint() + "//"), get("/search/"));

            assertThat(stub.seen().getFirst().path(), is("/search/"));
        }
    }

    @Test
    void bearerToken() throws Exception {
        try (var stub = VespaStub.start()) {
            var task = Probe.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .token(Property.ofExpression("{{ 'tok' ~ 'en' }}"))
                .build();

            send(task, get("/search/"));

            assertThat(stub.seen().getFirst().authorization(), contains("Bearer token"));
        }
    }

    @Test
    void noTokenNoAuthorization() throws Exception {
        try (var stub = VespaStub.start()) {
            send(probe(stub.endpoint()), get("/search/"));

            assertThat(stub.seen().getFirst().authorization(), is(nullValue()));
        }
    }

    @Test
    void blankTokenNoAuthorization() throws Exception {
        try (var stub = VespaStub.start()) {
            var task = Probe.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .token(Property.ofExpression("{{ '' }}"))
                .build();

            send(task, get("/search/"));

            assertThat(stub.seen().getFirst().authorization(), is(nullValue()));
        }
    }

    @Test
    void redirectIsRefusedAndKeepsTheToken() throws Exception {
        try (var target = VespaStub.start(); var redirecting = VespaStub.start(Duration.ZERO, 302, target.endpoint() + "/search/")) {
            var task = Probe.builder()
                .endpoint(Property.ofValue(redirecting.endpoint()))
                .token(Property.ofValue("token"))
                .build();

            var e = assertThrows(Exception.class, () -> send(task, get("/search/")));

            assertThat(e.getMessage(), containsString("response code '302'"));
            assertThat(target.seen(), is(empty()));
        }
    }

    @Test
    void requestsShareOneConnection() throws Exception {
        try (var stub = VespaStub.start()) {
            var task = probe(stub.endpoint());
            var runContext = runContextFactory.of();

            try (var client = task.client(runContext)) {
                task.request(runContext, client, get("/search/"));
                task.request(runContext, client, get("/search/"));
            }

            assertThat(stub.seen(), hasSize(2));
            assertThat(stub.seen().getLast().clientPort(), is(stub.seen().getFirst().clientPort()));
        }
    }

    @Test
    void endpointNotAnHttpUrl() {
        for (var invalid : List.of("", "localhost", "localhost:18080", "http://", "http:/search", "http://localhost:18080?x=1", "http://:8080", "http://localhost:abc", "http://u:pw@localhost", "http://u@localhost", "http://a^b:8080", "http://h:8080/%zz", "http://[zz]:8080", "http://a|b", "http://h/x{y}", "http://h:99999", "http://h:0", "http://h:99999999999", "http://[::1]:70000")) {
            var e = assertThrows(IllegalArgumentException.class, () -> send(probe(invalid), get("/search/")), invalid);

            assertThat(e.getMessage(), containsString("got: '%s'".formatted(invalid)));
        }
    }

    @Test
    void endpointWithAnyValidPort() throws Exception {
        var runContext = runContextFactory.of();
        for (var valid : List.of("http://h", "http://h:1", "http://h:65535", "http://a_b:8080", "http://[::1]:8080")) {
            assertThat(valid, probe(valid).endpoint(runContext), is(valid));
        }
    }

    @Test
    void answerOverTheCapFails() throws Exception {
        // a body that never ends: the read must stop at the cap, not drain the rest
        try (var stub = VespaStub.answering(VespaStub.Answer.streamed(Long.MAX_VALUE))) {
            var e = assertTimeoutPreemptively(Duration.ofSeconds(5), () -> assertThrows(IllegalStateException.class, () -> send(probe("%s/prefix".formatted(stub.endpoint())), get("/search/?yql=secret"))));

            assertThat(e.getMessage(), is("Vespa's answer to GET /prefix/search/ is larger than 33554432 bytes. Check that `endpoint` points to Vespa, and for a query ask for fewer `hits` or summary fields."));
        }
    }

    @Test
    void answerOneByteOverTheCapFails() throws Exception {
        try (var stub = VespaStub.answering(VespaStub.Answer.streamed(32L * 1024 * 1024 + 1))) {
            var e = assertThrows(IllegalStateException.class, () -> send(probe(stub.endpoint()), get("/search/")));

            assertThat(e.getMessage(), containsString("larger than 33554432 bytes"));
        }
    }

    @Test
    void answerAtTheCapIsRead() throws Exception {
        try (var stub = VespaStub.answering(VespaStub.Answer.streamed(32L * 1024 * 1024))) {
            var response = send(probe(stub.endpoint()), get("/search/"));

            assertThat(response.getBody().length(), is(32 * 1024 * 1024));
        }
    }

    @Test
    void errorAnswerOverTheCapFails() throws Exception {
        try (var stub = VespaStub.answering(VespaStub.Answer.streamed(500, Long.MAX_VALUE))) {
            var e = assertTimeoutPreemptively(Duration.ofSeconds(5), () -> assertThrows(IllegalStateException.class, () -> send(probe(stub.endpoint()), get("/search/"))));

            assertThat(e.getMessage(), startsWith("Vespa's answer to GET /search/ is larger than 33554432 bytes."));
        }
    }

    @Test
    void errorAnswerKeepsCoreMessageAndBody() throws Exception {
        try (var stub = VespaStub.answering(404, """
            {"message":"not here"}""")) {
            var e = assertThrows(HttpClientResponseException.class, () -> send(probe(stub.endpoint()), get("/document/v1/a/b/docid/1")));

            assertThat(e.getMessage(), allOf(startsWith("Failed http request with response code '404' and body:"), endsWith("""
                {"message":"not here"}""")));
            assertThat(e.getResponse().getStatus().getCode(), is(404));
            assertThat(new String((byte[]) e.getResponse().getBody(), StandardCharsets.UTF_8), is("""
                {"message":"not here"}"""));
        }
    }

    @Test
    void errorAnswerWithoutBody() throws Exception {
        try (var stub = VespaStub.answering(500, "")) {
            var head = HttpRequest.builder().uri(URI.create("/search/")).method("HEAD").build();

            var e = assertThrows(HttpClientResponseException.class, () -> send(probe(stub.endpoint()), head));

            assertThat(e.getMessage(), is("Failed http request with response code '500'"));
        }
    }

    @Test
    void answerCutOffFails() throws Exception {
        try (var stub = VespaStub.answering(VespaStub.Answer.cut(1024))) {
            var e = assertThrows(IllegalStateException.class, () -> send(probe(stub.endpoint()), get("/search/")));

            assertThat(e.getMessage(), allOf(
                startsWith("Reading Vespa's answer to GET /search/ failed: "),
                endsWith(". Check that `endpoint` points to Vespa and try again.")
            ));
        }
    }

    @Test
    void answerStalledFails() throws Exception {
        try (var stub = VespaStub.answering(VespaStub.Answer.stalled(1024, Duration.ofSeconds(2)))) {
            var task = Probe.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .readTimeout(Property.ofValue(Duration.ofMillis(300)))
                .build();

            var e = assertThrows(IllegalStateException.class, () -> send(task, get("/search/")));

            assertThat(e.getMessage(), is("Vespa's answer to GET /search/ stopped for longer than `readTimeout`. Raise `readTimeout`, or for a query ask for fewer `hits`."));
        }
    }

    @Test
    void requestKeepsMethodHeadersAndBody() throws Exception {
        try (var stub = VespaStub.start()) {
            var request = HttpRequest.builder()
                .uri(URI.create("/document/v1/music/music/docid/1"))
                .method("POST")
                .addHeader("X-Probe", "kept")
                .body(HttpRequest.JsonRequestBody.builder().content(Map.of("fields", Map.of("album", "Kind of Blue"))).build())
                .build();

            send(probe(stub.endpoint().replace("http://", "HTTP://")), request);

            assertThat(stub.seen().getFirst().method(), is("POST"));
            assertThat(stub.seen().getFirst().probeHeader(), is("kept"));
            assertThat(stub.seen().getFirst().body(), is("""
                {"fields":{"album":"Kind of Blue"}}"""));
        }
    }

    @Test
    void readTimeout() throws Exception {
        try (var stub = VespaStub.start(Duration.ofSeconds(2), 200, null)) {
            var task = Probe.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .readTimeout(Property.ofValue(Duration.ofMillis(500)))
                .build();
            var start = System.nanoTime();

            var e = assertThrows(Exception.class, () -> send(task, get("/search/")));

            assertThat(Duration.ofNanos(System.nanoTime() - start), lessThan(Duration.ofSeconds(2)));
            assertThat(e.getMessage(), containsStringIgnoringCase("timed out"));
        }
    }

    @Test
    void tokenNotInToString() {
        var task = Probe.builder().endpoint(Property.ofValue(endpoint())).token(Property.ofValue("s3cr3t-token")).build();

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

    private HttpResponse<String> send(Probe task, HttpRequest request) throws Exception {
        var runContext = runContextFactory.of();
        try (var client = task.client(runContext)) {
            return task.request(runContext, client, request);
        }
    }

    private static HttpRequest get(String pathAndQuery) {
        return HttpRequest.builder().uri(URI.create(pathAndQuery)).method("GET").build();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    @SuperBuilder
    @NoArgsConstructor
    public static class Probe extends AbstractVespaTask {
    }
}
