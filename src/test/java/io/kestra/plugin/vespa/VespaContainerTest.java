package io.kestra.plugin.vespa;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.kestra.core.serializers.JacksonMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VespaContainerTest extends VespaContainer {
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @Test
    void queryApiAnswersOnEndpoint() throws Exception {
        HttpResponse<String> response = get(endpoint + "/status.html");

        assertThat(endpoint, is("http://localhost:18080"));
        assertThat(response.statusCode(), is(200));
    }

    @Test
    void catalogIsFullyIndexed() throws Exception {
        String yql = URLEncoder.encode("select * from music where true", StandardCharsets.UTF_8);

        JsonNode root = json(get(endpoint + "/search/?hits=0&yql=" + yql)).path("root");

        assertThat(root.path("fields").path("totalCount").asLong(), is((long) catalog().size()));
    }

    @Test
    void everyCatalogDocumentRoundTrips() throws Exception {
        List<JsonNode> catalog = catalog();

        for (JsonNode expected : catalog) {
            String localId = expected.path("put").asText().split("::", 2)[1];
            String url = endpoint + "/document/v1/mynamespace/music/docid/" + URLEncoder.encode(localId, StandardCharsets.UTF_8) + "?format.tensors=short-value";

            JsonNode actual = json(get(url));

            assertThat(actual.path("id").asText(), is(expected.path("put").asText()));
            assertThat(expected.path("put").asText(), atTensorFloatPrecision(actual.path("fields")), is(atTensorFloatPrecision(expected.path("fields"))));
        }
        assertThat(catalog.stream().map(d -> d.path("put").asText()).toList(), hasItems(
            "id:mynamespace:music::liebe-ist-für-alle-da",
            "id:mynamespace:music::ac/dc back in black"
        ));
    }

    @Test
    void deployRejectionFailsWithBody(@TempDir Path app) throws Exception {
        Files.writeString(app.resolve("services.xml"), "<services version=\"1.0\"><not-a-cluster/></services>");

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> deploy(app));

        assertThat(e.getMessage(), allOf(containsString("400"), containsString("not-a-cluster")));
    }

    @Test
    void feedRejectionFailsWithBody(@TempDir Path dir) throws Exception {
        Path catalog = dir.resolve("bad.jsonl");
        Files.writeString(catalog, "{\"put\": \"id:mynamespace:music::bad-doc\", \"fields\": {\"no_such_field\": 1}}\n");

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> feed(catalog));

        assertThat(e.getMessage(), allOf(containsString("bad-doc"), containsString("400"), containsString("no_such_field")));
    }

    @Test
    void feedRejectsUnsupportedLine(@TempDir Path dir) throws Exception {
        for (String line : List.of(
            "{\"remove\": \"id:mynamespace:music::not-a-put\"}",
            "{\"put\": \"id:mynamespace:music:n=1:numbered\", \"fields\": {\"album\": \"x\"}}"
        )) {
            Path catalog = dir.resolve("unsupported.jsonl");
            Files.writeString(catalog, line + "\n");

            IllegalStateException e = assertThrows(IllegalStateException.class, () -> feed(catalog));

            assertThat(e.getMessage(), containsString(line));
        }
        assertThat(get(endpoint + "/document/v1/mynamespace/music/docid/numbered").statusCode(), is(404));
    }

    @Test
    void awaitTimeoutNamesLastAnswer() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> await("a missing page", Duration.ofSeconds(1), answers(endpoint + "/no-such-path", r -> r.statusCode() == 200)));

        assertThat(e.getMessage(), allOf(containsString("a missing page"), containsString("last answer: 404")));
    }

    private static JsonNode atTensorFloatPrecision(JsonNode node) {
        if (node.isNumber()) {
            double value = node.doubleValue();
            return value == Math.rint(value) ? LongNode.valueOf(node.longValue()) : DoubleNode.valueOf((float) value);
        }
        if (node.isObject()) {
            ObjectNode copy = JsonNodeFactory.instance.objectNode();
            node.properties().forEach(e -> copy.set(e.getKey(), atTensorFloatPrecision(e.getValue())));
            return copy;
        }
        if (node.isArray()) {
            ArrayNode copy = JsonNodeFactory.instance.arrayNode();
            node.forEach(item -> copy.add(atTensorFloatPrecision(item)));
            return copy;
        }
        return node;
    }

    private static List<JsonNode> catalog() throws Exception {
        Path path = Path.of(VespaContainerTest.class.getClassLoader().getResource("vespa/catalog.jsonl").toURI());
        return Files.readAllLines(path).stream().filter(l -> !l.isBlank()).map(VespaContainerTest::parse).toList();
    }

    private static HttpResponse<String> get(String url) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static JsonNode json(HttpResponse<String> response) {
        assertThat(response.body(), response.statusCode(), is(200));
        return parse(response.body());
    }

    private static JsonNode parse(String body) {
        try {
            return JacksonMapper.ofJson().readTree(body);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
