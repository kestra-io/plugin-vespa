package io.kestra.plugin.vespa;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.serializers.JacksonMapper;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GetTest extends VespaContainer {
    private static final String KIND_OF_BLUE = "id:mynamespace:music::kind-of-blue";
    private static final String MISSING = "id:mynamespace:music::nope-nope";
    private static final String DOCUMENT = """
        {"pathId": "/document/v1/mynamespace/music/docid/kind-of-blue", "id": "id:mynamespace:music::kind-of-blue", "fields": {"album": "Kind of Blue"}}""";

    @Inject
    private ModelValidator modelValidator;

    @Test
    void get() throws Exception {
        var task = Get.builder().endpoint(Property.ofValue(endpoint())).documentId(Property.ofValue(KIND_OF_BLUE)).build();

        var output = task.run(runContextFactory.of());

        var expected = document("/document/v1/mynamespace/music/docid/kind-of-blue");
        assertThat(output.getId(), is(KIND_OF_BLUE));
        assertThat(output.getDocument(), is(JacksonMapper.ofJson().convertValue(expected.get("fields"), new TypeReference<LinkedHashMap<String, Object>>() {})));
        assertThat(output.getDocument(), hasEntry("album", "Kind of Blue"));
        assertThat(output.getDocument().get("embedding"), is(instanceOf(Map.class)));
    }

    @Test
    void idWithSlashAndSpaces() throws Exception {
        var id = "id:mynamespace:music::ac/dc back in black";
        var task = Get.builder().endpoint(Property.ofValue(endpoint())).documentId(Property.ofValue(id)).build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getId(), is(id));
        assertThat(output.getDocument(), hasEntry("album", "Back in Black"));
        assertThat(output.getDocument(), hasEntry("artist", "AC/DC"));
    }

    @Test
    void numberGroupAndOddCharacters() throws Exception {
        // Vespa answers a missing document with the ID it read from the path, so a wrong path shows as a different ID
        for (var id : List.of("id:mynamespace:music:n=1:x", "id:mynamespace:music:g=g1:x", "id:mynamespace:music::a:b?c#d%e+f", "id:my namespace:music::x", "id:mynamespace:music::a.b", "id:mynamespace:music::...")) {
            var task = Get.builder().endpoint(Property.ofValue(endpoint())).documentId(Property.ofValue(id)).build();

            var output = task.run(runContextFactory.of());

            assertThat(id, output.getId(), is(id));
            assertThat(id, output.getDocument(), is(nullValue()));
        }
    }

    @Test
    void missing() throws Exception {
        var task = Get.builder().endpoint(Property.ofValue(endpoint())).documentId(Property.ofValue(MISSING)).build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getId(), is(MISSING));
        assertThat(output.getDocument(), is(nullValue()));
    }

    @Test
    void missingFails() {
        var task = Get.builder().endpoint(Property.ofValue(endpoint())).documentId(Property.ofValue(MISSING)).errorOnMissing(Property.ofValue(true)).build();

        var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

        assertThat(e.getMessage(), allOf(containsString(MISSING), containsString("doesn't exist")));
    }

    @Test
    void unknownDocumentType() {
        var task = Get.builder()
            .endpoint(Property.ofValue(endpoint()))
            .documentId(Property.ofValue("id:mynamespace:nosuchtype::kind-of-blue"))
            .errorOnMissing(Property.ofValue(true))
            .build();

        var e = assertThrows(HttpClientResponseException.class, () -> task.run(runContextFactory.of()));

        assertThat(e.getMessage(), allOf(containsString("404"), containsString("Unknown bucket space mapping")));
    }

    @Test
    void invalidId() throws Exception {
        var ids = List.of(
            "",
            "kind-of-blue",
            "index:music/0/abc",
            "id:mynamespace:music",
            "id:mynamespace:music:kind-of-blue",
            "id:mynamespace:music::",
            "id::music::x",
            "id:mynamespace:::x",
            "id:mynamespace:music:n=abc:x",
            "id:mynamespace:music:n=:x",
            "id:mynamespace:music:g=:x",
            "id:mynamespace:music:x=1:y",
            "id:mynamespace:music::.",
            "id:mynamespace:music::..",
            "id:.:music::x",
            "id:..:music::x",
            "id:mynamespace:.::x",
            "id:mynamespace:..::x",
            "id:mynamespace:music:g=.:x",
            "id:mynamespace:music:g=..:x"
        );
        try (var stub = VespaStub.answering(DOCUMENT)) {
            for (var id : ids) {
                var task = Get.builder().endpoint(Property.ofValue(stub.endpoint())).documentId(Property.ofValue(id)).build();

                var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()), "'" + id + "'");

                assertThat(id, e.getMessage(), allOf(containsString("'" + id + "'"), containsString("id:<namespace>:<document type>::<key>")));
            }
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void emptyAnswer() throws Exception {
        for (var answer : List.of("", " ")) {
            try (var stub = VespaStub.answering(answer)) {
                var task = Get.builder().endpoint(Property.ofValue(stub.endpoint())).documentId(Property.ofValue(KIND_OF_BLUE)).build();

                var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()), "'" + answer + "'");

                assertThat(e.getMessage(), allOf(containsString("empty answer"), containsString("endpoint")));
            }
        }
    }

    @Test
    void answerNotJson() throws Exception {
        try (var stub = VespaStub.answering("<html>" + "x".repeat(3000) + "</html>")) {
            var task = Get.builder().endpoint(Property.ofValue(stub.endpoint())).documentId(Property.ofValue(KIND_OF_BLUE)).build();

            var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), allOf(containsString("isn't JSON"), containsString("endpoint"), containsString("<html>xxx")));
            assertThat(e.getMessage().length(), is(lessThan(1200)));
        }
    }

    @Test
    void answerNotADocument() throws Exception {
        var answers = List.of(
            "{}",
            "[]",
            """
                {"id": "id:mynamespace:music::kind-of-blue"}""",
            """
                {"id": "id:mynamespace:music::kind-of-blue", "fields": 5}""",
            """
                {"fields": {"album": "Kind of Blue"}}""",
            """
                {"pathId": "/document/v1/mynamespace/music/docid/", "documents": [], "documentCount": 0}"""
        );
        for (var answer : answers) {
            try (var stub = VespaStub.answering(answer)) {
                var task = Get.builder().endpoint(Property.ofValue(stub.endpoint())).documentId(Property.ofValue(KIND_OF_BLUE)).build();

                var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()), answer);

                assertThat(answer, e.getMessage(), allOf(containsString("isn't a document"), containsString("endpoint")));
            }
        }
    }

    @Test
    void notFoundFromAnotherServer() throws Exception {
        for (var answer : List.of("<html>Not Found</html>", "", "{}", """
            {"id": 5}""", """
            {"id": "id:mynamespace:music::kind-of-blue", "message": "Unknown bucket space mapping"}""", """
            {"id": "request-123", "error": "route not found"}""", """
            {"pathId": "/api/route", "id": "id:mynamespace:music::kind-of-blue"}""")) {
            try (var stub = VespaStub.answering(404, answer)) {
                var task = Get.builder().endpoint(Property.ofValue(stub.endpoint())).documentId(Property.ofValue(KIND_OF_BLUE)).build();

                var e = assertThrows(HttpClientResponseException.class, () -> task.run(runContextFactory.of()), answer);

                assertThat(answer, e.getMessage(), allOf(containsString("404"), containsString(answer)));
            }
        }
    }

    @Test
    void serverError() throws Exception {
        // the second answer has the not-found shape, only its status tells it apart
        for (var answer : List.of("""
            {"pathId": "/document/v1/mynamespace/music/docid/kind-of-blue", "message": "Content cluster is down"}""", """
            {"id": "id:mynamespace:music::kind-of-blue"}""")) {
            try (var stub = VespaStub.answering(500, answer)) {
                var task = Get.builder().endpoint(Property.ofValue(stub.endpoint())).documentId(Property.ofValue(KIND_OF_BLUE)).errorOnMissing(Property.ofValue(false)).build();

                var e = assertThrows(HttpClientResponseException.class, () -> task.run(runContextFactory.of()), answer);

                assertThat(answer, e.getMessage(), allOf(containsString("500"), containsString(answer)));
            }
        }
    }

    @Test
    void errorOnMissingCheckedWhenFound() {
        var task = Get.builder().endpoint(Property.ofValue(endpoint())).documentId(Property.ofValue(KIND_OF_BLUE)).errorOnMissing(Property.ofExpression("{{ 'not a boolean' }}")).build();

        assertThrows(IllegalVariableEvaluationException.class, () -> task.run(runContextFactory.of()));
    }

    @Test
    void oneGetRequest() throws Exception {
        try (var stub = VespaStub.answering(DOCUMENT)) {
            var task = Get.builder()
                .endpoint(Property.ofValue(stub.endpoint() + "/vespa"))
                .token(Property.ofValue("token"))
                .documentId(Property.ofValue("id:mynamespace:music::ac/dc back in black"))
                .build();

            task.run(runContextFactory.of());

            assertThat(stub.seen(), hasSize(1));
            var seen = stub.seen().getFirst();
            assertThat(seen.method(), is("GET"));
            assertThat(seen.path(), is("/vespa/document/v1/mynamespace/music/docid/ac%2Fdc%20back%20in%20black"));
            assertThat(seen.query(), is(nullValue()));
            assertThat(seen.body(), is(""));
            assertThat(seen.authorization(), contains("Bearer token"));
        }
    }

    @Test
    void numberAndGroupPaths() throws Exception {
        var paths = Map.of(
            "id:mynamespace:music:n=42:x y", "/document/v1/mynamespace/music/number/42/x%20y",
            "id:mynamespace:music:g=my group:a/b", "/document/v1/mynamespace/music/group/my%20group/a%2Fb"
        );
        try (var stub = VespaStub.answering(DOCUMENT)) {
            for (var idAndPath : paths.entrySet()) {
                Get.builder().endpoint(Property.ofValue(stub.endpoint())).documentId(Property.ofValue(idAndPath.getKey())).build().run(runContextFactory.of());

                assertThat(idAndPath.getKey(), stub.seen().getLast().path(), is(idAndPath.getValue()));
            }
        }
    }

    @Test
    void documentIdRequired() {
        var task = Get.builder().id("get").type(Get.class.getName()).endpoint(Property.ofValue(endpoint())).build();

        var violations = modelValidator.isValid(task);

        assertThat(violations.isPresent(), is(true));
        assertThat(violations.get().getMessage(), containsString("documentId"));
    }
}
