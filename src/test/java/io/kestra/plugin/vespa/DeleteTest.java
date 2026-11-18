package io.kestra.plugin.vespa;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.models.property.Property;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DeleteTest extends VespaContainer {
    private static final String ID = "id:mynamespace:music::deletetest-delete";
    private static final String PATH = "/document/v1/mynamespace/music/docid/deletetest-delete";
    private static final String DELETED = """
        {"pathId": "/document/v1/mynamespace/music/docid/deletetest-delete", "id": "id:mynamespace:music::deletetest-delete"}""";

    @Test
    void delete() throws Exception {
        put(PATH, Map.of("artist", "deletetest", "album", "delete"));
        var task = Delete.builder().endpoint(Property.ofValue(endpoint())).documentId(Property.ofValue(ID)).build();

        var output = task.run(runContextFactory.of());

        assertThat(status(PATH), is(404));
        assertThat(output.getDeleted(), is(nullValue()));
    }

    @Test
    void missing() throws Exception {
        var id = "id:mynamespace:music::deletetest-never-fed";
        var task = Delete.builder().endpoint(Property.ofValue(endpoint())).documentId(Property.ofValue(id)).build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getDeleted(), is(nullValue()));
    }

    @Test
    void invalidId() throws Exception {
        try (var stub = VespaStub.answering(DELETED)) {
            for (var id : List.of("kind-of-blue", "id:mynamespace:music::..")) {
                var task = Delete.builder().endpoint(Property.ofValue(stub.endpoint())).documentId(Property.ofValue(id)).build();

                var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()), "'" + id + "'");

                assertThat(id, e.getMessage(), allOf(containsString("'" + id + "'"), containsString("id:<namespace>:<document type>::<key>")));
            }
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void oneDeleteRequest() throws Exception {
        try (var stub = VespaStub.answering(DELETED)) {
            var task = Delete.builder().endpoint(Property.ofValue(stub.endpoint())).documentId(Property.ofValue("id:mynamespace:music::ac/dc back in black")).build();

            task.run(runContextFactory.of());

            assertThat(stub.seen(), hasSize(1));
            var seen = stub.seen().getFirst();
            assertThat(seen.method(), is("DELETE"));
            assertThat(seen.path(), is("/document/v1/mynamespace/music/docid/ac%2Fdc%20back%20in%20black"));
            assertThat(seen.query(), is(nullValue()));
        }
    }

    @Test
    void selection() throws Exception {
        for (var key : List.of("a", "b", "c")) {
            put("/document/v1/mynamespace/music/docid/deletetest-selection-" + key, Map.of("artist", "deletetest selection", "album", key));
        }
        var allBefore = count("true");
        assertThat(count("artist contains \"deletetest selection\""), is(3L));
        var task = Delete.builder()
            .endpoint(Property.ofValue(endpoint()))
            .selection(Property.ofValue("music.artist == \"deletetest selection\""))
            .cluster(Property.ofValue("music"))
            .allowSelection(Property.ofValue(true))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(count("artist contains \"deletetest selection\""), is(0L));
        assertThat(count("true"), is(allBefore - 3));
        assertThat(output.getDeleted(), is(3L));
    }

    @Test
    void selectionMatchesNothing() throws Exception {
        var allBefore = count("true");
        var task = Delete.builder()
            .endpoint(Property.ofValue(endpoint()))
            .selection(Property.ofValue("music.artist == \"deletetest nobody\""))
            .cluster(Property.ofValue("music"))
            .allowSelection(Property.ofValue(true))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getDeleted(), is(0L));
        assertThat(count("true"), is(allBefore));
    }

    @Test
    void selectionNotAllowed() throws Exception {
        try (var stub = VespaStub.answering(DELETED)) {
            for (var allowSelection : Arrays.asList(null, false)) {
                var task = Delete.builder()
                    .endpoint(Property.ofValue(stub.endpoint()))
                    .selection(Property.ofValue("true"))
                    .cluster(Property.ofValue("music"))
                    .allowSelection(allowSelection == null ? null : Property.ofValue(allowSelection))
                    .build();

                var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()), String.valueOf(allowSelection));

                assertThat(e.getMessage(), containsString("allowSelection"));
            }
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void idAndSelection() throws Exception {
        try (var stub = VespaStub.answering(DELETED)) {
            var task = Delete.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .documentId(Property.ofValue(ID))
                .selection(Property.ofValue("true"))
                .cluster(Property.ofValue("music"))
                .allowSelection(Property.ofValue(true))
                .build();

            var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), allOf(containsString("exactly one"), containsString("documentId"), containsString("selection")));
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void neither() throws Exception {
        try (var stub = VespaStub.answering(DELETED)) {
            var task = Delete.builder().endpoint(Property.ofValue(stub.endpoint())).build();

            var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), allOf(containsString("exactly one"), containsString("documentId"), containsString("selection")));
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void selectionWithoutCluster() throws Exception {
        try (var stub = VespaStub.answering(DELETED)) {
            for (var cluster : Arrays.asList(null, " ")) {
                var task = Delete.builder()
                    .endpoint(Property.ofValue(stub.endpoint()))
                    .selection(Property.ofValue("true"))
                    .cluster(cluster == null ? null : Property.ofValue(cluster))
                    .allowSelection(Property.ofValue(true))
                    .build();

                var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()), "'" + cluster + "'");

                assertThat(e.getMessage(), containsString("cluster"));
            }
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void blankSelection() throws Exception {
        try (var stub = VespaStub.answering(DELETED)) {
            for (var selection : List.of("", " ")) {
                var task = Delete.builder()
                    .endpoint(Property.ofValue(stub.endpoint()))
                    .selection(Property.ofValue(selection))
                    .cluster(Property.ofValue("music"))
                    .allowSelection(Property.ofValue(true))
                    .build();

                var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()), "'" + selection + "'");

                assertThat(e.getMessage(), containsString("selection"));
            }
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void continuation() throws Exception {
        var answers = List.of(
            """
                {"pathId": "/document/v1/", "documentCount": 2, "continuation": "AAA+/="}""",
            """
                {"pathId": "/document/v1/", "documentCount": 0, "continuation": "BBB"}""",
            """
                {"pathId": "/document/v1/", "documentCount": 5}"""
        );
        try (var stub = VespaStub.answering(answers)) {
            var task = Delete.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .selection(Property.ofValue("music.artist == \"a b\""))
                .cluster(Property.ofValue("music"))
                .allowSelection(Property.ofValue(true))
                .build();

            var output = task.run(runContextFactory.of());

            assertThat(output.getDeleted(), is(7L));
            assertThat(stub.seen(), hasSize(3));
            var selection = "cluster=music&selection=music.artist%20%3D%3D%20%22a%20b%22";
            assertThat(stub.seen().stream().map(VespaStub.Seen::method).distinct().toList(), contains("DELETE"));
            assertThat(stub.seen().stream().map(VespaStub.Seen::path).distinct().toList(), contains("/document/v1/"));
            assertThat(stub.seen().stream().map(VespaStub.Seen::query).toList(), contains(
                selection,
                selection + "&continuation=AAA%2B%2F%3D",
                selection + "&continuation=BBB"
            ));
        }
    }

    @Test
    void selectionMessage() throws Exception {
        var lastAnswers = List.of("""
            {"pathId": "/document/v1/", "documentCount": 2, "message": "Visiting failed: content cluster is down"}""", """
            {"pathId": "/document/v1/", "message": "Visiting failed: content cluster is down"}""");
        var expected = List.of("after 6 documents", "after 4 documents");
        for (var i = 0; i < lastAnswers.size(); i++) {
            try (var stub = VespaStub.answering(List.of("""
                {"pathId": "/document/v1/", "documentCount": 4, "continuation": "AAA"}""", lastAnswers.get(i)))) {
                var task = Delete.builder()
                    .endpoint(Property.ofValue(stub.endpoint()))
                    .selection(Property.ofValue("true"))
                    .cluster(Property.ofValue("music"))
                    .allowSelection(Property.ofValue(true))
                    .build();

                var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()), lastAnswers.get(i));

                assertThat(lastAnswers.get(i), e.getMessage(), allOf(startsWith("Vespa stopped the delete by selection"), containsString(expected.get(i)), containsString("Visiting failed: content cluster is down")));
            }
        }
    }

    @Test
    void failedChunk() throws Exception {
        var firstChunk = new VespaStub.Answer(200, """
            {"pathId": "/document/v1/", "documentCount": 4, "continuation": "AAA"}""");
        var failures = List.of(
            new VespaStub.Answer(500, """
                {"pathId": "/document/v1/", "documentCount": 2, "message": "Visiting failed: content node down"}"""),
            new VespaStub.Answer(500, """
                {"pathId": "/document/v1/", "message": "Internal error"}"""),
            new VespaStub.Answer(500, """
                {"documentCount": 2, "message": "gateway error"}"""),
            new VespaStub.Answer(200, ""),
            new VespaStub.Answer(200, "<html>proxy</html>"),
            new VespaStub.Answer(200, """
                {"status": "ok"}"""),
            new VespaStub.Answer(502, "<html>bad gateway</html>"),
            new VespaStub.Answer(503, """
                {"pathId": "/document/v1/", "documentCount": "2", "message": "overloaded"}""")
        );
        var expected = List.of(
            List.of("after Vespa deleted 6 documents", "Visiting failed: content node down"),
            List.of("after Vespa deleted 4 documents", "500"),
            List.of("after Vespa deleted 4 documents", "gateway error"),
            List.of("after Vespa deleted 4 documents", "empty answer to the delete"),
            List.of("after Vespa deleted 4 documents", "delete isn't JSON"),
            List.of("after Vespa deleted 4 documents", "delete isn't a delete result"),
            List.of("after Vespa deleted 4 documents", "502"),
            List.of("after Vespa deleted 4 documents", "overloaded")
        );
        for (var i = 0; i < failures.size(); i++) {
            try (var stub = VespaStub.answering(firstChunk, failures.get(i))) {
                var task = Delete.builder()
                    .endpoint(Property.ofValue(stub.endpoint()))
                    .selection(Property.ofValue("true"))
                    .cluster(Property.ofValue("music"))
                    .allowSelection(Property.ofValue(true))
                    .build();

                var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()), failures.get(i).body());

                assertThat(failures.get(i).body(), e.getMessage(), allOf(containsString(expected.get(i).get(0)), containsString(expected.get(i).get(1))));
                assertThat(failures.get(i).body(), e.getCause(), is(notNullValue()));
                if (failures.get(i).status() == 500) {
                    assertThat(e.getCause(), instanceOf(HttpClientResponseException.class));
                }
            }
        }

        try (var stub = VespaStub.answering(firstChunk, new VespaStub.Answer(502, "<html>" + "x".repeat(5000) + "</html>"))) {
            var task = Delete.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .selection(Property.ofValue("true"))
                .cluster(Property.ofValue("music"))
                .allowSelection(Property.ofValue(true))
                .build();

            var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), allOf(containsString("after Vespa deleted 4 documents"), containsString("502")));
            assertThat(e.getMessage().length(), is(lessThan(1200)));
        }

        try (var stub = VespaStub.answering(firstChunk, new VespaStub.Answer(200, "<html>proxy</html>"))) {
            var task = Delete.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .selection(Property.ofValue("true"))
                .cluster(Property.ofValue("music"))
                .allowSelection(Property.ofValue(true))
                .build();

            var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getCause().getCause(), instanceOf(JsonProcessingException.class));
        }

        try (var stub = VespaStub.answering(new VespaStub.Answer(500, """
            {"pathId": "/document/v1/", "documentCount": 0, "message": "Visiting failed"}"""))) {
            var task = Delete.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .selection(Property.ofValue("true"))
                .cluster(Property.ofValue("music"))
                .allowSelection(Property.ofValue(true))
                .build();

            assertThrows(HttpClientResponseException.class, () -> task.run(runContextFactory.of()));
        }

        try (var stub = VespaStub.answering(new VespaStub.Answer(500, """
            {"pathId": "/document/v1/", "documentCount": 3, "message": "Visiting failed"}"""))) {
            var task = Delete.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .selection(Property.ofValue("true"))
                .cluster(Property.ofValue("music"))
                .allowSelection(Property.ofValue(true))
                .build();

            var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), containsString("after Vespa deleted 3 documents"));
        }
    }

    @Test
    void answerWrongShape() throws Exception {
        for (var answer : List.of("[]", """
            {"id": "id:mynamespace:music::deletetest-delete"}""", """
            {"pathId": "/search/", "id": "id:mynamespace:music::deletetest-delete"}""", """
            {"pathId": "/document/v1/mynamespace/music/docid/deletetest-delete"}""", """
            {"pathId": "/document/v1/mynamespace/music/docid/deletetest-delete", "id": 5}""")) {
            try (var stub = VespaStub.answering(answer)) {
                var task = Delete.builder().endpoint(Property.ofValue(stub.endpoint())).documentId(Property.ofValue(ID)).build();

                var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()), answer);

                assertThat(answer, e.getMessage(), allOf(containsString("delete isn't a delete result"), containsString("endpoint")));
            }
        }
        for (var answer : List.of("[]", """
            {"documentCount": 3}""", """
            {"pathId": "/search/", "documentCount": 3}""", """
            {"pathId": "/document/v1/", "documentCount": "3"}""", """
            {"message": "route not found"}""", DELETED)) {
            try (var stub = VespaStub.answering(answer)) {
                var task = Delete.builder()
                    .endpoint(Property.ofValue(stub.endpoint()))
                    .selection(Property.ofValue("true"))
                    .cluster(Property.ofValue("music"))
                    .allowSelection(Property.ofValue(true))
                    .build();

                var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()), answer);

                assertThat(answer, e.getMessage(), allOf(containsString("delete isn't a delete result"), containsString("endpoint")));
            }
        }

        try (var stub = VespaStub.answering(List.of("""
            {"pathId": "/document/v1/", "continuation": "AAA", "message": "partial"}""", """
            {"pathId": "/document/v1/", "documentCount": 0}"""))) {
            var task = Delete.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .selection(Property.ofValue("true"))
                .cluster(Property.ofValue("music"))
                .allowSelection(Property.ofValue(true))
                .build();

            var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), containsString("delete isn't a delete result"));
            assertThat(stub.seen(), hasSize(1));
        }
    }

    @Test
    void badSelection() {
        var task = Delete.builder()
            .endpoint(Property.ofValue(endpoint()))
            .selection(Property.ofValue("music.artist === "))
            .cluster(Property.ofValue("music"))
            .allowSelection(Property.ofValue(true))
            .build();

        var e = assertThrows(HttpClientResponseException.class, () -> task.run(runContextFactory.of()));

        assertThat(e.getMessage(), allOf(containsString("400"), containsString("Was expecting one of")));
    }

    private static long count(String where) throws IOException {
        return search(Map.of("yql", "select * from music where " + where, "hits", 0)).at("/root/fields/totalCount").asLong();
    }
}
