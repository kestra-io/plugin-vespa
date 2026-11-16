package io.kestra.plugin.vespa;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryTest extends VespaContainer {
    private static final String CONTAINS_JAZZ_QUERY = "select * from music where genre contains \"jazz\"";
    private static final String NO_HIT = "select * from music where genre contains \"nope\"";

    @Inject
    private ModelValidator modelValidator;

    @Test
    void fetch() throws Exception {
        var task = Query.builder().endpoint(Property.ofValue(endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).build();
        var runContext = runContextFactory.of();

        var output = task.run(runContext);

        var expected = rowsOf(search(Map.of("yql", CONTAINS_JAZZ_QUERY)));
        assertThat(output.getRows(), is(expected));
        assertThat(List.copyOf(output.getRows().getFirst().keySet()), is(List.copyOf(expected.getFirst().keySet())));
        assertThat(output.getSize(), is(6));
        assertThat(output.getTotalCount(), is(6L));
        assertThat(List.copyOf(output.getRows().getFirst().keySet()), is(List.of("id", "relevance", "fields")));
        assertThat((Map<?, ?>) output.getRows().getFirst().get("fields"), allOf(hasKey("album"), not(hasKey("sddocname")), not(hasKey("documentid"))));
        assertThat(runContext.metrics().stream().filter(m -> m.getName().equals("records")).findFirst().orElseThrow().getValue(), is(6.0));
    }

    @Test
    void fetchOne() throws Exception {
        var task = Query.builder().endpoint(Property.ofValue(endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).fetchType(Property.ofValue(FetchType.FETCH_ONE)).build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getRow(), is(rowsOf(search(Map.of("yql", CONTAINS_JAZZ_QUERY))).getFirst()));
        assertThat(output.getSize(), is(1));
        assertThat(output.getRows(), is(nullValue()));
        assertThat(output.getTotalCount(), is(6L));
    }

    @Test
    void fetchOneNoHit() throws Exception {
        var task = Query.builder().endpoint(Property.ofValue(endpoint())).yql(Property.ofValue(NO_HIT)).fetchType(Property.ofValue(FetchType.FETCH_ONE)).build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getRow(), is(nullValue()));
        assertThat(output.getSize(), is(0));
        assertThat(output.getTotalCount(), is(0L));
    }

    @Test
    void fetchNoHit() throws Exception {
        var task = Query.builder().endpoint(Property.ofValue(endpoint())).yql(Property.ofValue(NO_HIT)).build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getRows(), is(empty()));
        assertThat(output.getSize(), is(0));
        assertThat(output.getTotalCount(), is(0L));
    }

    @Test
    void none() throws Exception {
        var task = Query.builder().endpoint(Property.ofValue(endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).fetchType(Property.ofValue(FetchType.NONE)).build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getRows(), is(nullValue()));
        assertThat(output.getRow(), is(nullValue()));
        assertThat(output.getSize(), is(nullValue()));
        assertThat(output.getTotalCount(), is(6L));
    }

    @Test
    void hitsAndOffset() throws Exception {
        var yql = "select * from music where true order by year asc, added_at asc";
        var task = Query.builder()
            .endpoint(Property.ofValue(endpoint()))
            .yql(Property.ofValue(yql))
            .hits(Property.ofValue(5))
            .offset(Property.ofValue(5))
            .build();

        var output = task.run(runContextFactory.of());

        var expected = search(Map.of("yql", yql, "hits", 5, "offset", 5));
        assertThat(output.getRows(), is(rowsOf(expected)));
        assertThat(output.getSize(), is(5));
        assertThat(output.getTotalCount(), is(expected.at("/root/fields/totalCount").asLong()));
    }

    @Test
    void userQueryRankingAndParameters() throws Exception {
        var yql = "select * from music where userQuery() or ({targetHits:3}nearestNeighbor(embedding, q))";
        var task = Query.builder()
            .endpoint(Property.ofValue(endpoint()))
            .yql(Property.ofValue(yql))
            .query(Property.ofValue("blue"))
            .ranking(Property.ofValue("closeness"))
            .parameters(Property.ofValue(Map.of("input.query(q)", List.of(0, 0, 1, 0))))
            .build();

        var output = task.run(runContextFactory.of());

        var expected = rowsOf(search(Map.of("yql", yql, "query", "blue", "ranking", "closeness", "input.query(q)", List.of(0, 0, 1, 0))));
        assertThat(output.getRows(), is(expected));
        assertThat(((Map<?, ?>) output.getRows().getFirst().get("fields")).get("genre"), is("jazz"));
    }

    @Test
    void requestBody() throws Exception {
        try (var stub = VespaStub.answering("""
            {"root": {"fields": {"totalCount": 0}}}""")) {
            var task = Query.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .yql(Property.ofValue(CONTAINS_JAZZ_QUERY))
                .query(Property.ofValue("blue"))
                .hits(Property.ofValue(3))
                .offset(Property.ofValue(1))
                .ranking(Property.ofValue("closeness"))
                .queryTimeout(Property.ofValue(Duration.ofMillis(500)))
                .parameters(Property.ofValue(Map.of("presentation.format.tensors", "short-value")))
                .build();

            task.run(runContextFactory.of());

            var seen = stub.seen().getFirst();
            assertThat(seen.method(), is("POST"));
            assertThat(seen.path(), is("/search/"));
            assertThat(JacksonMapper.ofJson().readValue(seen.body(), new TypeReference<Map<String, Object>>() {}), is(Map.of(
                "yql", CONTAINS_JAZZ_QUERY,
                "query", "blue",
                "hits", 3,
                "offset", 1,
                "ranking.profile", "closeness",
                "timeout", "500ms",
                "presentation.format.tensors", "short-value"
            )));
        }
    }

    @Test
    void unsetPropertiesNotSent() throws Exception {
        try (var stub = VespaStub.answering("""
            {"root": {"fields": {"totalCount": 0}}}""")) {
            var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).build();

            task.run(runContextFactory.of());

            assertThat(JacksonMapper.ofJson().readValue(stub.seen().getFirst().body(), new TypeReference<Map<String, Object>>() {}), is(Map.of("yql", CONTAINS_JAZZ_QUERY)));
        }
    }

    @Test
    void parameterClashesWithProperty() throws Exception {
        try (var stub = VespaStub.start()) {
            var cases = Map.of(
                "yql", Query.builder().parameters(Property.ofValue(Map.of("yql", "select * from music where true"))),
                "query", Query.builder().query(Property.ofValue("blue")).parameters(Property.ofValue(Map.of("query", "red"))),
                "hits", Query.builder().hits(Property.ofValue(3)).parameters(Property.ofValue(Map.of("hits", 5))),
                "offset", Query.builder().offset(Property.ofValue(1)).parameters(Property.ofValue(Map.of("offset", 2))),
                "ranking", Query.builder().ranking(Property.ofValue("closeness")).parameters(Property.ofValue(Map.of("ranking", "default"))),
                "queryTimeout", Query.builder().queryTimeout(Property.ofValue(Duration.ofSeconds(1))).parameters(Property.ofValue(Map.of("timeout", "2s")))
            );
            for (var propertyAndBuilder : cases.entrySet()) {
                var task = propertyAndBuilder.getValue().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).build();

                var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()), propertyAndBuilder.getKey());

                assertThat(propertyAndBuilder.getKey(), e.getMessage(), containsString("task property '" + propertyAndBuilder.getKey() + "'"));
            }
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void parameterAliasClashesWithProperty() throws Exception {
        try (var stub = VespaStub.start()) {
            var cases = Map.of(
                "count", Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).hits(Property.ofValue(3)).parameters(Property.ofValue(Map.of("count", 1))).build(),
                "start", Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).offset(Property.ofValue(0)).parameters(Property.ofValue(Map.of("start", 5))).build(),
                "ranking.profile", Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).ranking(Property.ofValue("closeness")).parameters(Property.ofValue(Map.of("ranking.profile", "default"))).build(),
                "model.queryString", Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).query(Property.ofValue("blue")).parameters(Property.ofValue(Map.of("model.queryString", "red"))).build()
            );
            for (var aliasAndTask : cases.entrySet()) {
                var e = assertThrows(IllegalArgumentException.class, () -> aliasAndTask.getValue().run(runContextFactory.of()), aliasAndTask.getKey());

                assertThat(aliasAndTask.getKey(), e.getMessage(), containsString("'" + aliasAndTask.getKey() + "'"));
            }
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void parameterClashIgnoresCase() throws Exception {
        try (var stub = VespaStub.start()) {
            for (var key : List.of("HITS", "Count")) {
                var task = Query.builder()
                    .endpoint(Property.ofValue(stub.endpoint()))
                    .yql(Property.ofValue(CONTAINS_JAZZ_QUERY))
                    .hits(Property.ofValue(3))
                    .parameters(Property.ofValue(Map.of(key, 1)))
                    .build();

                var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()), key);

                assertThat(key, e.getMessage(), containsString("'" + key + "'"));
            }
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void parametersNeverClashWithEachOther() throws Exception {
        try (var stub = VespaStub.answering("""
            {"root": {"fields": {"totalCount": 0}}}""")) {
            var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).parameters(Property.ofValue(Map.of("hits", 3, "count", 1))).build();

            task.run(runContextFactory.of());

            assertThat(JacksonMapper.ofJson().readValue(stub.seen().getFirst().body(), new TypeReference<Map<String, Object>>() {}), is(Map.of("yql", CONTAINS_JAZZ_QUERY, "hits", 3, "count", 1)));
        }
    }

    @Test
    void aliasWithItsPropertyUnsetIsSent() throws Exception {
        try (var stub = VespaStub.answering("""
            {"root": {"fields": {"totalCount": 0}}}""")) {
            var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).parameters(Property.ofValue(Map.of("count", 1))).build();

            task.run(runContextFactory.of());

            assertThat(JacksonMapper.ofJson().readValue(stub.seen().getFirst().body(), new TypeReference<Map<String, Object>>() {}), is(Map.of("yql", CONTAINS_JAZZ_QUERY, "count", 1)));
        }
    }

    @Test
    void invalidYql() {
        for (var fetchType : List.of(FetchType.FETCH, FetchType.STORE)) {
            var task = Query.builder().endpoint(Property.ofValue(endpoint())).yql(Property.ofValue("select * frm music")).fetchType(Property.ofValue(fetchType)).build();

            var e = assertThrows(HttpClientResponseException.class, () -> task.run(runContextFactory.of()), fetchType.name());

            assertThat(fetchType.name(), e.getMessage(), allOf(containsString("400"), containsString("mismatched input 'frm'")));
        }
    }

    @Test
    void emptyAnswer() throws Exception {
        for (var answer : List.of("", " ")) {
            try (var stub = VespaStub.answering(answer)) {
                var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).build();

                var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()), "'" + answer + "'");

                assertThat(e.getMessage(), allOf(containsString("empty answer"), containsString("endpoint")));
            }
        }
    }

    @Test
    void answerWithoutRoot() throws Exception {
        for (var answer : List.of("{}", """
            {"root": null}""", """
            {"root": 5}""")) {
            try (var stub = VespaStub.answering(answer)) {
                var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).build();

                var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()), answer);

                assertThat(answer, e.getMessage(), allOf(containsString("no root"), containsString("endpoint")));
            }
        }
    }

    @Test
    void answerNotJson() throws Exception {
        try (var stub = VespaStub.answering("<html>Bad gateway</html>")) {
            var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).build();

            var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), allOf(containsString("isn't JSON"), containsString("<html>Bad gateway</html>"), containsString("endpoint")));
        }
    }

    @Test
    void longAnswerCutInMessage() throws Exception {
        var notJson = "<html>" + "x".repeat(5000) + "END</html>";
        var noRoot = "{\"other\": \"" + "x".repeat(5000) + "END\"}";
        for (var answer : List.of(notJson, noRoot)) {
            try (var stub = VespaStub.answering(answer)) {
                var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).build();

                var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

                assertThat(e.getMessage(), allOf(containsString(answer.substring(0, 20)), not(containsString("END"))));
                assertThat(e.getMessage().length(), lessThan(1300));
            }
        }
    }

    @Test
    void hitWithoutFields() throws Exception {
        try (var stub = VespaStub.answering("""
            {"root": {
              "fields": {"totalCount": 30},
              "children": [
                {"id": "group:root:0", "relevance": 1.0, "continuation": {"this": ""}, "children": []},
                {"id": "id:mynamespace:music::kind-of-blue", "relevance": 0.5, "fields": null}
              ]
            }}""")) {
            var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).build();

            var output = task.run(runContextFactory.of());

            assertThat(output.getRows(), is(List.of(
                Map.of("id", "group:root:0", "relevance", 1.0),
                Map.of("id", "id:mynamespace:music::kind-of-blue", "relevance", 0.5)
            )));
        }
    }

    @Test
    void partialAnswerWarns() throws Exception {
        try (var stub = VespaStub.answering("""
            {"root": {
              "fields": {"totalCount": 1},
              "coverage": {"coverage": 50, "degraded": {"timeout": true}},
              "errors": [{"code": 12, "summary": "Timed out", "message": "Missing hits from 1 of 2 nodes"}],
              "children": [{"id": "id:mynamespace:music::kind-of-blue", "relevance": 0.5, "fields": {"album": "Kind of Blue"}}]
            }}""")) {
            var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).build();
            var runContext = runContextFactory.of();
            var logs = capture(runContext);

            var output = task.run(runContext);

            assertThat(output.getRows(), is(List.of(Map.of("id", "id:mynamespace:music::kind-of-blue", "relevance", 0.5, "fields", Map.of("album", "Kind of Blue")))));
            assertThat(warnings(logs), hasItem(allOf(containsString("Timed out"), containsString("Missing hits from 1 of 2 nodes"))));
        }
    }

    @Test
    void documentIdAndRelevanceStayInFields() throws Exception {
        try (var stub = VespaStub.answering("""
            {"root": {
              "fields": {"totalCount": 1},
              "children": [{"id": "id:mynamespace:music::kind-of-blue", "relevance": 0.5, "fields": {"id": "mine", "relevance": "high", "album": "Kind of Blue"}}]
            }}""")) {
            var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).build();
            var runContext = runContextFactory.of();
            var logs = capture(runContext);

            var output = task.run(runContext);

            assertThat(output.getRows(), is(List.of(Map.of(
                "id", "id:mynamespace:music::kind-of-blue",
                "relevance", 0.5,
                "fields", Map.of("id", "mine", "relevance", "high", "album", "Kind of Blue")
            ))));
            assertThat(warnings(logs), is(empty()));
        }
    }

    @Test
    void store() throws Exception {
        var task = Query.builder().endpoint(Property.ofValue(endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).fetchType(Property.ofValue(FetchType.STORE)).build();
        var runContext = runContextFactory.of();

        var output = task.run(runContext);

        assertThat(output.getUri().getScheme(), is("kestra"));
        assertThat(stored(runContext, output), is(rowsOf(search(Map.of("yql", CONTAINS_JAZZ_QUERY)))));
        assertThat(output.getSize(), is(6));
        assertThat(output.getTotalCount(), is(6L));
        assertThat(output.getRows(), is(nullValue()));
        assertThat(output.getRow(), is(nullValue()));
    }

    @Test
    void storeNoHit() throws Exception {
        var task = Query.builder().endpoint(Property.ofValue(endpoint())).yql(Property.ofValue(NO_HIT)).fetchType(Property.ofValue(FetchType.STORE)).build();
        var runContext = runContextFactory.of();

        var output = task.run(runContext);

        assertThat(output.getUri(), is(notNullValue()));
        assertThat(stored(runContext, output), is(empty()));
        assertThat(output.getSize(), is(0));
        assertThat(output.getTotalCount(), is(0L));
        assertThat(runContext.metrics().stream().filter(m -> m.getName().equals("records")).findFirst().orElseThrow().getValue(), is(0.0));
    }

    @Test
    void storeDefaultHits() throws Exception {
        var yql = "select * from music where true";
        var task = Query.builder().endpoint(Property.ofValue(endpoint())).yql(Property.ofValue(yql)).fetchType(Property.ofValue(FetchType.STORE)).build();
        var runContext = runContextFactory.of();

        var output = task.run(runContext);

        assertThat(stored(runContext, output), hasSize(10));
        assertThat(output.getSize(), is(10));
        assertThat(output.getTotalCount(), is(search(Map.of("yql", yql)).at("/root/fields/totalCount").asLong()));
    }

    @Test
    void storeHitsAndOffset() throws Exception {
        var yql = "select * from music where true order by year asc, added_at asc";
        var task = Query.builder()
            .endpoint(Property.ofValue(endpoint()))
            .yql(Property.ofValue(yql))
            .hits(Property.ofValue(3))
            .offset(Property.ofValue(3))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();
        var runContext = runContextFactory.of();

        var output = task.run(runContext);

        assertThat(stored(runContext, output), is(rowsOf(search(Map.of("yql", yql, "hits", 3, "offset", 3)))));
        assertThat(output.getSize(), is(3));
    }

    @Test
    void storeOneRequest() throws Exception {
        try (var stub = VespaStub.answering("""
            {"root": {
              "fields": {"totalCount": 1000000},
              "children": [
                {"id": "id:mynamespace:music::kind-of-blue", "relevance": 0.9, "fields": {"album": "Kind of Blue"}},
                {"id": "id:mynamespace:music::time-out", "relevance": 0.8, "fields": {"album": "Time Out"}}
              ]
            }}""")) {
            var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).fetchType(Property.ofValue(FetchType.STORE)).build();
            var runContext = runContextFactory.of();

            var output = task.run(runContext);

            assertThat(stub.seen(), hasSize(1));
            assertThat(stored(runContext, output), is(List.of(
                Map.of("id", "id:mynamespace:music::kind-of-blue", "relevance", 0.9, "fields", Map.of("album", "Kind of Blue")),
                Map.of("id", "id:mynamespace:music::time-out", "relevance", 0.8, "fields", Map.of("album", "Time Out"))
            )));
            assertThat(output.getSize(), is(2));
            assertThat(output.getTotalCount(), is(1000000L));
        }
    }

    @Test
    void storeFailsOnBadAnswers() throws Exception {
        var messages = Map.of("", "empty answer", "<html>Bad gateway</html>", "isn't JSON", "{}", "no root");
        for (var answerAndMessage : messages.entrySet()) {
            try (var stub = VespaStub.answering(answerAndMessage.getKey())) {
                var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).fetchType(Property.ofValue(FetchType.STORE)).build();

                var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()), "'" + answerAndMessage.getKey() + "'");

                assertThat(answerAndMessage.getKey(), e.getMessage(), allOf(containsString(answerAndMessage.getValue()), containsString("endpoint")));
            }
        }
    }

    @Test
    void nestedParameterClashesWithProperty() throws Exception {
        try (var stub = VespaStub.start()) {
            var cases = Map.of(
                "model.queryString", Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).query(Property.ofValue("blue")).parameters(Property.ofValue(Map.of("model", Map.of("queryString", "red")))).build(),
                "ranking.profile", Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).ranking(Property.ofValue("closeness")).parameters(Property.ofValue(Map.of("ranking", Map.of("profile", "default")))).build()
            );
            for (var nameAndTask : cases.entrySet()) {
                var e = assertThrows(IllegalArgumentException.class, () -> nameAndTask.getValue().run(runContextFactory.of()), nameAndTask.getKey());

                assertThat(nameAndTask.getKey(), e.getMessage(), containsString("'" + nameAndTask.getKey() + "'"));
            }
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void nestedRankingBesideProfile() throws Exception {
        var yql = "select * from music where {targetHits:3}nearestNeighbor(embedding, q)";
        var task = Query.builder()
            .endpoint(Property.ofValue(endpoint()))
            .yql(Property.ofValue(yql))
            .ranking(Property.ofValue("closeness"))
            .parameters(Property.ofValue(Map.of("input.query(q)", List.of(0, 0, 1, 0), "ranking", Map.of("matchPhase", Map.of("maxHits", 100)))))
            .build();

        var output = task.run(runContextFactory.of());

        var expected = rowsOf(search(Map.of("yql", yql, "ranking.profile", "closeness", "input.query(q)", List.of(0, 0, 1, 0), "ranking", Map.of("matchPhase", Map.of("maxHits", 100)))));
        assertThat(expected, is(not(empty())));
        assertThat(output.getRows(), is(expected));
    }

    @Test
    void hitsForFetchType() throws Exception {
        try (var stub = VespaStub.answering("""
            {"root": {"fields": {"totalCount": 0}}}""")) {
            var cases = List.of(
                Map.entry(Query.builder().fetchType(Property.ofValue(FetchType.NONE)), Map.<String, Object>of("hits", 0)),
                Map.entry(Query.builder().fetchType(Property.ofValue(FetchType.FETCH_ONE)), Map.<String, Object>of("hits", 1)),
                Map.entry(Query.builder().fetchType(Property.ofValue(FetchType.FETCH)), Map.<String, Object>of()),
                Map.entry(Query.builder().fetchType(Property.ofValue(FetchType.STORE)), Map.<String, Object>of()),
                Map.entry(Query.builder().fetchType(Property.ofValue(FetchType.NONE)).hits(Property.ofValue(3)), Map.<String, Object>of("hits", 3)),
                Map.entry(Query.builder().fetchType(Property.ofValue(FetchType.FETCH_ONE)).parameters(Property.ofValue(Map.of("count", 2))), Map.<String, Object>of("count", 2)),
                Map.entry(Query.builder().fetchType(Property.ofValue(FetchType.STORE)).parameters(Property.ofValue(Map.of("count", 2))), Map.<String, Object>of("count", 2))
            );
            for (var builderAndSent : cases) {
                builderAndSent.getKey().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).build().run(runContextFactory.of());

                var body = JacksonMapper.ofJson().readValue(stub.seen().getLast().body(), new TypeReference<Map<String, Object>>() {});
                body.remove("yql");
                assertThat(body, is(builderAndSent.getValue()));
            }
        }
    }

    @Test
    void recordsCountsOutputRows() throws Exception {
        var expected = Map.of(FetchType.FETCH, 5.0, FetchType.FETCH_ONE, 1.0, FetchType.NONE, 0.0, FetchType.STORE, 5.0);
        for (var fetchTypeAndRecords : expected.entrySet()) {
            var task = Query.builder().endpoint(Property.ofValue(endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).hits(Property.ofValue(5)).fetchType(Property.ofValue(fetchTypeAndRecords.getKey())).build();
            var runContext = runContextFactory.of();

            task.run(runContext);

            var records = runContext.metrics().stream().filter(m -> m.getName().equals("records")).findFirst().orElseThrow().getValue();
            assertThat(fetchTypeAndRecords.getKey().name(), records, is(fetchTypeAndRecords.getValue()));
        }
    }

    @Test
    void negativeHitsAndOffsetRefused() {
        var cases = Map.of(
            "hits", Query.builder().id("query").type(Query.class.getName()).endpoint(Property.ofValue(endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).hits(Property.ofValue(-1)).build(),
            "offset", Query.builder().id("query").type(Query.class.getName()).endpoint(Property.ofValue(endpoint())).yql(Property.ofValue(CONTAINS_JAZZ_QUERY)).offset(Property.ofValue(-1)).build()
        );
        for (var nameAndTask : cases.entrySet()) {
            var violations = modelValidator.isValid(nameAndTask.getValue());

            assertThat(nameAndTask.getKey(), violations.isPresent(), is(true));
            assertThat(nameAndTask.getKey(), violations.get().getMessage(), containsString(nameAndTask.getKey()));
        }
    }

    @Test
    void yqlRequired() {
        var task = Query.builder().id("query").type(Query.class.getName()).endpoint(Property.ofValue(endpoint())).build();

        var violations = modelValidator.isValid(task);

        assertThat(violations.isPresent(), is(true));
        assertThat(violations.get().getMessage(), containsString("yql"));
    }

    // Written out here rather than reusing Query's code, so a wrong row rule can't pass on both sides.
    private static List<Map<String, Object>> rowsOf(JsonNode answer) {
        var rows = new ArrayList<Map<String, Object>>();
        for (var hit : answer.at("/root/children")) {
            var row = new LinkedHashMap<String, Object>();
            row.put("id", hit.get("id").asText());
            row.put("relevance", hit.get("relevance").asDouble());
            var fields = JacksonMapper.ofJson().convertValue(hit.get("fields"), new TypeReference<LinkedHashMap<String, Object>>() {});
            fields.remove("sddocname");
            fields.remove("documentid");
            row.put("fields", fields);
            rows.add(row);
        }
        return rows;
    }

    private static List<Map<String, Object>> stored(RunContext runContext, Query.Output output) throws IOException {
        try (var file = runContext.storage().getFile(output.getUri())) {
            return FileSerde.readAll(file, new TypeReference<Map<String, Object>>() {}).collectList().block();
        }
    }

    private static ListAppender<ILoggingEvent> capture(RunContext runContext) {
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        ((Logger) runContext.logger()).addAppender(appender);
        return appender;
    }

    private static List<String> warnings(ListAppender<ILoggingEvent> logs) {
        return logs.list.stream().filter(e -> e.getLevel().levelStr.equals("WARN")).map(ILoggingEvent::getFormattedMessage).toList();
    }
}
