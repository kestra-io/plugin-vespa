package io.kestra.plugin.vespa;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryTest extends VespaContainer {
    private static final String JAZZ = "select * from music where genre contains \"jazz\"";
    private static final String NO_HIT = "select * from music where genre contains \"nope\"";

    @Inject
    private ModelValidator modelValidator;

    @Test
    void fetch() throws Exception {
        var task = Query.builder().endpoint(Property.ofValue(endpoint)).yql(Property.ofValue(JAZZ)).build();
        var runContext = runContextFactory.of();

        var output = task.run(runContext);

        assertThat(output.getRows(), is(rowsOf(search(Map.of("yql", JAZZ)))));
        assertThat(output.getSize(), is(6));
        assertThat(output.getTotalCount(), is(6L));
        assertThat(output.getRows().getFirst(), allOf(hasKey("id"), hasKey("relevance"), hasKey("album"), not(hasKey("sddocname")), not(hasKey("documentid")), not(hasKey("source")), not(hasKey("fields"))));
        assertThat(runContext.metrics().stream().filter(m -> m.getName().equals("records")).findFirst().orElseThrow().getValue(), is(6.0));
    }

    @Test
    void fetchOne() throws Exception {
        var task = Query.builder().endpoint(Property.ofValue(endpoint)).yql(Property.ofValue(JAZZ)).fetchType(Property.ofValue(FetchType.FETCH_ONE)).build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getRow(), is(rowsOf(search(Map.of("yql", JAZZ))).getFirst()));
        assertThat(output.getSize(), is(1));
        assertThat(output.getRows(), is(nullValue()));
        assertThat(output.getTotalCount(), is(6L));
    }

    @Test
    void fetchOneNoHit() throws Exception {
        var task = Query.builder().endpoint(Property.ofValue(endpoint)).yql(Property.ofValue(NO_HIT)).fetchType(Property.ofValue(FetchType.FETCH_ONE)).build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getRow(), is(nullValue()));
        assertThat(output.getSize(), is(0));
        assertThat(output.getTotalCount(), is(0L));
    }

    @Test
    void fetchNoHit() throws Exception {
        var task = Query.builder().endpoint(Property.ofValue(endpoint)).yql(Property.ofValue(NO_HIT)).build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getRows(), is(empty()));
        assertThat(output.getSize(), is(0));
        assertThat(output.getTotalCount(), is(0L));
    }

    @Test
    void none() throws Exception {
        var task = Query.builder().endpoint(Property.ofValue(endpoint)).yql(Property.ofValue(JAZZ)).fetchType(Property.ofValue(FetchType.NONE)).build();

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
            .endpoint(Property.ofValue(endpoint))
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
            .endpoint(Property.ofValue(endpoint))
            .yql(Property.ofValue(yql))
            .query(Property.ofValue("blue"))
            .ranking(Property.ofValue("closeness"))
            .parameters(Property.ofValue(Map.of("input.query(q)", List.of(0, 0, 1, 0))))
            .build();

        var output = task.run(runContextFactory.of());

        var expected = rowsOf(search(Map.of("yql", yql, "query", "blue", "ranking", "closeness", "input.query(q)", List.of(0, 0, 1, 0))));
        assertThat(output.getRows(), is(expected));
        assertThat(output.getRows().getFirst().get("genre"), is("jazz"));
    }

    @Test
    void requestBody() throws Exception {
        try (var stub = VespaStub.answering("""
            {"root": {"fields": {"totalCount": 0}}}""")) {
            var task = Query.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .yql(Property.ofValue(JAZZ))
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
                "yql", JAZZ,
                "query", "blue",
                "hits", 3,
                "offset", 1,
                "ranking", "closeness",
                "timeout", "500ms",
                "presentation.format.tensors", "short-value"
            )));
        }
    }

    @Test
    void unsetPropertiesNotSent() throws Exception {
        try (var stub = VespaStub.answering("""
            {"root": {"fields": {"totalCount": 0}}}""")) {
            var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(JAZZ)).build();

            task.run(runContextFactory.of());

            assertThat(JacksonMapper.ofJson().readValue(stub.seen().getFirst().body(), new TypeReference<Map<String, Object>>() {}), is(Map.of("yql", JAZZ)));
        }
    }

    @Test
    void parameterClashesWithProperty() throws Exception {
        try (var stub = VespaStub.start()) {
            var task = Query.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .yql(Property.ofValue(JAZZ))
                .hits(Property.ofValue(3))
                .parameters(Property.ofValue(Map.of("hits", 5)))
                .build();

            var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), containsString("'hits'"));
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void invalidYql() {
        var task = Query.builder().endpoint(Property.ofValue(endpoint)).yql(Property.ofValue("select * frm music")).build();

        var e = assertThrows(Exception.class, () -> task.run(runContextFactory.of()));

        assertThat(e.getMessage(), allOf(containsString("400"), containsString("mismatched input 'frm'")));
    }

    @Test
    void emptyAnswer() throws Exception {
        try (var stub = VespaStub.answering("")) {
            var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(JAZZ)).build();

            var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), containsString("empty answer"));
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
            var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(JAZZ)).build();
            var runContext = runContextFactory.of();
            var logs = capture(runContext);

            var output = task.run(runContext);

            assertThat(output.getRows(), is(List.of(Map.of("id", "id:mynamespace:music::kind-of-blue", "relevance", 0.5, "album", "Kind of Blue"))));
            assertThat(warnings(logs), hasItem(containsString("Missing hits from 1 of 2 nodes")));
        }
    }

    @Test
    void vespaIdAndRelevanceWin() throws Exception {
        try (var stub = VespaStub.answering("""
            {"root": {
              "fields": {"totalCount": 1},
              "children": [{"id": "id:mynamespace:music::kind-of-blue", "relevance": 0.5, "fields": {"id": "mine", "relevance": "high", "album": "Kind of Blue"}}]
            }}""")) {
            var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(JAZZ)).build();
            var runContext = runContextFactory.of();
            var logs = capture(runContext);

            var output = task.run(runContext);

            assertThat(output.getRows(), is(List.of(Map.of("id", "id:mynamespace:music::kind-of-blue", "relevance", 0.5, "album", "Kind of Blue"))));
            assertThat(warnings(logs), hasItem(allOf(containsString("id"), containsString("relevance"))));
        }
    }

    @Test
    void storeNotSupported() throws Exception {
        try (var stub = VespaStub.start()) {
            var task = Query.builder().endpoint(Property.ofValue(stub.endpoint())).yql(Property.ofValue(JAZZ)).fetchType(Property.ofValue(FetchType.STORE)).build();

            var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), allOf(containsString("STORE"), containsString("FETCH"), containsString("FETCH_ONE"), containsString("NONE")));
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void yqlRequired() {
        var task = Query.builder().id("query").type(Query.class.getName()).endpoint(Property.ofValue(endpoint)).build();

        var violations = modelValidator.isValid(task);

        assertThat(violations.isPresent(), is(true));
        assertThat(violations.get().getMessage(), containsString("yql"));
    }

    // Vespa's answer flattened by hand: the hit's id and relevance, then its fields without Vespa's own sddocname and documentid.
    private static List<Map<String, Object>> rowsOf(JsonNode answer) {
        var rows = new ArrayList<Map<String, Object>>();
        for (var hit : answer.at("/root/children")) {
            var row = new LinkedHashMap<String, Object>();
            row.put("id", hit.get("id").asText());
            row.put("relevance", hit.get("relevance").asDouble());
            row.putAll(JacksonMapper.ofJson().convertValue(hit.get("fields"), new TypeReference<Map<String, Object>>() {}));
            row.remove("sddocname");
            row.remove("documentid");
            rows.add(row);
        }
        return rows;
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
