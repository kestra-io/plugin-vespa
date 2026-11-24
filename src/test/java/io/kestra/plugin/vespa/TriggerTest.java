package io.kestra.plugin.vespa;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TriggerTest extends VespaContainer {
    private static final String PATH = "/document/v1/mynamespace/music/docid/";
    private static final String NAMESPACE = "company.vespa";

    private final List<String> fed = new ArrayList<>();

    @AfterEach
    void removeFedDocuments() {
        fed.forEach(key -> delete(PATH + key));
        fed.clear();
    }

    @Test
    void firstPollDeliversMatchesInOrder() throws Exception {
        var genre = genre();
        feed("c", genre, 30);
        feed("a", genre, 10);
        feed("b", genre, 20);
        var trigger = trigger(genre).build();

        var execution = evaluate(trigger, "flow").orElseThrow();

        assertThat(keys(execution), contains(key("a"), key("b"), key("c")));
        assertThat(execution.getTrigger().getVariables().get("size"), is(3));
        var first = rows(execution).getFirst();
        assertThat(List.copyOf(first.keySet()), is(List.of("id", "relevance", "fields")));
        assertThat(((Map<?, ?>) first.get("fields")).get("added_at"), is(10));
    }

    @Test
    void secondPollFiresNothing() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        feed("b", genre, 20);
        var trigger = trigger(genre).build();
        assertThat(poll(trigger), hasSize(2));

        assertThat(evaluate(trigger, "flow"), is(Optional.empty()));
    }

    @Test
    void onlyNewMatchingDocumentFires() throws Exception {
        var genre = genre();
        var other = genre();
        feed("a", genre, 10);
        feed("b", other, 20);
        var trigger = trigger(genre)
            .yql(Property.ofValue("select * from music where genre contains \"%s\" or genre contains \"%s\"".formatted(genre, other)))
            .build();
        assertThat(poll(trigger), contains(key("a"), key("b")));

        feed("c", other, 40);
        feed("d", genre(), 50);

        assertThat(poll(trigger), contains(key("c")));
    }

    @Test
    void sameValueDeliversOnlyTheNewDocument() throws Exception {
        var genre = genre();
        feed("a", genre, 20);
        feed("b", genre, 20);
        var trigger = trigger(genre).build();
        assertThat(poll(trigger), containsInAnyOrder(key("a"), key("b")));

        feed("c", genre, 20);

        assertThat(poll(trigger), contains(key("c")));
        assertThat(poll(trigger), empty());
    }

    @Test
    void tieSplitAcrossTwoPollsDeliversTheRest() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        feed("b", genre, 20);
        var trigger = trigger(genre).hits(Property.ofValue(2)).build();
        assertThat(poll(trigger), contains(key("a"), key("b")));

        feed("c", genre, 20);

        assertThat(poll(trigger), contains(key("c")));
    }

    @Test
    void backlogDrainsOnePagePerPoll() throws Exception {
        var genre = genre();
        for (var i = 1; i <= 5; i++) {
            feed("d" + i, genre, i * 10L);
        }
        var trigger = trigger(genre).hits(Property.ofValue(2)).build();

        assertThat(poll(trigger), contains(key("d1"), key("d2")));
        assertThat(poll(trigger), contains(key("d3"), key("d4")));
        assertThat(poll(trigger), contains(key("d5")));
        assertThat(poll(trigger), empty());
    }

    @Test
    void tooManyAtOneValueFails() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        feed("b", genre, 10);
        feed("c", genre, 10);
        var trigger = trigger(genre).hits(Property.ofValue(2)).build();

        var error = assertThrows(IllegalStateException.class, () -> evaluate(trigger, "flow"));

        assertThat(error.getMessage(), allOf(containsString("added_at"), containsString("10"), containsString("hits")));
        assertThat(stored(trigger, "flow"), is(Optional.empty()));
    }

    @Test
    void fullBoundaryFailsBeforeRequest() throws Exception {
        var trigger = trigger(genre()).hits(Property.ofValue(2)).endpoint(Property.ofValue("http://localhost:1")).build();
        store(trigger, "flow", "{\"field\":\"added_at\",\"value\":10,\"ids\":[\"id:mynamespace:music::x\",\"id:mynamespace:music::y\"]}");

        var error = assertThrows(IllegalStateException.class, () -> evaluate(trigger, "flow"));

        assertThat(error.getMessage(), allOf(containsString("added_at"), containsString("10"), containsString("hits")));
    }

    @Test
    void unreadableWatermarkFails() throws Exception {
        var trigger = trigger(genre()).build();
        store(trigger, "flow", "not a watermark");

        var error = assertThrows(IllegalStateException.class, () -> evaluate(trigger, "flow"));

        assertThat(error.getMessage(), allOf(containsString(Trigger.watermarkKey("flow", trigger.getId())), containsString("Delete")));
    }

    @Test
    void watermarkNotSavedWhenExecutionCantBeGenerated() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        var trigger = trigger(genre).build();
        var context = context(trigger, "flow");
        var withoutFlow = ConditionContext.builder().runContext(context.getKey().getRunContext()).build();

        assertThrows(NullPointerException.class, () -> trigger.evaluate(withoutFlow, context.getValue()));

        assertThat(stored(trigger, "flow"), is(Optional.empty()));
        assertThat(poll(trigger), contains(key("a")));
    }

    @Test
    void rejectsOrderByOffsetAndOwnParameters() {
        var cases = new HashMap<String, Trigger>();
        cases.put("order by", trigger(genre()).yql(Property.ofValue("select * from music where true ORDER  BY year")).build());
        cases.put("offset", trigger(genre()).yql(Property.ofValue("select * from music where true limit 5 offset 2")).build());
        for (var parameter : List.of("recall", "Sorting", "ranking.sorting", "hits", "count", "offset", "start")) {
            cases.put(parameter, trigger(genre()).parameters(Property.ofValue(Map.of(parameter, "x"))).build());
        }
        cases.put("ranking.sorting", trigger(genre()).parameters(Property.ofValue(Map.of("ranking", Map.of("sorting", "x")))).build());

        cases.forEach((name, trigger) -> {
            var error = assertThrows(IllegalArgumentException.class, () -> evaluate(trigger, "flow"), name);
            assertThat(name, error.getMessage(), containsStringIgnoringCase(name.replace("  ", " ")));
        });
    }

    @Test
    void hitWithoutDocumentIdFails() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        var trigger = trigger(genre)
            .yql(Property.ofValue("select album, added_at from music where genre contains \"%s\"".formatted(genre)))
            .build();

        var error = assertThrows(IllegalStateException.class, () -> evaluate(trigger, "flow"));

        assertThat(error.getMessage(), allOf(containsString("document ID"), containsString("index:")));
    }

    @Test
    void hitWithoutWatermarkValueFails() throws Exception {
        var genre = genre();
        put(PATH + "nodate", Map.of("album", "No date", "genre", genre));
        fed.add("nodate");
        var missing = trigger(genre).build();
        var notANumber = trigger(genre).watermarkField(Property.ofValue("genre")).build();

        var missingError = assertThrows(IllegalStateException.class, () -> evaluate(missing, "flow"));
        var notANumberError = assertThrows(IllegalStateException.class, () -> evaluate(notANumber, "flow"));

        assertThat(missingError.getMessage(), allOf(containsString("added_at"), containsString(key("nodate"))));
        assertThat(notANumberError.getMessage(), allOf(containsString("genre"), containsString(key("nodate"))));
    }

    @Test
    void storeWritesTheRows() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        feed("b", genre, 20);
        var trigger = trigger(genre).fetchType(Property.ofValue(FetchType.STORE)).build();
        var context = context(trigger, "flow");

        var execution = trigger.evaluate(context.getKey(), context.getValue()).orElseThrow();

        var variables = execution.getTrigger().getVariables();
        assertThat(variables.get("size"), is(2));
        assertThat(variables.get("rows"), is(nullValue()));
        var uri = URI.create((String) variables.get("uri"));
        try (var file = context.getKey().getRunContext().storage().getFile(uri)) {
            var stored = FileSerde.readAll(file, new TypeReference<Map<String, Object>>() {}).collectList().block();
            assertThat(stored.stream().map(row -> row.get("id")).toList(), contains(key("a"), key("b")));
        }
    }

    @Test
    void fetchOneAndNoneFail() {
        for (var fetchType : List.of(FetchType.FETCH_ONE, FetchType.NONE)) {
            var trigger = trigger(genre()).fetchType(Property.ofValue(fetchType)).build();

            var error = assertThrows(IllegalArgumentException.class, () -> evaluate(trigger, "flow"), fetchType.name());

            assertThat(fetchType.name(), error.getMessage(), allOf(containsString(fetchType.name()), containsString("FETCH"), containsString("STORE")));
        }
    }

    @Test
    void hitsDefaultsTo100() throws Exception {
        var genre = genre();
        for (var i = 1; i <= 101; i++) {
            feed("h" + i, genre, i);
        }
        var trigger = trigger(genre).build();

        assertThat(poll(trigger), hasSize(100));
    }

    @Test
    void eachTriggerKeepsItsOwnWatermark() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        feed("b", genre, 20);
        // the KV store outlives the test run, so the flows get a fresh prefix; "<prefix>ab" + "c" and "<prefix>a" + "bc" still read alike
        var prefix = "f" + IdUtils.create().toLowerCase();
        var first = trigger(genre).id("c").build();
        var second = trigger(genre).id("bc").build();

        assertThat(keys(evaluate(first, prefix + "ab").orElseThrow()), contains(key("a"), key("b")));
        assertThat(keys(evaluate(second, prefix + "a").orElseThrow()), contains(key("a"), key("b")));
    }

    @Test
    void changingTheFieldStartsAgain() throws Exception {
        var genre = genre();
        put(PATH + "a", Map.of("album", "A", "genre", genre, "added_at", 10, "year", 2001));
        put(PATH + "b", Map.of("album", "B", "genre", genre, "added_at", 20, "year", 2000));
        fed.addAll(List.of("a", "b"));
        var id = "trigger_" + IdUtils.create();
        assertThat(poll(trigger(genre).id(id).build()), contains(key("a"), key("b")));

        var byYear = trigger(genre).id(id).watermarkField(Property.ofValue("year")).build();

        assertThat(poll(byYear), contains(key("b"), key("a")));
        assertThat(poll(byYear), empty());
    }

    @Test
    void vespaErrorFailsAndKeepsWatermark() throws Exception {
        try (var stub = VespaStub.answering(500, "{\"root\":{}}")) {
            var trigger = trigger(genre()).endpoint(Property.ofValue(stub.endpoint())).build();
            var watermark = "{\"field\":\"added_at\",\"value\":10,\"ids\":[\"id:mynamespace:music::x\"]}";
            store(trigger, "flow", watermark);

            var error = assertThrows(Exception.class, () -> evaluate(trigger, "flow"));

            assertThat(error.getMessage(), containsString("500"));
            assertThat(stored(trigger, "flow"), is(Optional.of(watermark)));
        }
    }

    @Test
    void tokenNotInToString() {
        var trigger = trigger(genre()).token(Property.ofValue("secret-token-value")).build();

        assertThat(trigger.toString(), not(containsString("secret-token-value")));
    }

    // a genre no other document has, so each test sees only what it feeds
    private static String genre() {
        return "t" + IdUtils.create().toLowerCase();
    }

    private void feed(String key, String genre, long addedAt) throws Exception {
        put(PATH + key, Map.of("album", key, "genre", genre, "added_at", addedAt));
        fed.add(key);
    }

    private static String key(String key) {
        return "id:mynamespace:music::" + key;
    }

    private Trigger.TriggerBuilder<?, ?> trigger(String genre) {
        return Trigger.builder()
            .id("trigger_" + IdUtils.create())
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue(endpoint()))
            .yql(Property.ofValue("select * from music where genre contains \"%s\"".formatted(genre)))
            .watermarkField(Property.ofValue("added_at"));
    }

    // TestsUtils.mockTrigger names the flow after its caller; a test that compares flows needs to name them
    private Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context(Trigger trigger, String flowId) {
        var flow = Flow.builder().namespace(NAMESPACE).id(flowId).tenantId(MAIN_TENANT).revision(1).build();
        var triggerContext = io.kestra.core.models.triggers.Trigger.builder()
            .triggerId(trigger.getId())
            .flowId(flowId)
            .tenantId(MAIN_TENANT)
            .namespace(NAMESPACE)
            .date(ZonedDateTime.now())
            .build();
        var runContext = runContextFactory.initializer().forScheduler((DefaultRunContext) runContextFactory.of(flow, trigger), triggerContext, trigger);
        return Map.entry(ConditionContext.builder().runContext(runContext).flow(flow).build(), triggerContext);
    }

    private Optional<Execution> evaluate(Trigger trigger, String flowId) throws Exception {
        var context = context(trigger, flowId);
        return trigger.evaluate(context.getKey(), context.getValue());
    }

    private List<String> poll(Trigger trigger) throws Exception {
        return evaluate(trigger, "flow").map(TriggerTest::keys).orElse(List.of());
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> rows(Execution execution) {
        return (List<Map<String, Object>>) execution.getTrigger().getVariables().get("rows");
    }

    private static List<String> keys(Execution execution) {
        return rows(execution).stream().map(row -> (String) row.get("id")).toList();
    }

    private void store(Trigger trigger, String flowId, String value) throws Exception {
        context(trigger, flowId).getKey().getRunContext().namespaceKv(NAMESPACE)
            .put(Trigger.watermarkKey(flowId, trigger.getId()), new KVValueAndMetadata(null, value));
    }

    private Optional<String> stored(Trigger trigger, String flowId) throws Exception {
        var value = context(trigger, flowId).getKey().getRunContext().namespaceKv(NAMESPACE).getValue(Trigger.watermarkKey(flowId, trigger.getId()));
        return value.map(v -> (String) v.value());
    }
}
