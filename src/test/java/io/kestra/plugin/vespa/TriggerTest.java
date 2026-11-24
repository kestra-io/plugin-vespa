package io.kestra.plugin.vespa;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

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
    void tieLargerThanAPageDrainsOverPolls() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        feed("b", genre, 10);
        feed("c", genre, 10);
        var trigger = trigger(genre).hits(Property.ofValue(2)).build();

        var first = poll(trigger);
        var second = poll(trigger);

        assertThat(first, hasSize(2));
        assertThat(second, hasSize(1));
        assertThat(Stream.concat(first.stream(), second.stream()).toList(), containsInAnyOrder(key("a"), key("b"), key("c")));
        assertThat(poll(trigger), empty());
    }

    @Test
    void requestPastMaxHitsFails() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        var trigger = trigger(genre).hits(Property.ofValue(2)).build();
        var ids = IntStream.range(0, 399).mapToObj(i -> "\"id:mynamespace:music::gone" + i + "\"").collect(Collectors.joining(","));
        var watermark = "{\"field\":\"added_at\",\"value\":10,\"ids\":[" + ids + "]}";
        store(trigger, "flow", watermark);

        var error = assertThrows(IllegalStateException.class, () -> evaluate(trigger, "flow"));

        assertThat(error.getMessage(), allOf(containsString("399 documents already delivered share added_at = 10"), containsString("401 hits"), containsString("lower `hits`"), containsString("raise `maxHits`")));
        assertThat(stored(trigger, "flow"), is(Optional.of(watermark)));
    }

    @Test
    void unreadableWatermarkFails() throws Exception {
        for (var value : List.of("not a watermark", "{\"field\":\"added_at\"}", "{\"field\":\"added_at\",\"value\":10}", "{\"value\":10,\"ids\":[]}")) {
            var trigger = trigger(genre()).build();
            store(trigger, "flow", value);

            var error = assertThrows(IllegalStateException.class, () -> evaluate(trigger, "flow"), value);

            assertThat(value, error.getMessage(), allOf(containsString(Trigger.watermarkKey("flow", trigger.getId())), containsString("Delete")));
        }
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
    void rejectsOrderByOffsetLimitAndOwnParameters() throws Exception {
        try (var stub = VespaStub.start()) {
            var endpoint = Property.ofValue(stub.endpoint());
            var cases = new ArrayList<Map.Entry<Trigger, String>>();
            cases.add(Map.entry(trigger(genre()).endpoint(endpoint).yql(Property.ofValue("select * from music where true ORDER  BY year")).build(), "order by"));
            cases.add(Map.entry(trigger(genre()).endpoint(endpoint).yql(Property.ofValue("select * from music where true limit 5 offset 2")).build(), "offset"));
            cases.add(Map.entry(trigger(genre()).endpoint(endpoint).yql(Property.ofValue("select * from music where true LIMIT 5")).build(), "limit"));
            cases.add(Map.entry(trigger(genre()).endpoint(endpoint).yql(Property.ofValue("select * from music where album contains \"x\" limit 10")).build(), "limit"));
            for (var parameter : List.of("recall", "Sorting", "ranking.sorting", "sorting.degrading", "hits", "count", "offset", "start")) {
                cases.add(Map.entry(trigger(genre()).endpoint(endpoint).parameters(Property.ofValue(Map.of(parameter, "x"))).build(), parameter));
            }
            cases.add(Map.entry(trigger(genre()).endpoint(endpoint).parameters(Property.ofValue(Map.of("ranking", Map.of("sorting", "x")))).build(), "ranking.sorting"));

            for (var testCase : cases) {
                var expected = testCase.getValue();
                var error = assertThrows(IllegalArgumentException.class, () -> evaluate(testCase.getKey(), "flow"), expected);
                assertThat(expected, error.getMessage(), containsStringIgnoringCase("'" + expected + "'"));
            }
            assertThat(searches(stub), empty());
        }
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
    void nonNumericFieldDeliversNothing() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        var aString = trigger(genre).watermarkField(Property.ofValue("genre")).build();

        assertThat(evaluate(aString, "flow"), is(Optional.empty()));
    }

    @Test
    void decimalWatermarkValueFails() throws Exception {
        for (var decimal : List.of("10.5", "10.0")) {
            var hit = "{\"id\":\"id:mynamespace:music::p\",\"relevance\":1.0,\"fields\":{\"added_at\":" + decimal + "}}";
            try (var stub = VespaStub.answering("{\"root\":{\"id\":\"toplevel\",\"fields\":{\"totalCount\":1},\"coverage\":{\"full\":true},\"children\":[" + hit + "]}}")) {
                var trigger = trigger(genre()).endpoint(Property.ofValue(stub.endpoint())).build();

                var error = assertThrows(IllegalStateException.class, () -> evaluate(trigger, "flow"), decimal);

                assertThat(decimal, error.getMessage(), allOf(containsString("added_at"), containsString("id:mynamespace:music::p"), containsString(decimal), containsString("`int` or `long`")));
            }
        }
    }

    @Test
    void epochMillisecondValues() throws Exception {
        var genre = genre();
        feed("a", genre, 1768780800000L);
        feed("b", genre, 1768867200000L);
        var trigger = trigger(genre).build();

        assertThat(poll(trigger), contains(key("a"), key("b")));

        feed("c", genre, 1768953600000L);

        assertThat(poll(trigger), contains(key("c")));
        assertThat(poll(trigger), empty());
    }

    @Test
    void documentWithoutValueIsNeverDelivered() throws Exception {
        var genre = genre();
        put(PATH + "nodate", Map.of("album", "No date", "genre", genre));
        fed.add("nodate");
        feed("a", genre, 10);
        var trigger = trigger(genre).hits(Property.ofValue(1)).build();

        assertThat(poll(trigger), contains(key("a")));
        assertThat(poll(trigger), empty());
    }

    @Test
    void hitWithoutValueIsSkippedWithAWarning() throws Exception {
        var noValue = "{\"id\":\"id:mynamespace:music::nodate\",\"relevance\":1.0,\"fields\":{\"album\":\"No date\"}}";
        var noFields = "{\"id\":\"id:mynamespace:music::nofields\",\"relevance\":1.0}";
        var dated = "{\"id\":\"id:mynamespace:music::dated\",\"relevance\":1.0,\"fields\":{\"added_at\":20}}";
        try (var stub = VespaStub.answering("{\"root\":{\"id\":\"toplevel\",\"fields\":{\"totalCount\":3},\"coverage\":{\"full\":true},\"children\":[" + noValue + "," + noFields + "," + dated + "]}}")) {
            var trigger = trigger(genre()).endpoint(Property.ofValue(stub.endpoint())).build();
            var context = context(trigger, "flow");
            try (var logs = capture(context.getKey().getRunContext())) {
                var execution = trigger.evaluate(context.getKey(), context.getValue()).orElseThrow();

                assertThat(keys(execution), contains("id:mynamespace:music::dated"));
                assertThat(logs.warnings(), hasItems(
                    allOf(containsString("id:mynamespace:music::nodate"), containsString("added_at")),
                    allOf(containsString("id:mynamespace:music::nofields"), containsString("added_at"))
                ));
            }
        }
    }

    @Test
    void quotedClauseWordsAreAllowed() throws Exception {
        var genre = genre();
        put(PATH + "limit", Map.of("album", "limit 10", "genre", genre, "added_at", 10));
        fed.add("limit");
        put(PATH + "order", Map.of("album", "order by", "genre", genre, "added_at", 20));
        fed.add("order");
        put(PATH + "offset", Map.of("album", "offset 5", "genre", genre, "added_at", 30));
        fed.add("offset");
        put(PATH + "escaped", Map.of("album", "a \" limit 10", "genre", genre, "added_at", 40));
        fed.add("escaped");
        put(PATH + "apostrophe", Map.of("album", "it's limit 10", "genre", genre, "added_at", 50));
        fed.add("apostrophe");
        put(PATH + "quoted", Map.of("album", "say \"limit 10\"", "genre", genre, "added_at", 60));
        fed.add("quoted");
        var cases = Map.of(
            "album contains \"limit 10\"", key("limit"),
            "album contains \"order by\"", key("order"),
            "album contains 'offset 5'", key("offset"),
            "album contains \"a \\\" limit 10\"", key("escaped"),
            "album contains \"it's limit 10\"", key("apostrophe"),
            "album contains 'say \"limit 10\"'", key("quoted")
        );

        for (var testCase : cases.entrySet()) {
            var yql = "select * from music where genre contains \"%s\" and %s".formatted(genre, testCase.getKey());
            var trigger = trigger(genre).yql(Property.ofValue(yql)).build();

            assertThat(yql, poll(trigger), hasItem(testCase.getValue()));
        }
    }

    @Test
    void longQuotedTextIsChecked() throws Exception {
        try (var stub = VespaStub.answering("{\"root\":{\"id\":\"toplevel\",\"fields\":{\"totalCount\":0},\"coverage\":{\"full\":true}}}")) {
            var text = "\\\" ".repeat(20_000);
            var trigger = trigger(genre()).endpoint(Property.ofValue(stub.endpoint()))
                .yql(Property.ofValue("select * from music where album contains \"%s limit 10\"".formatted(text)))
                .build();

            assertThat(evaluate(trigger, "flow"), is(Optional.empty()));
            assertThat(searches(stub), hasSize(1));
        }
    }

    @Test
    void unclosedQuoteIsCheckedQuickly() {
        var text = "\\\"".repeat(250_000);
        var trigger = trigger(genre())
            .yql(Property.ofValue("select * from music where album contains \"%s limit 10".formatted(text)))
            .build();

        var context = context(trigger, "flow");

        var error = assertTimeoutPreemptively(Duration.ofSeconds(5), () -> assertThrows(IllegalArgumentException.class, () -> trigger.evaluate(context.getKey(), context.getValue())));

        assertThat(error.getMessage(), containsString("'limit'"));
    }

    @Test
    void unselectedWatermarkFieldFails() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        var id = "trigger_" + IdUtils.create();
        var selectsAll = trigger(genre).id(id).build();
        var withoutField = trigger(genre).id(id)
            .yql(Property.ofValue("select documentid, album from music where genre contains \"%s\"".formatted(genre)))
            .build();

        var context = context(withoutField, "flow");
        try (var logs = capture(context.getKey().getRunContext())) {
            var first = assertThrows(IllegalStateException.class, () -> withoutField.evaluate(context.getKey(), context.getValue()));

            assertThat(first.getMessage(), allOf(containsString("added_at"), containsString("select")));
            assertThat(logs.warnings(), empty());
        }
        assertThat(stored(withoutField, "flow"), is(Optional.empty()));

        assertThat(poll(selectsAll), contains(key("a")));
        var watermark = stored(selectsAll, "flow");

        var afterDelivery = assertThrows(IllegalStateException.class, () -> evaluate(withoutField, "flow"));

        assertThat(afterDelivery.getMessage(), containsString("added_at"));
        assertThat(stored(selectsAll, "flow"), is(watermark));
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
        put(PATH + "a", Map.of("album", "A", "genre", genre, "added_at", 10, "year", 5));
        put(PATH + "b", Map.of("album", "B", "genre", genre, "added_at", 20, "year", 3));
        fed.addAll(List.of("a", "b"));
        var id = "trigger_" + IdUtils.create();
        assertThat(poll(trigger(genre).id(id).build()), contains(key("a"), key("b")));

        var byYear = trigger(genre).id(id).watermarkField(Property.ofValue("year")).build();

        assertThat(poll(byYear), contains(key("b"), key("a")));
        assertThat(poll(byYear), empty());
    }

    @Test
    void hitsBelowOneFails() throws Exception {
        try (var stub = VespaStub.start()) {
            var trigger = trigger(genre()).endpoint(Property.ofValue(stub.endpoint())).hits(Property.ofExpression("{{ 1 - 1 }}")).build();

            var error = assertThrows(Exception.class, () -> evaluate(trigger, "flow"));

            assertThat(error.getMessage(), allOf(containsString("hits"), containsString("1")));
            assertThat(searches(stub), empty());
        }
    }

    @Test
    void refedWithAHigherValueIsDeliveredAgain() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        var trigger = trigger(genre).build();
        assertThat(poll(trigger), contains(key("a")));

        feed("a", genre, 20);

        assertThat(poll(trigger), contains(key("a")));
        assertThat(poll(trigger), empty());
    }

    @Test
    void sendsSortingAndRecall() throws Exception {
        var hit = "{\"id\":\"id:mynamespace:music::p\",\"relevance\":1.0,\"fields\":{\"added_at\":50}}";
        try (var stub = VespaStub.answering("{\"root\":{\"id\":\"toplevel\",\"fields\":{\"totalCount\":1},\"coverage\":{\"full\":true},\"children\":[" + hit + "]}}")) {
            var trigger = trigger(genre()).endpoint(Property.ofValue(stub.endpoint())).build();

            evaluate(trigger, "flow");
            evaluate(trigger, "flow");

            var bodies = new ArrayList<Map<String, Object>>();
            for (var seen : stub.seen()) {
                if (seen.path().startsWith("/search/")) {
                    bodies.add(JacksonMapper.toMap(seen.body()));
                }
            }
            assertThat(bodies, hasSize(2));
            for (var body : bodies) {
                assertThat(body.get("sorting"), is("+added_at"));
                assertThat(body.get("sorting.degrading"), is(false));
            }
            assertThat(bodies.getFirst().get("recall"), is("+added_at:[-9223372036854775808;]"));
            assertThat(bodies.get(1).get("recall"), is("+added_at:[50;]"));
        }
    }

    @Test
    void hitsAtMaxHitsFailsWithAClearMessage() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        feed("b", genre, 20);
        var trigger = trigger(genre).hits(Property.ofValue(400)).build();
        assertThat(poll(trigger), contains(key("a"), key("b")));
        var watermark = stored(trigger, "flow");

        feed("c", genre, 20);
        var error = assertThrows(IllegalStateException.class, () -> evaluate(trigger, "flow"));

        assertThat(error.getMessage(), allOf(containsString("1 document already delivered shares added_at = 20"), containsString("401 hits"), containsString("lower `hits`")));
        assertThat(stored(trigger, "flow"), is(watermark));
    }

    @Test
    void hitsOverflowFails() throws Exception {
        try (var stub = VespaStub.start()) {
            var trigger = trigger(genre()).endpoint(Property.ofValue(stub.endpoint())).hits(Property.ofValue(Integer.MAX_VALUE)).build();
            store(trigger, "flow", "{\"field\":\"added_at\",\"value\":10,\"ids\":[\"id:mynamespace:music::x\"]}");

            var error = assertThrows(IllegalStateException.class, () -> evaluate(trigger, "flow"));

            assertThat(error.getMessage(), allOf(containsString("too large"), containsString("lower `hits`")));
            assertThat(searches(stub), empty());
        }
    }

    @Test
    void groupingQueryFails() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        var trigger = trigger(genre)
            .yql(Property.ofValue("select * from music where genre contains \"%s\" | all(group(genre) each(output(count())))".formatted(genre)))
            .build();

        var error = assertThrows(IllegalStateException.class, () -> evaluate(trigger, "flow"));

        assertThat(error.getMessage(), allOf(containsString("delivers documents, not grouping results"), containsString("remove the grouping")));
    }

    @Test
    void oneHitPerPoll() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        feed("b", genre, 20);
        feed("c", genre, 30);
        var trigger = trigger(genre).hits(Property.ofValue(1)).build();

        assertThat(poll(trigger), contains(key("a")));
        assertThat(poll(trigger), contains(key("b")));
        assertThat(poll(trigger), contains(key("c")));
        assertThat(poll(trigger), empty());
    }

    @Test
    void deliversAtMostHitsWhenStoredDocumentsAreGone() throws Exception {
        var genre = genre();
        feed("a", genre, 10);
        feed("b", genre, 20);
        feed("c", genre, 30);
        feed("d", genre, 40);
        var trigger = trigger(genre).hits(Property.ofValue(2)).build();
        store(trigger, "flow", "{\"field\":\"added_at\",\"value\":10,\"ids\":[\"id:mynamespace:music::gone1\",\"id:mynamespace:music::gone2\"]}");

        assertThat(poll(trigger), contains(key("a"), key("b")));
        assertThat(poll(trigger), contains(key("c"), key("d")));
        assertThat(poll(trigger), empty());
    }

    @Test
    void partialAnswerFailsAndKeepsWatermark() throws Exception {
        var hit = "{\"id\":\"id:mynamespace:music::p\",\"relevance\":1.0,\"fields\":{\"added_at\":50}}";
        var withErrors = "{\"root\":{\"id\":\"toplevel\",\"fields\":{\"totalCount\":1},\"coverage\":{\"coverage\":100,\"full\":true},"
            + "\"errors\":[{\"code\":12,\"summary\":\"Timed out\",\"message\":\"content node 1 timed out\"}],\"children\":[" + hit + "]}}";
        var notFull = "{\"root\":{\"id\":\"toplevel\",\"fields\":{\"totalCount\":1},\"coverage\":{\"coverage\":50,\"full\":false,"
            + "\"degraded\":{\"timeout\":true}},\"children\":[" + hit + "]}}";
        for (var answer : List.of(withErrors, notFull)) {
            try (var stub = VespaStub.answering(answer)) {
                var trigger = trigger(genre()).endpoint(Property.ofValue(stub.endpoint())).build();
                var watermark = "{\"field\":\"added_at\",\"value\":10,\"ids\":[\"id:mynamespace:music::x\"]}";
                store(trigger, "flow", watermark);

                var error = assertThrows(IllegalStateException.class, () -> evaluate(trigger, "flow"), answer);

                assertThat(answer, error.getMessage(), containsString("only part of"));
                assertThat(answer, stored(trigger, "flow"), is(Optional.of(watermark)));
            }
        }
    }


    @Test
    void requiredPropertiesFail() {
        var noYql = trigger(genre()).yql(Property.ofValue(" ")).build();
        var blankField = trigger(genre()).watermarkField(Property.ofValue(" ")).build();

        assertThat(assertThrows(IllegalArgumentException.class, () -> evaluate(noYql, "flow")).getMessage(), containsString("'yql'"));
        assertThat(assertThrows(IllegalArgumentException.class, () -> evaluate(blankField, "flow")).getMessage(), containsString("'watermarkField'"));
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

    private static List<String> searches(VespaStub stub) {
        return stub.seen().stream().map(VespaStub.Seen::path).filter(path -> path.startsWith("/search/")).toList();
    }

    private static Capture capture(RunContext runContext) {
        var capture = new Capture((Logger) runContext.logger(), new CopyOnWriteArrayList<>());
        capture.start();
        capture.logger().addAppender(capture);
        return capture;
    }

    // a list that's safe to read while the trigger's logger writes to it; detached when the test ends
    private static final class Capture extends AppenderBase<ILoggingEvent> implements AutoCloseable {
        private final Logger logger;
        private final List<ILoggingEvent> events;

        private Capture(Logger logger, List<ILoggingEvent> events) {
            this.logger = logger;
            this.events = events;
        }

        Logger logger() {
            return logger;
        }

        List<String> warnings() {
            return events.stream().filter(e -> e.getLevel() == Level.WARN).map(ILoggingEvent::getFormattedMessage).toList();
        }

        @Override
        protected void append(ILoggingEvent event) {
            events.add(event);
        }

        @Override
        public void close() {
            logger.detachAppender(this);
            stop();
        }
    }
}
