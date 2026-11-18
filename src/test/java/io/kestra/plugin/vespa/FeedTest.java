package io.kestra.plugin.vespa;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FeedTest extends VespaContainer {
    private static final String PATH = "/document/v1/mynamespace/music/docid/";
    private static final String ID = "id:mynamespace:music::";

    @Test
    void put() throws Exception {
        var catalog = count("true");
        var records = List.of(
            record("feedtest-put-a", Map.of("artist", "feedtest put", "album", "a", "year", 1959)),
            record("feedtest-put-b", Map.of("artist", "feedtest put", "album", "b")),
            record("feedtest-put-c", Map.of("artist", "feedtest put", "album", "c"))
        );
        var task = Feed.builder().endpoint(Property.ofValue(endpoint())).from(records).build();
        var runContext = runContextFactory.of();

        var output = task.run(runContext);

        assertThat(document(PATH + "feedtest-put-a").get("fields").get("year").asInt(), is(1959));
        assertThat(document(PATH + "feedtest-put-b").get("fields").get("album").asText(), is("b"));
        assertThat(document(PATH + "feedtest-put-c").get("fields").get("artist").asText(), is("feedtest put"));
        assertThat(output.getSuccessCount(), is(3L));
        assertThat(output.getFailureCount(), is(0L));
        assertThat(output.getUri(), is(nullValue()));
        assertThat(output.finalState().isEmpty(), is(true));
        assertThat(metric(runContext, "records.succeeded"), is(3.0));
        assertThat(metric(runContext, "records.failed"), is(0.0));
        assertThat(count("true") - 3, is(catalog));
        removeAll(records);
    }

    @Test
    void putFromStoredFile() throws Exception {
        var records = List.of(
            record("feedtest-file-a", Map.of("artist", "feedtest file", "album", "a")),
            record("feedtest-file-b", Map.of("artist", "feedtest file", "album", "b"))
        );
        var runContext = runContextFactory.of();
        var task = Feed.builder().endpoint(Property.ofValue(endpoint())).from(store(runContext, records).toString()).build();

        var output = task.run(runContext);

        assertThat(document(PATH + "feedtest-file-a").get("fields").get("album").asText(), is("a"));
        assertThat(document(PATH + "feedtest-file-b").get("fields").get("album").asText(), is("b"));
        assertThat(output.getSuccessCount(), is(2L));
        removeAll(records);
    }

    @Test
    void update() throws Exception {
        put(PATH + "feedtest-update", Map.of("artist", "feedtest update", "album", "before"));
        var task = Feed.builder()
            .endpoint(Property.ofValue(endpoint()))
            .operation(Property.ofValue(Feed.Operation.UPDATE))
            .from(List.of(Map.of("id", ID + "feedtest-update", "fields", Map.of("album", Map.of("assign", "after")))))
            .build();

        var output = task.run(runContextFactory.of());

        var fields = document(PATH + "feedtest-update").get("fields");
        assertThat(fields.get("album").asText(), is("after"));
        assertThat(fields.get("artist").asText(), is("feedtest update"));
        assertThat(output.getSuccessCount(), is(1L));
        delete(PATH + "feedtest-update");
    }

    @Test
    void remove() throws Exception {
        for (var key : List.of("feedtest-remove-a", "feedtest-remove-b")) {
            put(PATH + key, Map.of("artist", "feedtest remove", "album", key));
        }
        var task = Feed.builder()
            .endpoint(Property.ofValue(endpoint()))
            .operation(Property.ofValue(Feed.Operation.REMOVE))
            .from(List.of(Map.of("id", ID + "feedtest-remove-a"), Map.of("id", ID + "feedtest-remove-b")))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(status(PATH + "feedtest-remove-a"), is(404));
        assertThat(status(PATH + "feedtest-remove-b"), is(404));
        assertThat(output.getSuccessCount(), is(2L));
    }

    @Test
    void fullIdAndPlainKey() throws Exception {
        var task = Feed.builder()
            .endpoint(Property.ofValue(endpoint()))
            .namespace(Property.ofValue("mynamespace"))
            .documentType(Property.ofValue("music"))
            .from(List.of(
                record("feedtest-full-id", Map.of("artist", "feedtest id", "album", "full")),
                Map.of("id", "feedtest-plain-key", "fields", Map.of("artist", "feedtest id", "album", "key"))
            ))
            .build();

        task.run(runContextFactory.of());

        assertThat(document(PATH + "feedtest-full-id").get("id").asText(), is(ID + "feedtest-full-id"));
        assertThat(document(PATH + "feedtest-plain-key").get("id").asText(), is(ID + "feedtest-plain-key"));
        delete(PATH + "feedtest-full-id");
        delete(PATH + "feedtest-plain-key");
    }

    static Stream<Arguments> invalidRecords() {
        var fields = Map.of("album", "x");
        var noId = new HashMap<String, Object>(Map.of("fields", fields));
        return Stream.of(
            Arguments.of("key without namespace", Map.of("id", "kind-of-blue", "fields", fields), Feed.Operation.PUT, null, "music", "`namespace`"),
            Arguments.of("key without document type", Map.of("id", "kind-of-blue", "fields", fields), Feed.Operation.PUT, "mynamespace", null, "`documentType`"),
            Arguments.of("no id", noId, Feed.Operation.PUT, null, null, "no `id`"),
            Arguments.of("id not text", Map.of("id", 42, "fields", fields), Feed.Operation.PUT, null, null, "must be text"),
            Arguments.of("id that doesn't parse", Map.of("id", "id:mynamespace:music", "fields", fields), Feed.Operation.PUT, null, null, "id:<namespace>:<document type>::<key>"),
            Arguments.of("dot-dot part", Map.of("id", ID + "..", "fields", fields), Feed.Operation.PUT, null, null, "id:<namespace>:<document type>::<key>"),
            Arguments.of("put without fields", Map.of("id", ID + "a"), Feed.Operation.PUT, null, null, "`fields`"),
            Arguments.of("put with fields not a map", Map.of("id", ID + "a", "fields", "x"), Feed.Operation.PUT, null, null, "`fields`"),
            Arguments.of("update without fields", Map.of("id", ID + "a"), Feed.Operation.UPDATE, null, null, "`fields`")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidRecords")
    void invalidRecord(String name, Map<String, Object> invalid, Feed.Operation operation, String namespace, String documentType, String error) throws Exception {
        try (var stub = VespaStub.start()) {
            var task = Feed.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .operation(Property.ofValue(operation))
                .namespace(namespace == null ? null : Property.ofValue(namespace))
                .documentType(documentType == null ? null : Property.ofValue(documentType))
                .failOnError(Property.ofValue(false))
                .from(List.of(invalid, Map.of("id", ID + "good", "fields", Map.of("album", "y"))))
                .build();
            var runContext = runContextFactory.of();

            var output = task.run(runContext);

            assertThat(output.getSuccessCount(), is(1L));
            assertThat(output.getFailureCount(), is(1L));
            var failed = stored(runContext, output.getUri()).getFirst();
            assertThat(failed.get("record"), is(invalid));
            assertThat(failed.get("error").toString(), containsString(error));
            assertThat(stub.seen().stream().filter(s -> !handshake(s)).map(VespaStub.Seen::path).toList(), contains(PATH + "good"));
        }
    }

    @Test
    void failedRecordsAllowed() throws Exception {
        var records = badBatch("allowed");
        var task = Feed.builder().endpoint(Property.ofValue(endpoint())).failOnError(Property.ofValue(false)).from(records).build();
        var runContext = runContextFactory.of();
        var logs = capture(runContext);

        var output = task.run(runContext);

        assertThat(output.finalState().orElseThrow(), is(State.Type.WARNING));
        assertThat(output.getSuccessCount(), is(2L));
        assertThat(output.getFailureCount(), is(1L));
        assertThat(warnings(logs), contains(allOf(containsString("2"), containsString("1"), containsString("the failed records are in `uri`"))));
        var failed = stored(runContext, output.getUri());
        assertThat(failed, hasSize(1));
        assertThat(failed.getFirst().get("record"), is(records.get(1)));
        assertThat(failed.getFirst().get("error").toString(), containsString("nosuchfield"));
        assertThat(document(PATH + "feedtest-bad-allowed-a").get("fields").get("album").asText(), is("a"));
        assertThat(document(PATH + "feedtest-bad-allowed-c").get("fields").get("album").asText(), is("c"));
        assertThat(metric(runContext, "records.failed"), is(1.0));
        removeAll(List.of(records.getFirst(), records.getLast()));
    }

    @Test
    void failedRecordsFailTheTask() throws Exception {
        var records = badBatch("refused");
        var task = Feed.builder().endpoint(Property.ofValue(endpoint())).from(records).build();

        var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

        assertThat(e.getMessage(), allOf(
            containsString("2 records were fed and 1 failed"),
            containsString(ID + "feedtest-bad-refused-b"),
            containsString("nosuchfield"),
            containsString("set `failOnError: false`")
        ));
        assertThat(e.getMessage().length(), lessThanOrEqualTo(1000));
        assertThat(document(PATH + "feedtest-bad-refused-a").get("fields").get("album").asText(), is("a"));
        assertThat(document(PATH + "feedtest-bad-refused-c").get("fields").get("album").asText(), is("c"));
        removeAll(List.of(records.getFirst(), records.getLast()));
    }

    @Test
    void manyFailuresCutTheMessage() throws Exception {
        try (var stub = VespaStub.answering(new VespaStub.Answer(200, "{}"), new VespaStub.Answer(400, "{\"message\": \"%s\"}".formatted("x".repeat(300))))) {
            var records = IntStream.range(0, 20).mapToObj(i -> (Map<String, Object>) record("feedtest-many-failed-" + i, Map.of("album", "x"))).toList();
            var task = Feed.builder().endpoint(Property.ofValue(stub.endpoint())).from(records).build();

            var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), allOf(containsString("0 records were fed and 20 failed"), endsWith("set `failOnError: false` to keep the good records and get the failed ones in a file.")));
            assertThat(e.getMessage().length(), lessThanOrEqualTo(1000));
        }
    }

    @Test
    void noRecords() throws Exception {
        try (var stub = VespaStub.start()) {
            for (var task : List.of(
                Feed.builder().endpoint(Property.ofValue(stub.endpoint())).from(List.of()).build(),
                Feed.builder().endpoint(Property.ofValue(stub.endpoint())).build()
            )) {
                var output = task.run(runContextFactory.of());

                assertThat(output.getSuccessCount(), is(0L));
                assertThat(output.getFailureCount(), is(0L));
                assertThat(output.getUri(), is(nullValue()));
            }
            assertThat(stub.seen().stream().filter(s -> !handshake(s)).toList(), is(empty()));
        }
    }

    @Test
    void unreachableEndpoint() throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        var task = Feed.builder().endpoint(Property.ofValue("http://localhost:" + port)).from(List.of(record("a", Map.of()))).build();
        var start = Instant.now();

        var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

        assertThat(Duration.between(start, Instant.now()), lessThan(Duration.ofSeconds(60)));
        assertThat(e.getMessage(), allOf(containsString("`endpoint`"), containsString("http://localhost:" + port)));
    }

    @Test
    void refusedHandshake() throws Exception {
        try (var stub = VespaStub.answering(500, "{\"message\": \"the container is starting\"}")) {
            var task = Feed.builder().endpoint(Property.ofValue(stub.endpoint())).from(List.of(record("a", Map.of()))).build();

            var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), allOf(containsString("`endpoint`"), containsString("the container is starting")));
            assertThat(stub.seen(), hasSize(1));
        }
    }

    static Stream<Arguments> invalidProperties() {
        return Stream.of(
            Arguments.of("operation", (UnaryOperator<Feed.FeedBuilder<?, ?>>) b -> b.operation(Property.ofExpression("{{ 'UPSERT' }}"))),
            Arguments.of("failOnError", (UnaryOperator<Feed.FeedBuilder<?, ?>>) b -> b.failOnError(Property.ofExpression("{{ 'maybe' }}"))),
            Arguments.of("endpoint", (UnaryOperator<Feed.FeedBuilder<?, ?>>) b -> b.endpoint(Property.ofValue("ftp://localhost")))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidProperties")
    void invalidProperty(String property, UnaryOperator<Feed.FeedBuilder<?, ?>> invalid) throws Exception {
        try (var stub = VespaStub.start()) {
            var task = invalid.apply(Feed.builder().endpoint(Property.ofValue(stub.endpoint())).from(List.of(record("a", Map.of("album", "x"))))).build();

            var e = assertThrows(Exception.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), containsStringIgnoringCase(property));
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void bearerToken() throws Exception {
        try (var stub = VespaStub.start()) {
            var task = Feed.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .token(Property.ofValue("feed-secret-token"))
                .from(List.of(record("a", Map.of("album", "x")), record("b", Map.of("album", "y"))))
                .build();
            var runContext = runContextFactory.of();
            var logs = capture(runContext);

            var output = task.run(runContext);

            assertThat(stub.seen(), hasSize(3));
            assertThat(stub.seen().stream().map(VespaStub.Seen::authorization).toList(), everyItem(is(List.of("Bearer feed-secret-token"))));
            assertThat(task.toString(), not(containsString("feed-secret-token")));
            assertThat(JacksonMapper.ofJson().writeValueAsString(output), not(containsString("feed-secret-token")));
            assertThat(logs.stream().map(ILoggingEvent::getFormattedMessage).toList(), everyItem(not(containsString("feed-secret-token"))));
        }
    }

    @Test
    void noTokenNoAuthorization() throws Exception {
        try (var stub = VespaStub.start()) {
            for (var token : Arrays.asList(Property.<String>ofValue(" "), null)) {
                var task = Feed.builder().endpoint(Property.ofValue(stub.endpoint())).token(token).from(List.of(record("a", Map.of("album", "x")))).build();

                task.run(runContextFactory.of());
            }
            assertThat(stub.seen(), hasSize(4));
            assertThat(stub.seen().stream().map(VespaStub.Seen::authorization).toList(), everyItem(is(nullValue())));
        }
    }

    @Test
    void killStopsTheFeed() throws Exception {
        try (var stub = VespaStub.answering(new VespaStub.Answer(200, "{}"), new VespaStub.Answer(200, "{}", Duration.ofSeconds(60)))) {
            var records = IntStream.range(0, 5000).mapToObj(i -> (Map<String, Object>) record("feedtest-kill-" + i, Map.of("album", "x"))).toList();
            var task = Feed.builder().endpoint(Property.ofValue(stub.endpoint())).from(records).build();
            var run = CompletableFuture.runAsync(() -> {
                try {
                    task.run(runContextFactory.of());
                } catch (Exception ignored) {
                    // a killed feed may end either way; only its timing is checked
                }
            });
            await("records in flight", Duration.ofSeconds(30), () -> {
                if (stub.seen().size() < 2) {
                    throw new IllegalStateException("no record sent yet");
                }
                return null;
            });

            var killStart = Instant.now();
            task.kill();
            assertThat(Duration.between(killStart, Instant.now()), lessThan(Duration.ofSeconds(1)));
            run.get(10, TimeUnit.SECONDS);
            var seenAfterKill = stub.seen().size();
            task.kill();
            Thread.sleep(Duration.ofSeconds(1));

            assertThat(stub.seen().size(), is(seenAfterKill));
        }
    }

    @Test
    void operationTimeout() throws Exception {
        try (var stub = VespaStub.answering(new VespaStub.Answer(200, "{}"), new VespaStub.Answer(200, "{}", Duration.ofSeconds(5)))) {
            var task = Feed.builder()
                .endpoint(Property.ofValue(stub.endpoint()))
                .readTimeout(Property.ofValue(Duration.ofSeconds(1)))
                .failOnError(Property.ofValue(false))
                .from(List.of(record("feedtest-timeout", Map.of("album", "x"))))
                .build();
            var runContext = runContextFactory.of();
            var start = Instant.now();

            var output = task.run(runContext);

            assertThat(Duration.between(start, Instant.now()), lessThan(Duration.ofSeconds(5)));
            assertThat(output.getFailureCount(), is(1L));
            assertThat(stored(runContext, output.getUri()).getFirst().get("error").toString(), containsString("Vespa may still have applied it"));
        }
    }

    @Test
    void manyRecordsFromStoredFile() throws Exception {
        var records = IntStream.range(0, 2000).mapToObj(i -> (Map<String, Object>) record("feedtest-many-" + i, Map.of("artist", "feedtest many", "album", "a" + i))).toList();
        var runContext = runContextFactory.of();
        var task = Feed.builder().endpoint(Property.ofValue(endpoint())).from(store(runContext, records).toString()).build();

        var output = task.run(runContext);

        assertThat(output.getSuccessCount(), is(2000L));
        assertThat(count("artist contains \"feedtest many\""), is(2000L));
        Delete.builder()
            .endpoint(Property.ofValue(endpoint()))
            .selection(Property.ofValue("music.artist == \"feedtest many\""))
            .cluster(Property.ofValue("music"))
            .allowSelection(Property.ofValue(true))
            .build()
            .run(runContextFactory.of());
    }

    // one good record, one with a field the schema doesn't have, one good record
    private static List<Map<String, Object>> badBatch(String name) {
        return List.of(
            record("feedtest-bad-" + name + "-a", Map.of("artist", "feedtest bad", "album", "a")),
            record("feedtest-bad-" + name + "-b", Map.of("artist", "feedtest bad", "nosuchfield", "b")),
            record("feedtest-bad-" + name + "-c", Map.of("artist", "feedtest bad", "album", "c"))
        );
    }

    private static Map<String, Object> record(String key, Map<String, Object> fields) {
        return Map.of("id", ID + key, "fields", fields);
    }

    private static boolean handshake(VespaStub.Seen seen) {
        return seen.path().contains("/feeder/handshake/");
    }

    private static URI store(RunContext runContext, List<Map<String, Object>> records) throws IOException {
        var file = runContext.workingDir().createTempFile(".ion");
        try (var out = Files.newOutputStream(file)) {
            for (var r : records) {
                FileSerde.write(out, r);
            }
        }
        return runContext.storage().putFile(file.toFile());
    }

    private static List<Map<String, Object>> stored(RunContext runContext, URI uri) throws IOException {
        try (var file = runContext.storage().getFile(uri)) {
            return FileSerde.readAll(file, new TypeReference<Map<String, Object>>() {}).collectList().block();
        }
    }

    private static Object metric(RunContext runContext, String name) {
        return runContext.metrics().stream().filter(m -> m.getName().equals(name)).findFirst().orElseThrow().getValue();
    }

    private static long count(String where) throws Exception {
        return search(Map.of("yql", "select * from music where " + where, "hits", 0)).path("root").path("fields").path("totalCount").asLong();
    }

    private static void removeAll(List<Map<String, Object>> records) {
        records.forEach(r -> delete(PATH + r.get("id").toString().substring(ID.length())));
    }

    private static List<ILoggingEvent> capture(RunContext runContext) {
        var logs = new CopyOnWriteArrayList<ILoggingEvent>();
        var appender = new AppenderBase<ILoggingEvent>() {
            @Override
            protected void append(ILoggingEvent event) {
                logs.add(event);
            }
        };
        appender.start();
        ((Logger) runContext.logger()).addAppender(appender);
        return logs;
    }

    private static List<String> warnings(List<ILoggingEvent> logs) {
        return logs.stream().filter(e -> e.getLevel().levelStr.equals("WARN")).map(ILoggingEvent::getFormattedMessage).toList();
    }
}
