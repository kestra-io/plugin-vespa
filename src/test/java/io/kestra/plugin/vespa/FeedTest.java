package io.kestra.plugin.vespa;

import ai.vespa.feed.client.FeedException;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class FeedTest extends VespaContainer {
    private static final String PATH = "/document/v1/mynamespace/music/docid/";
    private static final String ID = "id:mynamespace:music::";

    @Inject
    private ModelValidator modelValidator;

    @Test
    void put() throws Exception {
        var catalog = count("true");
        var records = List.of(
            record("feedtest-put-a", Map.of("artist", "feedtest put", "album", "a", "year", 1959)),
            record("feedtest-put-b", Map.of("artist", "feedtest put", "album", "b")),
            record("feedtest-put-c", Map.of("artist", "feedtest put", "album", "c"))
        );
        var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(endpoint())).from(records).build();
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
        var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(endpoint())).from(store(runContext, records).toString()).build();

        var output = task.run(runContext);

        assertThat(document(PATH + "feedtest-file-a").get("fields").get("album").asText(), is("a"));
        assertThat(document(PATH + "feedtest-file-b").get("fields").get("album").asText(), is("b"));
        assertThat(output.getSuccessCount(), is(2L));
        removeAll(records);
    }

    @Test
    void update() throws Exception {
        put(PATH + "feedtest-update", Map.of("artist", "feedtest update", "album", "before"));
        var task = Feed.builder().id(IdUtils.create())
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
        var task = Feed.builder().id(IdUtils.create())
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
        var task = Feed.builder().id(IdUtils.create())
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
            var task = Feed.builder().id(IdUtils.create())
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
            assertThat(stub.seen().stream().filter(s -> !handshake(s) && !preflight(s)).map(VespaStub.Seen::path).toList(), contains(PATH + "good"));
        }
    }

    @Test
    void failedRecordsAllowed() throws Exception {
        var records = badBatch("allowed");
        var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(endpoint())).failOnError(Property.ofValue(false)).from(records).build();
        var runContext = runContextFactory.of();
        var logs = capture(runContext);

        var output = task.run(runContext);

        assertThat(output.finalState().orElseThrow(() -> new AssertionError("no final state")), is(State.Type.WARNING));
        assertThat(output.getSuccessCount(), is(2L));
        assertThat(output.getFailureCount(), is(1L));
        assertThat(warnings(logs), contains("2 records were fed and 1 failed; the failed records are in `uri`."));
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
        var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(endpoint())).from(records).build();

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
            var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(stub.endpoint())).from(records).build();

            var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), allOf(containsString("0 records were fed and 20 failed"), endsWith("set `failOnError: false` to keep the good records and get the failed ones in a file.")));
            assertThat(e.getMessage().length(), lessThanOrEqualTo(1000));
        }
    }

    @Test
    void noRecords() throws Exception {
        try (var stub = VespaStub.start()) {
            var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(stub.endpoint())).from(List.of()).build();

            var output = task.run(runContextFactory.of());

            assertThat(output.getSuccessCount(), is(0L));
            assertThat(output.getFailureCount(), is(0L));
            assertThat(output.getUri(), is(nullValue()));
            assertThat(stub.seen().stream().filter(s -> !handshake(s) && !preflight(s)).toList(), is(empty()));
        }
    }

    @Test
    void fromRequired() {
        var task = Feed.builder().id(IdUtils.create()).type(Feed.class.getName()).endpoint(Property.ofValue("http://localhost:8080")).build();

        var violations = modelValidator.isValid(task);

        assertThat(violations.isPresent(), is(true));
        assertThat(violations.get().getMessage(), containsString("from"));
    }

    @Test
    void unreachableEndpoint() throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue("http://localhost:" + port)).from(List.of(record("a", Map.of()))).build();
        var start = Instant.now();

        var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

        assertThat(Duration.between(start, Instant.now()), lessThan(Duration.ofSeconds(60)));
        assertThat(e.getMessage(), allOf(containsString("`endpoint`"), containsString("http://localhost:" + port)));
        assertThat(e.getCause(), instanceOf(FeedException.class));
    }

    @Test
    void refusedHandshake() throws Exception {
        try (var stub = VespaStub.answering(500, "{\"message\": \"the container is starting\"}")) {
            var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(stub.endpoint())).from(List.of(record("a", Map.of()))).build();

            var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), allOf(containsString("`endpoint`"), containsString("the container is starting")));
            assertThat(stub.seen().stream().filter(s -> !preflight(s)).toList(), hasSize(1));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("healthAnswers")
    void preflightBeforeFeed(String name, VespaStub.Answer health) throws Exception {
        try (var stub = VespaStub.withHealth(health, new VespaStub.Answer(200, "{}"))) {
            var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(stub.endpoint())).from(List.of(record("a", Map.of("album", "x")))).build();

            var output = task.run(runContextFactory.of());

            assertThat(output.getSuccessCount(), is(1L));
            assertThat(stub.seen().getFirst().method(), is("HEAD"));
            assertThat(stub.seen().getFirst().path(), is("/state/v1/health"));
            assertThat(stub.seen().stream().filter(FeedTest::preflight).toList(), hasSize(1));
            assertThat(handshake(stub.seen().get(1)), is(true));
        }
    }

    static Stream<Arguments> healthAnswers() {
        return Stream.of(
            Arguments.of("healthy", new VespaStub.Answer(200, "{}")),
            Arguments.of("not found", new VespaStub.Answer(404, "{}")),
            Arguments.of("server error", new VespaStub.Answer(500, "{\"message\": \"down\"}"))
        );
    }

    @Test
    void preflightSlowEndpoint() throws Exception {
        try (var stub = VespaStub.start(); var proxy = new SlowHealthProxy(stub.endpoint())) {
            var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(proxy.endpoint())).from(List.of(record("a", Map.of("album", "x")))).build();
            var start = Instant.now();

            var output = task.run(runContextFactory.of());

            assertThat(Duration.between(start, Instant.now()), lessThan(Duration.ofSeconds(15)));
            assertThat(proxy.healthSeen(), is(true));
            assertThat(output.getSuccessCount(), is(1L));
        }
    }

    @Test
    void killDuringPreflight() throws Exception {
        try (var stub = VespaStub.start(); var proxy = new SlowHealthProxy(stub.endpoint())) {
            var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(proxy.endpoint())).from(List.of(record("a", Map.of("album", "x")))).build();
            var run = CompletableFuture.supplyAsync(() -> {
                try {
                    return task.run(runContextFactory.of());
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            await("health request held", Duration.ofSeconds(30), () -> {
                if (!proxy.healthSeen()) {
                    throw new IllegalStateException("no health request yet");
                }
                return null;
            });
            Thread.sleep(Duration.ofSeconds(3));
            var killStart = Instant.now();

            task.kill();
            var output = run.get(5, TimeUnit.SECONDS);

            assertThat(Duration.between(killStart, Instant.now()), lessThan(Duration.ofSeconds(5)));
            assertThat(output.getSuccessCount(), is(0L));
            assertThat(output.getFailureCount(), is(0L));
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void killBeforeRun() throws Exception {
        try (var stub = VespaStub.start()) {
            var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(stub.endpoint())).from(List.of(record("a", Map.of("album", "x")))).build();
            task.kill();
            var start = Instant.now();

            var output = task.run(runContextFactory.of());

            assertThat(Duration.between(start, Instant.now()), lessThan(Duration.ofSeconds(5)));
            assertThat(output.getSuccessCount(), is(0L));
            assertThat(output.getFailureCount(), is(0L));
            assertThat(stub.seen(), is(empty()));
        }
    }

    // the endpoint column is a format: %s is the stub's endpoint
    static Stream<Arguments> invalidProperties() {
        return Stream.of(
            Arguments.of("operation", "`operation`", Property.ofExpression("{{ 'UPSERT' }}"), Property.ofValue(true), "%s"),
            Arguments.of("failOnError", "`failOnError`", Property.ofValue(Feed.Operation.PUT), Property.ofExpression("{{ 'maybe' }}"), "%s"),
            Arguments.of("endpoint not http", "endpoint", Property.ofValue(Feed.Operation.PUT), Property.ofValue(true), "ftp://localhost"),
            Arguments.of("endpoint with a path", "`endpoint`", Property.ofValue(Feed.Operation.PUT), Property.ofValue(true), "%s/prefix")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidProperties")
    void invalidProperty(String name, String property, Property<Feed.Operation> operation, Property<Boolean> failOnError, String endpoint) throws Exception {
        try (var stub = VespaStub.start()) {
            var task = Feed.builder()
                .id(IdUtils.create())
                .endpoint(Property.ofValue(endpoint.formatted(stub.endpoint())))
                .operation(operation)
                .failOnError(failOnError)
                .from(List.of(record("a", Map.of("album", "x"))))
                .build();

            var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), containsString(property));
            assertThat(stub.seen(), is(empty()));
        }
    }

    @Test
    void endpointWithTrailingSlash() throws Exception {
        try (var stub = VespaStub.start()) {
            var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(stub.endpoint() + "/")).from(List.of(record("a", Map.of("album", "x")))).build();

            var output = task.run(runContextFactory.of());

            assertThat(output.getSuccessCount(), is(1L));
            assertThat(stub.seen().getLast().path(), is(PATH + "a"));
        }
    }

    @Test
    void failedRecordsFileUnwritable() throws Exception {
        try (var stub = VespaStub.start()) {
            var unwritable = Map.<String, Object>of("fields", Map.of("album", "x"), "extra", new Unwritable());
            var task = Feed.builder()
                .id(IdUtils.create())
                .endpoint(Property.ofValue(stub.endpoint()))
                .failOnError(Property.ofValue(false))
                .from(List.of(unwritable, record("after-a", Map.of("album", "x")), record("after-b", Map.of("album", "x"))))
                .build();

            var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), allOf(containsString("temporary file"), containsString("free disk space")));
            assertThat(e.getCause(), instanceOf(IOException.class));
            assertThat(stub.seen().stream().filter(s -> !handshake(s) && !preflight(s)).toList(), is(empty()));
        }
    }

    @Test
    void unwritableRefusedRecordEndsTheRun() throws Exception {
        try (var stub = VespaStub.answering(new VespaStub.Answer(200, "{}"), new VespaStub.Answer(400, "{\"message\": \"refused\"}"))) {
            var unwritable = Map.<String, Object>of("id", ID + "feedtest-unwritable", "fields", Map.of("album", "x"), "extra", new Unwritable());
            var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(stub.endpoint())).failOnError(Property.ofValue(false)).from(List.of(unwritable)).build();

            var e = assertTimeoutPreemptively(Duration.ofSeconds(30), () -> assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of())));

            assertThat(e.getMessage(), containsString("temporary file"));
        }
    }

    @Test
    void writeErrorWinsOverCloseError() throws Exception {
        var fullDisk = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("No space left on device");
            }

            @Override
            public void close() throws IOException {
                throw new IOException("close failed");
            }
        };
        var failures = new Feed.Failures(fullDisk);

        failures.add(Map.of("id", "a"), "refused");
        failures.close();

        assertThat(failures.writeError().orElseThrow(() -> new AssertionError("no write error kept")).getMessage(), is("No space left on device"));
    }

    @Test
    void onlyFirstWriteErrorKept() throws Exception {
        var writes = new AtomicInteger();
        var failing = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("write " + writes.incrementAndGet());
            }
        };
        var failures = new Feed.Failures(failing);

        failures.add(Map.of("id", "a"), "refused");
        failures.add(Map.of("id", "b"), "refused");

        assertThat(writes.get(), is(1));
        assertThat(failures.writeError().orElseThrow(() -> new AssertionError("no write error kept")).getMessage(), is("write 1"));
    }

    @Test
    void closeErrorKept() throws Exception {
        var buffered = new BufferedOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("No space left on device");
            }
        });
        var failures = new Feed.Failures(buffered);

        failures.add(Map.of("id", "a"), "refused");
        failures.close();

        assertThat(failures.writeError().orElseThrow(() -> new AssertionError("no write error kept")).getMessage(), is("No space left on device"));
    }

    @Test
    void firstTenFailures() throws Exception {
        try (var stub = VespaStub.answering(new VespaStub.Answer(200, "{}"), new VespaStub.Answer(400, "{\"message\": \"x\"}"))) {
            var records = IntStream.range(0, 15).mapToObj(i -> (Map<String, Object>) Map.<String, Object>of("id", "id:n:t::k%02d".formatted(i), "fields", Map.of("a", "x"))).toList();
            var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(stub.endpoint())).from(records).build();

            var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));

            assertThat(e.getMessage(), not(containsString("...")));
            assertThat(e.getMessage().split("id:n:t::k", -1).length - 1, is(10));
        }
    }

    @Test
    void bearerToken() throws Exception {
        try (var stub = VespaStub.start()) {
            var task = Feed.builder().id(IdUtils.create())
                .endpoint(Property.ofValue(stub.endpoint()))
                .token(Property.ofValue("feed-secret-token"))
                .from(List.of(record("a", Map.of("album", "x")), record("b", Map.of("album", "y"))))
                .build();
            var runContext = runContextFactory.of();
            var logs = capture(runContext);

            var output = task.run(runContext);

            assertThat(stub.seen(), hasSize(4));
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
                var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(stub.endpoint())).token(token).from(List.of(record("a", Map.of("album", "x")))).build();

                task.run(runContextFactory.of());
            }
            assertThat(stub.seen(), hasSize(6));
            assertThat(stub.seen().stream().map(VespaStub.Seen::authorization).toList(), everyItem(is(nullValue())));
        }
    }

    @Test
    void killStopsTheFeed() throws Exception {
        try (var stub = VespaStub.answering(new VespaStub.Answer(200, "{}"), new VespaStub.Answer(200, "{}", Duration.ofSeconds(60)))) {
            var records = IntStream.range(0, 5000).mapToObj(i -> (Map<String, Object>) record("feedtest-kill-" + i, Map.of("album", "x"))).toList();
            var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(stub.endpoint())).from(records).build();
            var ended = new AtomicReference<Exception>();
            var run = CompletableFuture.runAsync(() -> {
                try {
                    task.run(runContextFactory.of());
                } catch (Exception e) {
                    ended.set(e);
                }
            });
            await("records in flight", Duration.ofSeconds(30), () -> {
                if (stub.seen().stream().filter(s -> !preflight(s)).count() < 2) {
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
            assertThat(String.valueOf(ended.get()), not(containsString("Client is closed")));
        }
    }

    @Test
    void operationTimeout() throws Exception {
        try (var stub = VespaStub.answering(new VespaStub.Answer(200, "{}"), new VespaStub.Answer(200, "{}", Duration.ofSeconds(5)))) {
            var task = Feed.builder().id(IdUtils.create())
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
        var task = Feed.builder().id(IdUtils.create()).endpoint(Property.ofValue(endpoint())).from(store(runContext, records).toString()).build();

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

    private static boolean preflight(VespaStub.Seen seen) {
        return seen.path().equals("/state/v1/health");
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
        return runContext.metrics().stream().filter(m -> m.getName().equals(name)).findFirst().orElseThrow(() -> new AssertionError("no metric " + name)).getValue();
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

    // a value the failed-records file can't serialize, standing in for a write that fails
    public static class Unwritable {
        public String getValue() {
            throw new IllegalStateException("cannot serialize");
        }
    }
}
