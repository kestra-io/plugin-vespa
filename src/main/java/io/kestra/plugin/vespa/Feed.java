package io.kestra.plugin.vespa;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.FeedClientBuilder;
import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Rethrow;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Feed documents to Vespa",
    description = """
        Writes, updates or removes many documents through Vespa's feed client, which retries throttled and unavailable \
        answers and adapts how many operations run at once. Each record is a map with `id` and, except for `REMOVE`, \
        `fields`. Records Vespa refuses fail the task, or with `failOnError: false` end it in `WARNING` with the failed \
        records in an ION file. The feed client has its own HTTP stack: `connectTimeout` is unused (the client waits \
        30 seconds to connect), and `endpoint` must have no path (only scheme, host and port), since the client sends \
        to the host's root. Before feeding, `Feed` sends one `HEAD /state/v1/health` through Kestra's HTTP \
        client, so Kestra's allowed and denied hosts apply to it too."""
)
@Plugin(
    metrics = {
        @Metric(name = "records.succeeded", type = Counter.TYPE, unit = "records", description = "Records Vespa applied"),
        @Metric(name = "records.failed", type = Counter.TYPE, unit = "records", description = "Records refused, invalid or timed out")
    },
    examples = {
        @Example(
            title = "Feed albums written by a previous task, keyed by their name.",
            full = true,
            code = """
                id: vespa_feed
                namespace: company.team

                tasks:
                  - id: write
                    type: io.kestra.plugin.core.storage.Write
                    extension: .ion
                    content: |
                      {id:"kind-of-blue",fields:{artist:"Miles Davis",album:"Kind of Blue",year:1959}}
                      {id:"a-love-supreme",fields:{artist:"John Coltrane",album:"A Love Supreme",year:1965}}

                  - id: feed
                    type: io.kestra.plugin.vespa.Feed
                    endpoint: "{{ secret('VESPA_ENDPOINT') }}"
                    token: "{{ secret('VESPA_TOKEN') }}"
                    from: "{{ outputs.write.uri }}"
                    namespace: mynamespace
                    documentType: music
                """
        ),
        @Example(
            title = "Update one field of many documents and keep going when some fail.",
            full = true,
            code = """
                id: vespa_feed_update
                namespace: company.team

                tasks:
                  - id: update
                    type: io.kestra.plugin.vespa.Feed
                    endpoint: "{{ secret('VESPA_ENDPOINT') }}"
                    token: "{{ secret('VESPA_TOKEN') }}"
                    operation: UPDATE
                    failOnError: false
                    from:
                      - id: "id:mynamespace:music::kind-of-blue"
                        fields:
                          genre:
                            assign: jazz
                      - id: "id:mynamespace:music::a-love-supreme"
                        fields:
                          genre:
                            assign: jazz

                  - id: log_failures
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.update.failureCount }} updates failed, see {{ outputs.update.uri ?? 'nothing' }}"
                """
        ),
        @Example(
            title = "Upsert documents, creating the ones that don't exist yet, and update only those still in stock.",
            full = true,
            code = """
                id: vespa_feed_upsert
                namespace: company.team

                tasks:
                  - id: upsert
                    type: io.kestra.plugin.vespa.Feed
                    endpoint: "{{ secret('VESPA_ENDPOINT') }}"
                    token: "{{ secret('VESPA_TOKEN') }}"
                    operation: UPDATE
                    createIfNotExists: true
                    namespace: mynamespace
                    documentType: music
                    from:
                      - id: kind-of-blue
                        fields:
                          stock:
                            assign: 3

                  - id: reprice
                    type: io.kestra.plugin.vespa.Feed
                    endpoint: "{{ secret('VESPA_ENDPOINT') }}"
                    token: "{{ secret('VESPA_TOKEN') }}"
                    operation: UPDATE
                    condition: "music.stock > 0"
                    failOnError: false
                    namespace: mynamespace
                    documentType: music
                    from:
                      - id: kind-of-blue
                        fields:
                          price:
                            assign: 9.99
                """
        )
    }
)
public class Feed extends AbstractVespaTask implements RunnableTask<Feed.Output>, Data.From {
    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION + """

            Each record is a map with `id` and `fields`; `REMOVE` needs only `id`, and other keys are ignored. Kestra \
            reads a `kestra://` file whole into memory before the first record is sent, so split very large files \
            across several runs.""",
        anyOf = {String.class, List.class, Map.class}
    )
    @NotNull
    @PluginProperty(dynamic = true, group = "main")
    private Object from;

    @Schema(
        title = "What to do with each record",
        description = """
            `PUT` (default) writes the whole document, replacing one with the same ID. `UPDATE` changes fields of an \
            existing document, with `fields` in Vespa's update format, like `{"album": {"assign": "Kind of Blue"}}`; \
            Vespa accepts an update of a missing document and changes nothing, so it counts as a success; set \
            `createIfNotExists: true` to create it instead. A retried \
            `UPDATE` with `increment` applies twice. \
            `REMOVE` deletes the document; removing a missing one succeeds."""
    )
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<Operation> operation = Property.ofValue(Operation.PUT);

    @Schema(
        title = "Vespa namespace for record keys",
        description = """
            With `documentType`, completes a record `id` that doesn't start with `id:` into \
            `id:<namespace>:<documentType>::<id>`. An `id` that starts with `id:` is used as is."""
    )
    @PluginProperty(group = "destination")
    private Property<String> namespace;

    @Schema(
        title = "Vespa document type for record keys",
        description = """
            With `namespace`, completes a record `id` that doesn't start with `id:`. An `id` that starts with `id:` is \
            used as is."""
    )
    @PluginProperty(group = "destination")
    private Property<String> documentType;

    @Schema(
        title = "Fail the task when a record fails",
        description = """
            Default `true`: the good records are still written, then the task fails naming the first failed ones. \
            `false` ends the task in `WARNING` with the failed records in the ION file `uri`."""
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> failOnError = Property.ofValue(true);

    @Schema(
        title = "Create the document when it doesn't exist",
        description = """
            `true` makes an `UPDATE` of a missing document create it first (an upsert). With `condition`, a `PUT` or \
            `UPDATE` of a missing document is written without checking the condition. No effect on `REMOVE`, or on `PUT` \
            without `condition`. Default `false`."""
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> createIfNotExists = Property.ofValue(false);

    @Schema(
        title = "Write each record only if its document matches",
        description = """
            Vespa's test-and-set condition, in its document selection language with the document type before the \
            field, like `music.year==2000`. A record whose document doesn't match, or doesn't exist, is a failed record \
            (see `failOnError`), unless `createIfNotExists` writes the missing document. One condition for the whole run."""
    )
    @PluginProperty(group = "reliability")
    private Property<String> condition;

    @Schema(
        title = "Most records waiting for Vespa's answer at one time",
        description = """
            From 1 to 4096, default 4096 (the feed client's own ceiling). The client still adapts how many it sends \
            below this value; lower it to put less load on Vespa. It doesn't change how much of `from` the worker \
            holds in memory."""
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<@Min(1) @Max(4096) Integer> maxInFlight = Property.ofValue(4096);

    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicReference<Runnable> killable = new AtomicReference<>();

    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicBoolean isKilled = new AtomicBoolean(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rOperation = rendered("operation", "set it to PUT, UPDATE or REMOVE", () -> runContext.render(operation).as(Operation.class).orElse(Operation.PUT));
        var rFailOnError = rendered("failOnError", "set it to true or false", () -> runContext.render(failOnError).as(Boolean.class).orElse(true));
        var rNamespace = runContext.render(namespace).as(String.class).orElse(null);
        var rDocumentType = runContext.render(documentType).as(String.class).orElse(null);
        var rCreateIfNotExists = rendered("createIfNotExists", "set it to true or false", () -> runContext.render(createIfNotExists).as(Boolean.class).orElse(false));
        var rMaxInFlight = rendered("maxInFlight", "set it to a number from 1 to 4096", () -> runContext.render(maxInFlight).as(Integer.class).orElse(4096));
        var rCondition = runContext.render(condition).as(String.class).orElse(null);
        var parameters = OperationParameters.empty().timeout(runContext.render(getReadTimeout()).as(Duration.class).orElse(Duration.ofMinutes(5))).createIfNonExistent(rCreateIfNotExists);
        if (rCondition != null) {
            if (rCondition.isBlank()) {
                throw new IllegalArgumentException("The `condition` property is blank; remove it, or give a Vespa document selection, like music.year==2000.");
            }
            parameters = parameters.testAndSetCondition(rCondition);
        }
        var rToken = runContext.render(getToken()).as(String.class).orElse("");
        var rEndpoint = endpoint(runContext);
        if (!URI.create(rEndpoint).getRawPath().isEmpty()) {
            throw new IllegalArgumentException("`Feed` can't send under a path, so its `endpoint` must be only scheme, host and port, like http://localhost:8080, got: '%s'.".formatted(rEndpoint));
        }

        preflight(runContext);
        if (isKilled.get()) {
            return Output.builder().successCount(0L).failureCount(0L).build();
        }

        var failedFile = runContext.workingDir().createTempFile(".ion");
        var failures = new Failures(new BufferedOutputStream(Files.newOutputStream(failedFile), FileSerde.BUFFER_SIZE));
        var succeeded = new AtomicLong();
        // the client holds a caller back at its own limit, except for repeated operations on one document ID, which it queues without limit
        var inFlight = new Semaphore(rMaxInFlight);
        var client = client(rEndpoint, rToken);
        // the client waits up to a minute for its result threads; a kill must return at once
        killable.set(() -> Thread.ofVirtual().start(() -> client.close(false)));
        // closing the stream cancels the read, so an early exit doesn't leave the storage file open
        try (failures; var records = Data.from(from).read(runContext).toStream()) {
            for (var record : (Iterable<Map<String, Object>>) records::iterator) {
                if (isKilled.get() || failures.writeError().isPresent()) {
                    break;
                }
                inFlight.acquire();
                CompletableFuture<Result> sent;
                try {
                    sent = send(client, rOperation, record, rNamespace, rDocumentType, parameters);
                } catch (IllegalArgumentException invalid) {
                    inFlight.release();
                    failures.add(record, invalid.getMessage());
                    continue;
                } catch (IllegalStateException closed) {
                    inFlight.release();
                    if (isKilled.get()) {
                        break;
                    }
                    throw closed;
                }
                sent.whenComplete((result, thrown) -> {
                    try {
                        if (thrown == null && result.type() == Result.Type.success) {
                            succeeded.incrementAndGet();
                        } else {
                            failures.add(record, thrown == null ? refused(result) : error(thrown));
                        }
                    } finally {
                        inFlight.release();
                    }
                });
            }
            inFlight.acquire(rMaxInFlight);
        } finally {
            client.close(!isKilled.get());
        }

        var writeError = failures.writeError();
        if (writeError.isPresent()) {
            throw new IllegalStateException("Can't write the failed records to the worker's temporary file; check the worker's free disk space.", writeError.get());
        }
        runContext.metric(Counter.of("records.succeeded", succeeded.get()));
        runContext.metric(Counter.of("records.failed", failures.count()));
        runContext.logger().info("Feed done: {} succeeded, {} failed.", succeeded.get(), failures.count());
        if (failures.count() == 0) {
            return Output.builder().successCount(succeeded.get()).failureCount(0L).build();
        }
        if (rFailOnError) {
            var what = " records were fed and %d failed: ".formatted(failures.count());
            var fix = "; set `failOnError: false` to keep the good records and get the failed ones in a file.";
            var first = StringUtils.abbreviate(String.join("; ", failures.first()), 1000 - fix.length() - what.length() - 20);
            throw new IllegalStateException(succeeded.get() + what + first + fix);
        }
        runContext.logger().warn("{} records were fed and {} failed; the failed records are in `uri`.", succeeded.get(), failures.count());
        return Output.builder()
            .successCount(succeeded.get())
            .failureCount(failures.count())
            .uri(runContext.storage().putFile(failedFile.toFile()))
            .build();
    }

    @Override
    public void kill() {
        if (isKilled.compareAndSet(false, true)) {
            Optional.ofNullable(killable.get()).ifPresent(Runnable::run);
        }
    }

    // the feed client has its own HTTP stack, so one request through Kestra's client applies the 2.0 allowed and denied hosts first
    private void preflight(RunContext runContext) throws IllegalVariableEvaluationException {
        var timeout = TimeoutConfiguration.builder()
            .connectTimeout(Property.ofValue(Duration.ofSeconds(5)))
            .readIdleTimeout(Property.ofValue(Duration.ofSeconds(5)))
            .build();
        try (var client = client(runContext, timeout)) {
            Runnable close = () -> {
                try {
                    client.close();
                } catch (IOException ignored) {
                    // the request it ends fails, which is all that's needed
                }
            };
            // core limits only the connection and each read, so a slow answer would hold the run without end
            var deadline = Thread.ofVirtual().start(() -> {
                try {
                    Thread.sleep(Duration.ofSeconds(10));
                    close.run();
                } catch (InterruptedException done) {
                    // the request ended in time
                }
            });
            killable.set(() -> Thread.ofVirtual().start(close));
            // a kill that came before the line above found nothing to close
            if (isKilled.get()) {
                deadline.interrupt();
                return;
            }
            try {
                request(runContext, client, HttpRequest.builder().uri(URI.create("/state/v1/health")).method("HEAD").build());
            } finally {
                deadline.interrupt();
            }
        } catch (IllegalArgumentException refused) {
            throw refused;
        } catch (Exception ignored) {
            // an endpoint that is down or isn't Vespa is reported by the feed client's handshake, with its own message
        }
    }

    private static FeedClient client(String rEndpoint, String rToken) {
        var builder = FeedClientBuilder.create(URI.create(rEndpoint + "/"));
        if (!rToken.isBlank()) {
            builder.addRequestHeader("Authorization", "Bearer " + rToken);
        }
        try {
            return builder.build();
        } catch (FeedException e) {
            if (String.valueOf(e.getMessage()).startsWith("server responded non-OK to handshake")) {
                throw (IllegalStateException) wrongAnswer("feed handshake is an error", e.getMessage()).initCause(e);
            }
            throw new IllegalStateException("Vespa can't be reached at `endpoint` %s: %s".formatted(rEndpoint, StringUtils.abbreviate(e.getMessage(), 1000)), e);
        }
    }

    private static CompletableFuture<Result> send(FeedClient client, Operation rOperation, Map<String, Object> record, String rNamespace, String rDocumentType,
                                                  OperationParameters parameters) throws Exception {
        var id = documentId(record.get("id"), rNamespace, rDocumentType);
        if (rOperation == Operation.REMOVE) {
            return client.remove(id, parameters);
        }
        if (!(record.get("fields") instanceof Map<?, ?> fields)) {
            throw new IllegalArgumentException("The record '%s' has no `fields` map; a %s needs one, like fields: {album: Kind of Blue}.".formatted(id, rOperation));
        }
        var body = JacksonMapper.ofJson().writeValueAsString(Map.of("fields", fields));
        return rOperation == Operation.PUT ? client.put(id, body, parameters) : client.update(id, body, parameters);
    }

    private static DocumentId documentId(Object id, String rNamespace, String rDocumentType) {
        if (id == null) {
            throw new IllegalArgumentException("The record has no `id`; set it to a full document ID, like id:mynamespace:music::kind-of-blue, or to a key with `namespace` and `documentType` set.");
        }
        if (!(id instanceof String text)) {
            throw new IllegalArgumentException("The record `id` must be text, like id:mynamespace:music::kind-of-blue, got: %s.".formatted(id));
        }
        var full = text;
        if (!text.startsWith("id:")) {
            if (rNamespace == null || rDocumentType == null) {
                throw new IllegalArgumentException("The record `id` '%s' is a key, not a full document ID; set `namespace` and `documentType`, or give the full ID, like id:mynamespace:music::%s.".formatted(text, text));
            }
            full = "id:%s:%s::%s".formatted(rNamespace, rDocumentType, text);
        }
        documentPath(full);
        return DocumentId.of(full);
    }

    private static String refused(Result result) {
        var message = result.resultMessage().orElse(result.type().name());
        return result.type() == Result.Type.conditionNotMet ? "The `condition` was not met, or the document doesn't exist: " + message : message;
    }

    // the client gives up when Vespa's own limit ends, so Vespa's answer can be lost while the write landed
    private static String error(Throwable thrown) {
        var cause = ExceptionUtils.getRootCause(thrown);
        var message = Optional.ofNullable(thrown.getMessage()).orElse(cause.toString());
        var idPrefix = thrown instanceof FeedException feedException ? feedException.documentId().map(id -> "(" + id + ") ").orElse("") : "";
        if (!idPrefix.isEmpty() && message.startsWith(idPrefix)) {
            message = message.substring(idPrefix.length());
        }
        return cause instanceof TimeoutException ? message + "; Vespa may still have applied it" : message;
    }

    private static <T> T rendered(String property, String fix, Rethrow.SupplierChecked<T, Exception> render) {
        try {
            return render.get();
        } catch (Exception e) {
            throw new IllegalArgumentException("The `%s` property can't be read; %s.".formatted(property, fix), e);
        }
    }

    // the callbacks of the client's threads write here, one at a time; a write error is kept, not thrown on their thread
    static final class Failures implements AutoCloseable {
        private final OutputStream file;
        private final List<String> first = new ArrayList<>();
        private long count;
        private Exception writeError;

        Failures(OutputStream file) {
            this.file = file;
        }

        synchronized void add(Map<String, Object> record, String error) {
            count++;
            if (first.size() < 10) {
                first.add("%s: %s".formatted(record.get("id"), error));
            }
            if (writeError != null) {
                return;
            }
            try {
                FileSerde.write(file, Map.of("record", record, "error", error));
            } catch (Exception e) {
                writeError = e;
            }
        }

        synchronized Optional<Exception> writeError() {
            return Optional.ofNullable(writeError);
        }

        synchronized long count() {
            return count;
        }

        synchronized List<String> first() {
            return List.copyOf(first);
        }

        @Override
        public synchronized void close() {
            try {
                file.close();
            } catch (Exception e) {
                // a full disk fails again here, on the buffered bytes; the first error is the one to report
                if (writeError == null) {
                    writeError = e;
                }
            }
        }
    }

    public enum Operation {
        PUT,
        UPDATE,
        REMOVE
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of records Vespa applied")
        private Long successCount;

        @Schema(
            title = "Number of records that failed",
            description = """
                Refused by Vespa, invalid (no usable `id`, no `fields`) or out of time. A record that ran out of time \
                may still have been applied by Vespa."""
        )
        private Long failureCount;

        @Schema(
            title = "URI of the failed records",
            description = """
                An ION file in Kestra's internal storage, one row per failed record: `record` (the record as given) \
                and `error`. To feed them again, read each row's `record`. Null when nothing failed."""
        )
        private URI uri;

        @Override
        public Optional<State.Type> finalState() {
            return failureCount > 0 ? Optional.of(State.Type.WARNING) : Optional.empty();
        }
    }
}
