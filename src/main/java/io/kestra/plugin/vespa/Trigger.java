package io.kestra.plugin.vespa;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Start a flow when new documents match a Vespa query",
    description = """
        Runs a YQL query every `interval` and starts the flow with the documents it hasn't delivered before, as \
        `trigger.rows` (or in the file at `trigger.uri` with `fetchType=STORE`), in the same shape as the `Query` task. \
        Each document is delivered once: the trigger remembers the highest `watermarkField` value it delivered, so \
        that field must grow with every write, like the time the document was fed."""
)
@Plugin(
    examples = {
        @Example(
            title = "Log each album added to the catalog, once.",
            full = true,
            code = """
                id: vespa_new_albums
                namespace: company.team

                triggers:
                  - id: new_albums
                    type: io.kestra.plugin.vespa.Trigger
                    endpoint: "{{ secret('VESPA_ENDPOINT') }}"
                    token: "{{ secret('VESPA_TOKEN') }}"
                    yql: "select * from music where genre contains \\"jazz\\""
                    watermarkField: added_at
                    interval: PT5M

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.size }} new albums, the first is {{ trigger.rows[0].fields.album }}"
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Query.Output>, VespaConnectionInterface {
    private static final Pattern ORDER_BY = Pattern.compile("(?i)\\border\\s+by\\b");
    private static final Pattern OFFSET = Pattern.compile("(?i)\\boffset\\s+\\d+");
    private static final Pattern LIMIT = Pattern.compile("(?i)\\blimit\\s+\\d+");
    // the trigger sets the sorting, recall and hits parameters itself; an offset would skip documents
    private static final Set<String> RESERVED_PARAMETERS = Set.of("recall", "sorting", "ranking.sorting", "sorting.degrading", "hits", "count", "offset", "start");

    private Property<String> endpoint;

    @ToString.Exclude
    private Property<String> token;

    private Property<Duration> connectTimeout;

    private Property<Duration> readTimeout;

    @Schema(
        title = "YQL query",
        description = """
            Like `select * from music where genre contains "jazz"`. Each hit must carry its document ID and a \
            `watermarkField` value, as `select *` does. No `order by` (the trigger sorts by `watermarkField`), no \
            `offset` and no `limit` (set `hits` instead)."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> yql;

    @Schema(
        title = "Numeric attribute that orders the documents",
        description = """
            An `int` or `long` attribute that grows with every write, like the time the document was fed in \
            milliseconds. The watermark lives in the flow's namespace KV store under \
            `vespa_trigger_watermark_<flow id length>_<flow id>_<trigger id length>_<trigger id>`: delete that key to \
            start over."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> watermarkField;

    @Schema(
        title = "Text for `userQuery()` in the YQL"
    )
    @PluginProperty(group = "main")
    private Property<String> query;

    @Schema(
        title = "Most documents delivered by one poll",
        description = """
            Default 100, at least 1. The trigger asks Vespa for `hits` plus the documents it already delivered at the \
            watermark value, and Vespa refuses more than its `maxHits` in one answer (400 unless the application's \
            query profile raises it), so leave room: keep `hits` well under `maxHits`."""
    )
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<@Min(1) Integer> hits = Property.ofValue(100);

    @Schema(
        title = "Rank profile to rank the hits with",
        description = """
            Sent as `ranking.profile`. The hits are still delivered in `watermarkField` order."""
    )
    @PluginProperty(group = "main")
    private Property<String> ranking;

    @Schema(
        title = "Vespa's own time limit for the query"
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> queryTimeout;

    @Schema(
        title = "Other query API parameters",
        description = """
            Sent as they are, like in the `Query` task. `recall`, `sorting`, `sorting.degrading`, `hits`, `offset` and \
            their aliases are refused: the trigger sets the first ones itself, and an offset would skip documents."""
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> parameters;

    @Schema(
        title = "Result handling mode",
        description = """
            Default `FETCH`, which delivers the documents as `trigger.rows`; `STORE` delivers them in an ION file at \
            `trigger.uri`. `FETCH_ONE` and `NONE` are refused."""
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Schema(
        title = "Time between two polls",
        description = """
            Default 60 seconds."""
    )
    @Builder.Default
    @PluginProperty(group = "main")
    private Duration interval = Duration.ofSeconds(60);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var settings = settings(runContext);
        var kv = runContext.namespaceKv(context.getNamespace());
        var key = watermarkKey(context.getFlowId(), context.getTriggerId());
        var watermark = load(kv, key, context.getNamespace(), settings.field());

        var answer = search(runContext, settings, watermark);
        var delivered = newHits(runContext, answer.rows(), settings, watermark);
        if (delivered.isEmpty()) {
            return Optional.empty();
        }

        var output = Query.Output.builder().size(delivered.size()).totalCount(answer.totalCount());
        var rows = delivered.stream().map(Hit::row).toList();
        var built = settings.fetchType() == FetchType.STORE ? output.uri(Query.store(runContext, rows)).build() : output.rows(rows).build();
        var execution = TriggerService.generateExecution(this, conditionContext, context, built);

        // saved once the execution exists, so a poll that can't start one delivers the same documents next time
        save(kv, key, next(watermark, settings.field(), delivered));
        return Optional.of(execution);
    }

    private Settings settings(RunContext runContext) throws Exception {
        var rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        if (rFetchType != FetchType.FETCH && rFetchType != FetchType.STORE) {
            throw new IllegalArgumentException("The trigger supports `fetchType` FETCH and STORE, got %s.".formatted(rFetchType));
        }
        var rYql = runContext.render(yql).as(String.class).filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("The trigger property 'yql' is required; set a YQL query."));
        checkYql(rYql);
        var rParameters = runContext.render(parameters).asMap(String.class, Object.class);
        checkParameters(rParameters);
        var rField = runContext.render(watermarkField).as(String.class).filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("The trigger property 'watermarkField' is required; name a numeric attribute that grows with every write."));
        var rHits = runContext.render(hits).as(Integer.class).orElse(100);
        return new Settings(rFetchType, rYql, rParameters, rField, rHits);
    }

    private Query.Answer search(RunContext runContext, Settings settings, Watermark watermark) throws Exception {
        var vespaParameters = new HashMap<>(settings.parameters());
        vespaParameters.put("sorting", "+" + settings.field());
        // Vespa's shortcut for a fast-search sort key can answer with partial coverage, which fails the poll
        vespaParameters.put("sorting.degrading", false);
        // documents without a value can't be ordered, so they are never delivered and never fill a page (as plugin-mongodb's watermark)
        var from = watermark == null ? Long.MIN_VALUE : watermark.value();
        vespaParameters.put("recall", "+%s:[%d;]".formatted(settings.field(), from));
        // the IDs already delivered at the watermark value come back too, so they get room on top of `hits`
        var requested = requested(settings, watermark);
        runContext.logger().debug("Asking Vespa for {} hits with recall {}", requested, vespaParameters.get("recall"));
        Query.Answer answer;
        try {
            answer = Query.builder()
            .id(getId())
            .type(Query.class.getName())
            .endpoint(endpoint)
            .token(token)
            .connectTimeout(connectTimeout)
            .readTimeout(readTimeout)
            .yql(Property.ofValue(settings.yql()))
            .query(query)
            .hits(Property.ofValue(requested))
            .ranking(ranking)
            .queryTimeout(queryTimeout)
            .parameters(Property.ofValue(vespaParameters))
            .build()
            .answer(runContext, FetchType.FETCH);
        } catch (HttpClientResponseException e) {
            if (watermark != null && e.getMessage() != null && e.getMessage().contains("hits requested")) {
                throw new IllegalStateException(("%s, so the trigger asked Vespa for %d hits (`hits` %d plus those documents), more than its `maxHits` "
                    + "allows; lower `hits`, raise `maxHits` in the application's query profile, or use a finer-grained `watermarkField`.")
                    .formatted(delivered(watermark, settings.field()), requested, settings.hits()), e);
            }
            throw e;
        }
        if (!answer.errors().isEmpty() || !answer.fullCoverage()) {
            throw new IllegalStateException("Vespa searched only part of the corpus (errors: %s, full coverage: %s), so the trigger delivers nothing and tries again at the next poll."
                .formatted(answer.errors(), answer.fullCoverage()));
        }
        return answer;
    }

    private static int requested(Settings settings, Watermark watermark) {
        if (watermark == null) {
            return settings.hits();
        }
        try {
            return Math.addExact(settings.hits(), watermark.ids().size());
        } catch (ArithmeticException e) {
            throw new IllegalStateException("`hits` (%d) plus the documents already delivered is too large: %s; lower `hits`."
                .formatted(settings.hits(), delivered(watermark, settings.field())), e);
        }
    }

    private static String delivered(Watermark watermark, String field) {
        var count = watermark.ids().size();
        if (count == 1) {
            return "1 document already delivered shares %s = %d".formatted(field, watermark.value());
        }
        return "%d documents already delivered share %s = %d".formatted(count, field, watermark.value());
    }

    private static List<Hit> newHits(RunContext runContext, List<Map<String, Object>> rows, Settings settings, Watermark watermark) {
        var values = rows.stream().map(row -> value(row, settings.field())).toList();
        if (!rows.isEmpty() && values.stream().allMatch(OptionalLong::isEmpty)) {
            throw new IllegalStateException("No hit carries '%s'; select it in the YQL (`select *` or a summary that has it).".formatted(settings.field()));
        }
        var hits = new ArrayList<Hit>();
        var withoutValue = 0;
        for (var i = 0; i < rows.size(); i++) {
            var row = rows.get(i);
            var value = values.get(i);
            if (value.isEmpty()) {
                runContext.logger().warn("Skipped the document '{}': it has no value in '{}', so the trigger never delivers it.", row.get("id"), settings.field());
                withoutValue++;
            } else {
                hits.add(new Hit(row, value.getAsLong()));
            }
        }
        var deliveredIds = watermark == null ? Set.<String>of() : Set.copyOf(watermark.ids());
        var fresh = hits.stream()
            .filter(hit -> watermark == null || hit.value() != watermark.value() || !deliveredIds.contains(hit.id()))
            .toList();
        var delivered = fresh.stream().limit(settings.hits()).toList();
        runContext.logger().debug("Vespa answered {} hits: {} already delivered, {} without a value, {} left for the next poll",
            rows.size(), hits.size() - fresh.size(), withoutValue, fresh.size() - delivered.size());
        return delivered;
    }

    // the flow and trigger IDs are length-prefixed, so ("ab", "c") and ("a", "bc") can't share a key
    static String watermarkKey(String flowId, String triggerId) {
        return "vespa_trigger_watermark_" + flowId.length() + "_" + flowId + "_" + triggerId.length() + "_" + triggerId;
    }

    private static void checkYql(String rYql) {
        var clauses = withoutQuotedText(rYql);
        if (ORDER_BY.matcher(clauses).find()) {
            throw new IllegalArgumentException("The trigger's YQL can't have an 'order by': the trigger sorts by `watermarkField` to deliver each document once.");
        }
        if (OFFSET.matcher(clauses).find()) {
            throw new IllegalArgumentException("The trigger's YQL can't have an 'offset': it would skip documents.");
        }
        if (LIMIT.matcher(clauses).find()) {
            throw new IllegalArgumentException("The trigger's YQL can't have a 'limit': it would stop the trigger at documents it already delivered; set `hits` instead.");
        }
    }

    // one pass with no regex: a quote that never closes leaves the rest as is, so no input makes the check slow
    private static String withoutQuotedText(String yql) {
        var text = new StringBuilder(yql.length());
        var i = 0;
        while (i < yql.length()) {
            var c = yql.charAt(i);
            if (c != '"' && c != '\'') {
                text.append(c);
                i++;
                continue;
            }
            var end = closingQuote(yql, i);
            if (end < 0) {
                return text.append(yql, i, yql.length()).toString();
            }
            text.append("\"\"");
            i = end + 1;
        }
        return text.toString();
    }

    private static int closingQuote(String yql, int open) {
        var quote = yql.charAt(open);
        for (var i = open + 1; i < yql.length(); i++) {
            var c = yql.charAt(i);
            if (c == '\\') {
                i++;
            } else if (c == quote) {
                return i;
            }
        }
        return -1;
    }

    private static void checkParameters(Map<String, Object> rParameters) {
        Query.dottedNames("", rParameters)
            .filter(name -> RESERVED_PARAMETERS.contains(name.toLowerCase(Locale.ROOT)))
            .findFirst()
            .ifPresent(name -> {
                throw new IllegalArgumentException("The parameter '%s' can't be set on the trigger: it sets the sorting, recall and hits parameters itself, and an offset would skip documents.".formatted(name));
            });
    }

    private static OptionalLong value(Map<String, Object> row, String field) {
        var id = (String) row.get("id");
        if (id.startsWith("group:")) {
            throw new IllegalStateException("The trigger's YQL has a grouping: the trigger delivers documents, not grouping results; remove the grouping (the part after `|`).");
        }
        if (!id.startsWith("id:")) {
            throw new IllegalStateException("The trigger needs each hit's document ID, but Vespa answered '%s'; select it with `select *` or a summary that has `documentid`.".formatted(id));
        }
        var fields = (Map<?, ?>) row.get("fields");
        var value = fields == null ? null : fields.get(field);
        if (value == null) {
            return OptionalLong.empty();
        }
        if (!(value instanceof Integer) && !(value instanceof Long)) {
            throw new IllegalStateException("The hit '%s' has no whole-number value in '%s' (got %s); select the field in the YQL and make it an `int` or `long` attribute.".formatted(id, field, value));
        }
        return OptionalLong.of(((Number) value).longValue());
    }

    private static Watermark load(KVStore kv, String key, String namespace, String field) throws Exception {
        var stored = kv.getValue(key);
        if (stored.isEmpty()) {
            return null;
        }
        Watermark watermark;
        try {
            watermark = JacksonMapper.ofJson().readValue((String) stored.get().value(), Watermark.class);
        } catch (Exception e) {
            throw new IllegalStateException(unreadable(key, namespace), e);
        }
        if (watermark.field() == null || watermark.value() == null || watermark.ids() == null) {
            throw new IllegalStateException(unreadable(key, namespace));
        }
        // another field's values say nothing about this one's
        return watermark.field().equals(field) ? watermark : null;
    }

    private static String unreadable(String key, String namespace) {
        return "Could not read the Vespa trigger watermark in the KV key '%s' of namespace '%s'. Delete that KV entry to start again from the first matching document.".formatted(key, namespace);
    }

    private static Watermark next(Watermark previous, String field, List<Hit> delivered) {
        var highest = Long.MIN_VALUE;
        for (var hit : delivered) {
            highest = Math.max(highest, hit.value());
        }
        var ids = new LinkedHashSet<String>();
        if (previous != null && previous.value() == highest) {
            ids.addAll(previous.ids());
        }
        for (var hit : delivered) {
            if (hit.value() == highest) {
                ids.add(hit.id());
            }
        }
        return new Watermark(field, highest, List.copyOf(ids));
    }

    private static void save(KVStore kv, String key, Watermark watermark) throws Exception {
        kv.put(key, new KVValueAndMetadata(null, JacksonMapper.ofJson().writeValueAsString(watermark)));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Watermark(String field, Long value, List<String> ids) {
    }

    private record Settings(FetchType fetchType, String yql, Map<String, Object> parameters, String field, int hits) {
    }

    private record Hit(Map<String, Object> row, long value) {
        String id() {
            return (String) row.get("id");
        }
    }
}
