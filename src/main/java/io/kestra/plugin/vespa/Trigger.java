package io.kestra.plugin.vespa;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.metrics.Counter;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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
        Each document is delivered once: the trigger keeps the highest `watermarkField` value it delivered, and the \
        document IDs at that value, in the namespace KV store, and asks Vespa only for documents at or above it, oldest \
        first. A document written later with a lower `watermarkField` value is never delivered, so the field must grow \
        with every write, like the time the document was fed. The first poll delivers what already matches, one page of \
        `hits` per poll."""
)
@Plugin(
    metrics = {
        @Metric(name = "records", type = Counter.TYPE, unit = "records", description = "Number of hits in Vespa's answer to one poll, the ones already delivered included")
    },
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
    // the first three the trigger sets itself, the others would skip documents
    private static final Set<String> RESERVED_PARAMETERS = Set.of("recall", "sorting", "ranking.sorting", "hits", "count", "offset", "start");

    private Property<String> endpoint;

    @ToString.Exclude
    private Property<String> token;

    private Property<Duration> connectTimeout;

    private Property<Duration> readTimeout;

    @Schema(
        title = "YQL query",
        description = """
            Like `select * from music where genre contains "jazz"`. Each hit must carry its document ID and a \
            `watermarkField` value, as `select *` does. No `order by` (the trigger sorts by `watermarkField`) and no \
            `offset`; a `limit` makes the page smaller."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> yql;

    @Schema(
        title = "Numeric attribute that orders the documents",
        description = """
            A whole-number attribute of the schema that grows with every write, like the time the document was fed. \
            Changing it starts over from the lowest value of the new field."""
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
            Default 100. Vespa refuses more than 400 in one answer unless the application raises `maxHits` in its query \
            profile, and the trigger also asks for the documents it already delivered at the watermark value."""
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
            Sent as they are, like in the `Query` task. `recall`, `sorting`, `hits`, `offset` and their aliases are \
            refused: the trigger sets the first ones itself, and an offset would skip documents."""
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> parameters;

    @Schema(
        title = "Result handling mode",
        description = """
            `FETCH` delivers the documents as `trigger.rows`, `STORE` in an ION file at `trigger.uri`."""
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Schema(title = "Time between two polls")
    @Builder.Default
    @PluginProperty(group = "main")
    private Duration interval = Duration.ofSeconds(60);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        if (rFetchType != FetchType.FETCH && rFetchType != FetchType.STORE) {
            throw new IllegalArgumentException("The trigger supports `fetchType` FETCH and STORE, got %s.".formatted(rFetchType));
        }
        var rYql = runContext.render(yql).as(String.class).orElseThrow(() -> new IllegalArgumentException("The trigger property 'yql' is required; set a YQL query."));
        checkYql(rYql);
        var rParameters = runContext.render(parameters).asMap(String.class, Object.class);
        checkParameters(rParameters);
        var rField = runContext.render(watermarkField).as(String.class).filter(field -> !field.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("The trigger property 'watermarkField' is required; name a numeric attribute that grows with every write."));
        var rHits = runContext.render(hits).as(Integer.class).orElse(100);

        var kv = runContext.namespaceKv(context.getNamespace());
        var key = watermarkKey(context.getFlowId(), context.getTriggerId());
        var watermark = load(kv, key, context.getNamespace(), rField);
        if (watermark != null && watermark.ids().size() >= rHits) {
            throw tooManyAtOneValue(rField, watermark.value(), rHits);
        }

        var requested = rHits + (watermark == null ? 0 : watermark.ids().size());
        var vespaParameters = new HashMap<>(rParameters);
        vespaParameters.put("sorting", "+" + rField);
        if (watermark != null) {
            vespaParameters.put("recall", "+%s:[%d;]".formatted(rField, watermark.value()));
        }
        var answer = Query.builder()
            .id(getId())
            .type(Query.class.getName())
            .endpoint(endpoint)
            .token(token)
            .connectTimeout(connectTimeout)
            .readTimeout(readTimeout)
            .yql(Property.ofValue(rYql))
            .query(query)
            .hits(Property.ofValue(requested))
            .ranking(ranking)
            .queryTimeout(queryTimeout)
            .parameters(Property.ofValue(vespaParameters))
            .build()
            .run(runContext);

        var rows = answer.getRows();
        var values = rows.stream().map(row -> value(row, rField)).toList();
        if (rows.size() == requested && !values.isEmpty() && values.getFirst().equals(values.getLast())) {
            throw tooManyAtOneValue(rField, values.getFirst(), rHits);
        }

        var delivered = new ArrayList<Map<String, Object>>();
        var deliveredValues = new ArrayList<Long>();
        for (var i = 0; i < rows.size(); i++) {
            var id = (String) rows.get(i).get("id");
            if (watermark == null || !values.get(i).equals(watermark.value()) || !watermark.ids().contains(id)) {
                delivered.add(rows.get(i));
                deliveredValues.add(values.get(i));
            }
        }
        runContext.logger().debug("Vespa answered {} hits, {} of them already delivered", rows.size(), rows.size() - delivered.size());
        if (delivered.isEmpty()) {
            return Optional.empty();
        }

        var output = Query.Output.builder().size(delivered.size()).totalCount(answer.getTotalCount());
        var built = rFetchType == FetchType.STORE ? output.uri(Query.store(runContext, delivered)).build() : output.rows(delivered).build();
        var execution = TriggerService.generateExecution(this, conditionContext, context, built);

        // saved once the execution exists, so a poll that can't start one delivers the same documents next time
        save(kv, key, next(watermark, rField, delivered, deliveredValues));
        return Optional.of(execution);
    }

    // the flow and trigger IDs are length-prefixed, so ("ab", "c") and ("a", "bc") can't share a key
    static String watermarkKey(String flowId, String triggerId) {
        return "vespa_trigger_watermark_" + flowId.length() + "_" + flowId + "_" + triggerId.length() + "_" + triggerId;
    }

    private static void checkYql(String rYql) {
        if (ORDER_BY.matcher(rYql).find()) {
            throw new IllegalArgumentException("The trigger's YQL can't have an `order by`: the trigger sorts by `watermarkField` to deliver each document once.");
        }
        if (OFFSET.matcher(rYql).find()) {
            throw new IllegalArgumentException("The trigger's YQL can't have an `offset`: it would skip documents.");
        }
    }

    private static void checkParameters(Map<String, Object> rParameters) {
        dottedNames("", rParameters)
            .filter(name -> RESERVED_PARAMETERS.contains(name.toLowerCase(Locale.ROOT)))
            .findFirst()
            .ifPresent(name -> {
                throw new IllegalArgumentException("The parameter '%s' can't be set on the trigger: it sets `recall`, `sorting` and `hits` itself, and an offset would skip documents.".formatted(name));
            });
    }

    private static Stream<String> dottedNames(String prefix, Map<?, ?> parameters) {
        return parameters.entrySet().stream().flatMap(entry -> entry.getValue() instanceof Map<?, ?> nested
            ? dottedNames(prefix + entry.getKey() + ".", nested)
            : Stream.of(prefix + entry.getKey()));
    }

    private static long value(Map<String, Object> row, String field) {
        var id = (String) row.get("id");
        if (!id.startsWith("id:")) {
            throw new IllegalStateException("The trigger needs each hit's document ID, but Vespa answered '%s'; select it with `select *` or a summary that has `documentid`.".formatted(id));
        }
        var fields = (Map<?, ?>) row.get("fields");
        var value = fields == null ? null : fields.get(field);
        if (!(value instanceof Integer) && !(value instanceof Long)) {
            throw new IllegalStateException("The hit '%s' has no whole-number value in '%s'; select the field in the YQL and make it a numeric attribute.".formatted(id, field));
        }
        return ((Number) value).longValue();
    }

    private static IllegalStateException tooManyAtOneValue(String field, long value, int hits) {
        return new IllegalStateException("More documents share %s = %d than one poll can page through with hits = %d; raise `hits`.".formatted(field, value, hits));
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

    private static Watermark next(Watermark previous, String field, List<Map<String, Object>> delivered, List<Long> values) {
        // a primitive, so the == below compares values, not Long objects
        long highest = Collections.max(values);
        var ids = new LinkedHashSet<String>();
        if (previous != null && previous.value() == highest) {
            ids.addAll(previous.ids());
        }
        for (var i = 0; i < delivered.size(); i++) {
            if (values.get(i) == highest) {
                ids.add((String) delivered.get(i).get("id"));
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
}
