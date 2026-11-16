package io.kestra.plugin.vespa;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query Vespa with YQL",
    description = """
        Runs one YQL query on the Vespa query API and returns the hits of that one answer as rows, in Vespa's own hit \
        shape: `id`, `relevance` and the document's summary fields under `fields`, read like `rows[0].fields.album`. \
        With `fetchType=STORE` the rows go to an ION file in Kestra's internal storage instead, and the outputs carry its \
        `uri`. Default `fetchType=FETCH`."""
)
@Plugin(
    metrics = {
        @Metric(name = "records", type = Counter.TYPE, unit = "records", description = "Number of rows in the outputs or in the stored file")
    },
    examples = {
        @Example(
            title = "Search albums by text and log how many matched.",
            full = true,
            code = """
                id: vespa_query
                namespace: company.team

                inputs:
                  - id: text
                    type: STRING
                    defaults: blue

                tasks:
                  - id: search
                    type: io.kestra.plugin.vespa.Query
                    endpoint: "{{ secret('VESPA_ENDPOINT') }}"
                    token: "{{ secret('VESPA_TOKEN') }}"
                    yql: "select * from music where userQuery()"
                    query: "{{ inputs.text }}"
                    hits: 5

                  - id: log_hits
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.search.size }} of {{ outputs.search.totalCount }} albums, first: {{ outputs.search.rows[0].fields.album }}"
                """
        ),
        @Example(
            title = "Find the nearest neighbours of a vector with a rank profile.",
            full = true,
            code = """
                id: vespa_nearest_neighbours
                namespace: company.team

                tasks:
                  - id: nearest
                    type: io.kestra.plugin.vespa.Query
                    endpoint: "{{ secret('VESPA_ENDPOINT') }}"
                    token: "{{ secret('VESPA_TOKEN') }}"
                    yql: "select * from music where {targetHits:3}nearestNeighbor(embedding, q)"
                    ranking: closeness
                    parameters:
                      input.query(q): [0, 0, 1, 0]
                    fetchType: FETCH_ONE

                  - id: log_closest
                    type: io.kestra.plugin.core.log.Log
                    message: "Closest album: {{ outputs.nearest.row.fields.album }}"
                """
        )
    }
)
public class Query extends AbstractVespaTask implements RunnableTask<Query.Output> {
    @Schema(
        title = "YQL query",
        description = """
            Like `select * from music where genre contains "jazz"`."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> yql;

    @Schema(
        title = "Text for `userQuery()` in the YQL",
        description = """
            Unset, a YQL that uses `userQuery()` fails with Vespa's "No query"."""
    )
    @PluginProperty(group = "main")
    private Property<String> query;

    @Schema(
        title = "Number of hits to return in one answer",
        description = """
            Unset, Vespa returns 10 hits with `fetchType=FETCH` or `STORE`; the task asks for 1 with `FETCH_ONE` and 0 with `NONE`. \
            Vespa refuses more than 400 unless the application raises `maxHits` in its query profile."""
    )
    @PluginProperty(group = "main")
    private Property<@Min(0) Integer> hits;

    @Schema(
        title = "Number of hits to skip",
        description = """
            Default 0. Vespa refuses more than 1000 unless the application raises `maxOffset` in its query profile."""
    )
    @PluginProperty(group = "main")
    private Property<@Min(0) Integer> offset;

    @Schema(
        title = "Rank profile to rank the hits with",
        description = """
            Sent as `ranking.profile`. Unset, Vespa ranks with the `default` profile."""
    )
    @PluginProperty(group = "main")
    private Property<String> ranking;

    @Schema(
        title = "Vespa's own time limit for the query",
        description = """
            Unset, Vespa's default is 0.5 seconds. The task's `timeout` bounds the whole task; this one is sent to Vespa."""
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> queryTimeout;

    @Schema(
        title = "Other query API parameters",
        description = """
            Sent as they are, flat or nested, like `input.query(q)` for a query tensor, \
            `presentation.format.tensors: short-value` or `ranking: {matchPhase: {maxHits: 100}}`. A parameter also set \
            by a property of this task, by name or by Vespa's alias (`count`, `start`, `ranking.profile`, \
            `model.queryString`), in any case and flat or nested, fails the task."""
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> parameters;

    @Schema(
        title = "Result handling mode",
        description = """
            `FETCH` returns the hits of one answer as `rows` (as many as `hits`, Vespa's default 10), `FETCH_ONE` the \
            first hit as `row`, `STORE` writes the hits of one answer to an ION file in Kestra's internal storage and \
            returns its `uri`, `NONE` only `totalCount`."""
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.FETCH);

        var request = HttpRequest.builder()
            .uri(URI.create("/search/"))
            .method("POST")
            .body(HttpRequest.JsonRequestBody.of(body(runContext, rFetchType)))
            .build();
        String answer;
        try (var client = client(runContext)) {
            answer = request(runContext, client, request).getBody();
        }
        if (answer == null || answer.isBlank()) {
            throw new IllegalStateException("Vespa sent an empty answer to the query; check that `endpoint` is the URL of the Vespa container cluster.");
        }

        var root = root(answer);
        // degraded coverage (like a soft timeout) still returns usable hits, so Vespa's errors are shown, not thrown
        root.path("errors").forEach(error -> runContext.logger().warn("Vespa answered with an error: {}: {}", error.path("summary").asText(), error.path("message").asText()));

        var rows = rows(root);
        var output = Output.builder().totalCount(root.at("/fields/totalCount").asLong());
        var built = switch (rFetchType) {
            case FETCH -> output.rows(rows).size(rows.size()).build();
            case FETCH_ONE -> output.row(rows.isEmpty() ? null : rows.getFirst()).size(Math.min(rows.size(), 1)).build();
            case STORE -> output.uri(store(runContext, rows)).size(rows.size()).build();
            default -> output.build();
        };
        runContext.metric(Counter.of("records", built.getSize() == null ? 0 : built.getSize()));
        return built;
    }

    private static URI store(RunContext runContext, List<Map<String, Object>> rows) throws IOException {
        var file = runContext.workingDir().createTempFile(".ion").toFile();
        // one stream, not FileSerde.write per row: on Kestra 2.0 each write is a whole binary ION document (kestra-io/kestra#22557)
        try (var output = new BufferedOutputStream(new FileOutputStream(file), FileSerde.BUFFER_SIZE)) {
            FileSerde.writeAll(output, Flux.fromIterable(rows)).block();
        }
        return runContext.storage().putFile(file);
    }

    private static JsonNode root(String answer) {
        JsonNode json;
        try {
            json = JacksonMapper.ofJson().readTree(answer);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Vespa's answer to the query isn't JSON; check that `endpoint` is the URL of the Vespa container cluster. Answer: %s".formatted(StringUtils.abbreviate(answer, 1000)), e);
        }
        if (!json.path("root").isObject()) {
            throw new IllegalStateException("Vespa's answer to the query has no root; check that `endpoint` is the URL of the Vespa container cluster. Answer: %s".formatted(StringUtils.abbreviate(answer, 1000)));
        }
        return json.get("root");
    }

    private Map<String, Object> body(RunContext runContext, FetchType rFetchType) throws Exception {
        var body = new LinkedHashMap<String, Object>();
        body.put("yql", runContext.render(yql).as(String.class).orElseThrow(() -> new IllegalArgumentException("The task property 'yql' is required; set a YQL query.")));
        runContext.render(query).as(String.class).ifPresent(v -> body.put("query", v));
        runContext.render(hits).as(Integer.class).ifPresent(v -> body.put("hits", v));
        runContext.render(offset).as(Integer.class).ifPresent(v -> body.put("offset", v));
        runContext.render(ranking).as(String.class).ifPresent(v -> body.put("ranking.profile", v));
        runContext.render(queryTimeout).as(Duration.class).ifPresent(v -> body.put("timeout", v.toMillis() + "ms"));

        // Vespa's names and aliases for what the task properties set, lower-cased since Vespa reads names in any case
        var properties = Map.of(
            "yql", "yql", "query", "query", "model.querystring", "query", "hits", "hits", "count", "hits",
            "offset", "offset", "start", "offset", "ranking", "ranking", "ranking.profile", "ranking", "timeout", "queryTimeout"
        );
        var setByProperties = Set.copyOf(body.keySet().stream().map(properties::get).toList());

        var rParameters = runContext.render(parameters).asMap(String.class, Object.class);
        var parameterHits = false;
        for (var name : dottedNames("", rParameters).toList()) {
            var property = properties.get(name.toLowerCase(Locale.ROOT));
            if (property != null && setByProperties.contains(property)) {
                throw new IllegalArgumentException("The parameter '%s' is also set by the task property '%s'; set it in one place only.".formatted(name, property));
            }
            parameterHits |= "hits".equals(property);
        }
        body.putAll(rParameters);

        // no summaries built for hits the outputs would throw away
        if (!body.containsKey("hits") && !parameterHits && (rFetchType == FetchType.FETCH_ONE || rFetchType == FetchType.NONE)) {
            body.put("hits", rFetchType == FetchType.FETCH_ONE ? 1 : 0);
        }
        return body;
    }

    // nested maps are read as dotted names, so `model: {queryString: x}` is checked like `model.queryString`
    private static Stream<String> dottedNames(String prefix, Map<?, ?> parameters) {
        return parameters.entrySet().stream().flatMap(entry -> entry.getValue() instanceof Map<?, ?> nested
            ? dottedNames(prefix + entry.getKey() + ".", nested)
            : Stream.of(prefix + entry.getKey()));
    }

    private static List<Map<String, Object>> rows(JsonNode root) {
        var rows = new ArrayList<Map<String, Object>>();
        for (var hit : root.path("children")) {
            var row = new LinkedHashMap<String, Object>();
            row.put("id", hit.path("id").asText());
            row.put("relevance", hit.path("relevance").asDouble());
            if (hit.path("fields").isObject()) {
                var fields = JacksonMapper.ofJson().convertValue(hit.get("fields"), new TypeReference<LinkedHashMap<String, Object>>() {});
                fields.remove("sddocname");
                fields.remove("documentid");
                row.put("fields", fields);
            }
            rows.add(row);
        }
        return rows;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Hits as rows",
            description = """
                Only with `fetchType=FETCH`; with `STORE` the rows are in the file at `uri`. Each row holds `id`, \
                `relevance` and the document under `fields`."""
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "First hit as a row",
            description = """
                Only with `fetchType=FETCH_ONE`; null when nothing matched."""
        )
        private Map<String, Object> row;

        @Schema(
            title = "Number of rows in the outputs or in the stored file",
            description = """
                Null with `fetchType=NONE`."""
        )
        private Integer size;

        @Schema(
            title = "Number of documents matching the query",
            description = """
                Vespa's `totalCount`, which can be larger than the hits returned."""
        )
        private Long totalCount;

        @Schema(
            title = "URI of the file with the hits",
            description = """
                Only with `fetchType=STORE`: an ION file in Kestra's internal storage, one record per hit, each with `id`, \
                `relevance` and the document under `fields`."""
        )
        private URI uri;
    }
}
