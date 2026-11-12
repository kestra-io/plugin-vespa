package io.kestra.plugin.vespa;

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
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query Vespa with YQL",
    description = "Runs one YQL query on the Vespa query API and returns the hits of that one answer as rows: each row holds the hit's `id` and `relevance` beside its summary fields. Default `fetchType=FETCH`. Vespa caps one answer at 400 hits and the offset at 1000."
)
@Plugin(
    metrics = {
        @Metric(name = "records", type = Counter.TYPE, unit = "records", description = "Number of hits in Vespa's answer")
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
                    message: "{{ outputs.search.size }} of {{ outputs.search.totalCount }} albums, first: {{ outputs.search.rows[0].album }}"
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
                    message: "Closest album: {{ outputs.nearest.row.album }}"
                """
        )
    }
)
public class Query extends AbstractVespaTask implements RunnableTask<Query.Output> {
    @Schema(title = "YQL query", description = "Like `select * from music where genre contains \"jazz\"`.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> yql;

    @Schema(title = "Text for `userQuery()` in the YQL")
    @PluginProperty(group = "main")
    private Property<String> query;

    @Schema(title = "Number of hits to return", description = "Vespa's default is 10 and its limit 400.")
    @PluginProperty(group = "main")
    private Property<Integer> hits;

    @Schema(title = "Number of hits to skip", description = "Vespa's limit is 1000.")
    @PluginProperty(group = "main")
    private Property<Integer> offset;

    @Schema(title = "Rank profile to rank the hits with")
    @PluginProperty(group = "main")
    private Property<String> ranking;

    @Schema(title = "Vespa's own time limit for the query", description = "The task's `timeout` bounds the whole task; this one is sent to Vespa.")
    @PluginProperty(group = "advanced")
    private Property<Duration> queryTimeout;

    @Schema(
        title = "Other query API parameters",
        description = "Sent as they are, like `input.query(q)` for a query tensor or `presentation.format.tensors: short-value`. A parameter also set by a property of this task fails the task."
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> parameters;

    @Schema(
        title = "Result handling mode",
        description = "`FETCH` returns all hits as `rows`, `FETCH_ONE` the first hit as `row`, `NONE` only `totalCount`. `STORE` is not supported yet."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rFetchType = runContext.render(fetchType).as(FetchType.class).orElseThrow();
        if (rFetchType == FetchType.STORE) {
            throw new IllegalArgumentException("Query doesn't support fetchType STORE yet; use FETCH, FETCH_ONE or NONE.");
        }

        var request = HttpRequest.builder()
            .uri(URI.create("/search/"))
            .method("POST")
            .body(HttpRequest.JsonRequestBody.builder().content(body(runContext)).build())
            .build();
        var answer = request(runContext, request).getBody();
        if (answer == null || answer.isBlank()) {
            throw new IllegalStateException("Vespa sent an empty answer to the query.");
        }

        var root = JacksonMapper.ofJson().readTree(answer).path("root");
        // degraded coverage (like a soft timeout) still returns usable hits, so Vespa's errors are shown, not thrown
        root.path("errors").forEach(error -> runContext.logger().warn("Vespa answered with an error: {}", error.path("message").asText()));

        var rows = rows(runContext, root);
        runContext.metric(Counter.of("records", rows.size()));

        var output = Output.builder().totalCount(root.at("/fields/totalCount").asLong());
        return switch (rFetchType) {
            case FETCH -> output.rows(rows).size(rows.size()).build();
            case FETCH_ONE -> output.row(rows.isEmpty() ? null : rows.getFirst()).size(Math.min(rows.size(), 1)).build();
            default -> output.build();
        };
    }

    private Map<String, Object> body(RunContext runContext) throws Exception {
        var body = new HashMap<String, Object>();
        body.put("yql", runContext.render(yql).as(String.class).orElseThrow());
        runContext.render(query).as(String.class).ifPresent(v -> body.put("query", v));
        runContext.render(hits).as(Integer.class).ifPresent(v -> body.put("hits", v));
        runContext.render(offset).as(Integer.class).ifPresent(v -> body.put("offset", v));
        runContext.render(ranking).as(String.class).ifPresent(v -> body.put("ranking", v));
        runContext.render(queryTimeout).as(Duration.class).ifPresent(v -> body.put("timeout", v.toMillis() + "ms"));

        for (var parameter : runContext.render(parameters).asMap(String.class, Object.class).entrySet()) {
            if (body.containsKey(parameter.getKey())) {
                throw new IllegalArgumentException("The parameter '%s' is also set by a property of the task; set it in one place only.".formatted(parameter.getKey()));
            }
            body.put(parameter.getKey(), parameter.getValue());
        }
        return body;
    }

    private static List<Map<String, Object>> rows(RunContext runContext, JsonNode root) {
        var rows = new ArrayList<Map<String, Object>>();
        for (var hit : root.path("children")) {
            var fields = JacksonMapper.ofJson().convertValue(hit.path("fields"), new TypeReference<LinkedHashMap<String, Object>>() {});
            fields.remove("sddocname");
            fields.remove("documentid");
            var clashes = fields.keySet().stream().filter(name -> name.equals("id") || name.equals("relevance")).toList();
            if (!clashes.isEmpty()) {
                runContext.logger().warn("Dropped the document fields {} of hit {}: Vespa's own id and relevance keep those names.", clashes, hit.path("id").asText());
                clashes.forEach(fields::remove);
            }

            var row = new LinkedHashMap<String, Object>();
            row.put("id", hit.path("id").asText());
            row.put("relevance", hit.path("relevance").asDouble());
            row.putAll(fields);
            rows.add(row);
        }
        return rows;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Hits as rows", description = "Only with `fetchType=FETCH`.")
        private List<Map<String, Object>> rows;

        @Schema(title = "First hit as a row", description = "Only with `fetchType=FETCH_ONE`; null when nothing matched.")
        private Map<String, Object> row;

        @Schema(title = "Number of rows in the outputs", description = "Null with `fetchType=NONE`.")
        private Integer size;

        @Schema(title = "Number of documents matching the query", description = "Vespa's `totalCount`, which can be larger than the hits returned.")
        private Long totalCount;
    }
}
