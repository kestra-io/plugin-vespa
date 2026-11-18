package io.kestra.plugin.vespa;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get one Vespa document by its ID",
    description = """
        Reads one document from the Vespa document API (`/document/v1`) and returns its fields as `document`. A \
        document that doesn't exist gives a null `document`, or fails the task with `errorOnMissing: true`."""
)
@Plugin(
    examples = {
        @Example(
            title = "Find the closest album to a vector, then read the whole document.",
            full = true,
            code = """
                id: vespa_get
                namespace: company.team

                tasks:
                  - id: nearest
                    type: io.kestra.plugin.vespa.Query
                    endpoint: "{{ secret('VESPA_ENDPOINT') }}"
                    token: "{{ secret('VESPA_TOKEN') }}"
                    yql: "select * from music where {targetHits:1}nearestNeighbor(embedding, q)"
                    ranking: closeness
                    parameters:
                      input.query(q): [0, 0, 1, 0]
                    fetchType: FETCH_ONE

                  - id: get
                    type: io.kestra.plugin.vespa.Get
                    endpoint: "{{ secret('VESPA_ENDPOINT') }}"
                    token: "{{ secret('VESPA_TOKEN') }}"
                    documentId: "{{ outputs.nearest.row.id }}"
                    errorOnMissing: true

                  - id: log_document
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.get.document.artist }}: {{ outputs.get.document.album }}"
                """
        )
    }
)
public class Get extends AbstractVespaTask implements RunnableTask<Get.Output> {
    @Schema(
        title = "Vespa document ID",
        description = """
            The full ID, like `id:mynamespace:music::kind-of-blue`, or with a number or group part like \
            `id:mynamespace:music:g=mygroup:kind-of-blue`. A `Query` hit's `id` is one when the document's summary \
            carries it, as with `select *`."""
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> documentId;

    @Schema(
        title = "Fail the task when the document doesn't exist",
        description = """
            Default `false`: a missing document gives a null `document`."""
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> errorOnMissing = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rDocumentId = runContext.render(documentId).as(String.class).orElse("");
        var rErrorOnMissing = runContext.render(errorOnMissing).as(Boolean.class).orElse(false);
        var request = HttpRequest.builder().uri(URI.create(documentPath(rDocumentId))).method("GET").build();

        String answer;
        try (var client = client(runContext)) {
            answer = request(runContext, client, request).getBody();
        } catch (HttpClientResponseException e) {
            var missingId = missingId(e).orElseThrow(() -> e);
            if (rErrorOnMissing) {
                throw new IllegalStateException("The Vespa document '%s' doesn't exist; check `documentId`, or set `errorOnMissing: false` to get a null `document`.".formatted(missingId));
            }
            return Output.builder().id(missingId).build();
        }

        var json = document(answer);
        return Output.builder()
            .id(json.get("id").asText())
            .document(JacksonMapper.ofJson().convertValue(json.get("fields"), new TypeReference<LinkedHashMap<String, Object>>() {}))
            .build();
    }

    // a wrong document type or a gateway's 404 lacks Vespa's not-found shape; the ID isn't compared, Vespa normalises it (n=01 → n=1)
    private static Optional<String> missingId(HttpClientResponseException e) {
        if (e.getResponse() == null || e.getResponse().getStatus().getCode() != 404 || !(e.getResponse().getBody() instanceof byte[] body)) {
            return Optional.empty();
        }
        try {
            var json = JacksonMapper.ofJson().readTree(body);
            var notFound = json.path("pathId").asText().startsWith("/document/v1/") && json.path("id").isTextual() && !json.has("message");
            return notFound ? Optional.of(json.get("id").asText()) : Optional.empty();
        } catch (IOException notJson) {
            return Optional.empty();
        }
    }

    private static JsonNode document(String answer) {
        var json = answer(answer, "get");
        if (!json.path("id").isTextual() || !json.path("fields").isObject()) {
            throw wrongAnswer("get isn't a document", answer);
        }
        return json;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Vespa document ID",
            description = """
                As Vespa returns it, also when the document doesn't exist."""
        )
        private String id;

        @Schema(
            title = "Document fields",
            description = """
                The document's fields by name, tensors as Vespa renders them; null when the document doesn't exist."""
        )
        private Map<String, Object> document;
    }
}
