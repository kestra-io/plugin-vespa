package io.kestra.plugin.vespa;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete Vespa documents by ID or by selection",
    description = """
        Removes one document by its ID, or every document a selection expression matches in a content cluster, \
        through the Vespa document API (`/document/v1`). A delete by selection needs `allowSelection: true`."""
)
@Plugin(
    examples = {
        @Example(
            title = "Delete one document by its ID.",
            full = true,
            code = """
                id: vespa_delete
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.vespa.Delete
                    endpoint: "{{ secret('VESPA_ENDPOINT') }}"
                    token: "{{ secret('VESPA_TOKEN') }}"
                    documentId: "id:mynamespace:music::kind-of-blue"
                """
        ),
        @Example(
            title = "Delete every album of one artist and log how many were removed.",
            full = true,
            code = """
                id: vespa_delete_selection
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.vespa.Delete
                    endpoint: "{{ secret('VESPA_ENDPOINT') }}"
                    token: "{{ secret('VESPA_TOKEN') }}"
                    selection: 'music.artist == "Miles Davis"'
                    cluster: music
                    allowSelection: true

                  - id: log_deleted
                    type: io.kestra.plugin.core.log.Log
                    message: "Deleted {{ outputs.delete.deleted }} documents"
                """
        )
    }
)
public class Delete extends AbstractVespaTask implements RunnableTask<Delete.Output> {
    @Schema(
        title = "Vespa document ID",
        description = """
            The full ID, like `id:mynamespace:music::kind-of-blue`, or with a number or group part like \
            `id:mynamespace:music:g=mygroup:kind-of-blue`. Set this or `selection`. Vespa answers the same whether \
            the document existed or not, so a mistyped ID succeeds and removes nothing."""
    )
    @PluginProperty(group = "main")
    private Property<String> documentId;

    @Schema(
        title = "Vespa document selection",
        description = """
            Every document it matches is removed, like `music.artist == "Miles Davis"`; `true` matches every \
            document of the cluster. Set this or `documentId`; it needs `cluster` and `allowSelection: true`."""
    )
    @PluginProperty(group = "main")
    private Property<String> selection;

    @Schema(
        title = "Vespa content cluster",
        description = """
            The name of the content cluster to delete from, required with `selection`."""
    )
    @PluginProperty(group = "main")
    private Property<String> cluster;

    @Schema(
        title = "Allow a delete by selection",
        description = """
            Default `false`: a task with `selection` fails before sending anything, so a flow never removes many \
            documents by accident."""
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> allowSelection = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rDocumentId = runContext.render(documentId).as(String.class).orElse(null);
        var rSelection = runContext.render(selection).as(String.class).orElse(null);
        if ((rDocumentId == null) == (rSelection == null)) {
            throw new IllegalArgumentException("Set exactly one of `documentId` or `selection`, got %s.".formatted(rDocumentId == null ? "neither" : "both"));
        }

        if (rDocumentId != null) {
            var request = HttpRequest.builder().uri(URI.create(documentPath(rDocumentId))).method("DELETE").build();
            try (var client = client(runContext)) {
                var answer = request(runContext, client, request).getBody();
                var json = answer(answer, "delete");
                if (!json.path("id").isTextual()) {
                    throw notAResult(answer);
                }
                return Output.builder().id(json.get("id").asText()).build();
            }
        }

        if (rSelection.isBlank()) {
            throw new IllegalArgumentException("The `selection` is blank; set a Vespa document selection, like music.artist == \"Miles Davis\".");
        }
        var rCluster = runContext.render(cluster).as(String.class).orElse("");
        if (rCluster.isBlank()) {
            throw new IllegalArgumentException("A delete by `selection` needs `cluster`, the name of the Vespa content cluster.");
        }
        if (!runContext.render(allowSelection).as(Boolean.class).orElse(false)) {
            throw new IllegalArgumentException("A delete by `selection` removes every document it matches; set `allowSelection: true` to allow it.");
        }
        try (var client = client(runContext)) {
            return Output.builder().deleted(deleteBySelection(runContext, client, rSelection, rCluster)).build();
        }
    }

    // Vespa deletes in chunks and answers each with a continuation token until the last one
    private long deleteBySelection(RunContext runContext, HttpClient client, String rSelection, String rCluster) throws Exception {
        var path = "/document/v1/?cluster=" + encode(rCluster) + "&selection=" + encode(rSelection);
        var deleted = 0L;
        String continuation = null;
        do {
            var uri = continuation == null ? path : path + "&continuation=" + encode(continuation);
            var answer = request(runContext, client, HttpRequest.builder().uri(URI.create(uri)).method("DELETE").build()).getBody();
            var json = answer(answer, "delete");
            continuation = json.path("continuation").isTextual() ? json.get("continuation").asText() : null;
            // without a continuation, a message means the delete stopped on an error
            if (continuation == null && json.path("message").isTextual()) {
                throw new IllegalStateException("Vespa stopped the delete by selection after %d documents: %s".formatted(deleted, json.get("message").asText()));
            }
            if (!json.path("documentCount").isIntegralNumber()) {
                throw notAResult(answer);
            }
            deleted += json.get("documentCount").asLong();
            runContext.logger().info("Vespa deleted {} documents in this chunk, {} so far.", json.get("documentCount").asLong(), deleted);
        } while (continuation != null);
        runContext.logger().info("Vespa deleted {} documents matching the selection.", deleted);
        return deleted;
    }

    private static IllegalStateException notAResult(String answer) {
        return new IllegalStateException("Vespa's answer to the delete isn't a delete result; check that `endpoint` is the URL of the Vespa container cluster. Answer: %s".formatted(StringUtils.abbreviate(answer, 1000)));
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Vespa document ID",
            description = """
                As Vespa returns it for a delete by ID; null for a delete by selection."""
        )
        private String id;

        @Schema(
            title = "Number of documents deleted",
            description = """
                For a delete by selection, the sum Vespa reports over all chunks; null for a delete by ID, since Vespa \
                answers the same whether the document existed."""
        )
        private Long deleted;
    }
}
