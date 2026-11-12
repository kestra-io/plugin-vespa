package io.kestra.plugin.vespa;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.configurations.BearerAuthConfiguration;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;

/**
 * Connection shared by the Vespa tasks and the trigger, which can't extend a task class.
 */
public interface VespaConnectionInterface {
    @Schema(
        title = "Vespa endpoint",
        description = "Base URL of the Vespa container cluster serving the query and document APIs, like `http://localhost:8080` or a Vespa Cloud endpoint."
    )
    @NotNull
    @PluginProperty(group = "connection")
    Property<String> getEndpoint();

    @Schema(
        title = "Data-plane token",
        description = "Vespa Cloud data-plane token, sent as a bearer token. Leave empty for a self-hosted Vespa without authentication."
    )
    @PluginProperty(secret = true, group = "connection")
    Property<String> getToken();

    @Schema(title = "The time allowed to connect to Vespa")
    @PluginProperty(group = "connection")
    Property<Duration> getConnectTimeout();

    @Schema(title = "The longest wait for data from Vespa", description = "Defaults to 5 minutes.")
    @PluginProperty(group = "connection")
    Property<Duration> getReadTimeout();

    /**
     * Sends a request whose URI is a path and query relative to the endpoint; a non-2xx answer throws with its status and body.
     */
    default HttpResponse<String> request(RunContext runContext, HttpRequest.HttpRequestBuilder request)
        throws IllegalVariableEvaluationException, IOException, HttpClientException {
        var rEndpoint = StringUtils.removeEnd(runContext.render(getEndpoint()).as(String.class).orElseThrow(), "/");
        var httpRequest = request.uri(URI.create(rEndpoint + request.build().getUri())).build();

        var configuration = HttpConfiguration.builder()
            .timeout(TimeoutConfiguration.builder()
                .connectTimeout(getConnectTimeout())
                .readIdleTimeout(getReadTimeout() == null ? Property.ofValue(Duration.ofMinutes(5)) : getReadTimeout())
                .build());
        if (getToken() != null) {
            configuration.auth(BearerAuthConfiguration.builder().token(getToken()).build());
        }

        runContext.logger().debug("{} {}", httpRequest.getMethod(), httpRequest.getUri().getPath());
        try (var client = new HttpClient(runContext, configuration.build())) {
            return client.request(httpRequest, String.class);
        }
    }
}
