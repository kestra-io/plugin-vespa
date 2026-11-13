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

import java.net.URI;
import java.time.Duration;
import java.util.stream.IntStream;

/**
 * Connection shared by the Vespa tasks and the trigger, which can't extend a task class.
 */
public interface VespaConnectionInterface {
    @Schema(
        title = "Vespa endpoint",
        description = "Base URL of the Vespa container cluster serving the query and document APIs, like `http://localhost:8080` or a Vespa Cloud endpoint. A trailing `/` is ignored."
    )
    @NotNull
    @PluginProperty(group = "connection")
    Property<String> getEndpoint();

    @Schema(
        title = "Data-plane token",
        description = "Vespa Cloud data-plane token, sent as a bearer token. Unset or empty sends no token, for a self-hosted Vespa without authentication."
    )
    @PluginProperty(secret = true, group = "connection")
    Property<String> getToken();

    @Schema(title = "The time allowed to connect to Vespa", description = "Unset uses the Kestra HTTP client's default.")
    @PluginProperty(group = "connection")
    Property<Duration> getConnectTimeout();

    @Schema(title = "The longest wait for data from Vespa", description = "Defaults to 5 minutes.")
    @PluginProperty(group = "connection")
    Property<Duration> getReadTimeout();

    /**
     * Builds the client a run sends all its requests through, so connections are reused; the caller closes it before the run ends.
     */
    default HttpClient client(RunContext runContext) throws IllegalVariableEvaluationException {
        var timeout = TimeoutConfiguration.builder().connectTimeout(getConnectTimeout());
        if (getReadTimeout() != null) {
            timeout.readIdleTimeout(getReadTimeout());
        }
        var configuration = HttpConfiguration.builder()
            .timeout(timeout.build())
            // a redirect would carry the token to another host, and Vespa's APIs don't redirect
            .followRedirects(Property.ofValue(false))
            .allowedResponseCodes(Property.ofValue(IntStream.range(200, 300).boxed().toList()));
        var rToken = runContext.render(getToken()).as(String.class).orElse("");
        if (!rToken.isBlank()) {
            configuration.auth(BearerAuthConfiguration.builder().token(Property.ofValue(rToken)).build());
        }
        return new HttpClient(runContext, configuration.build());
    }

    /**
     * Sends a request whose URI is a path and query relative to the endpoint; any answer other than 2xx throws with its status and body.
     */
    default HttpResponse<String> request(RunContext runContext, HttpClient client, HttpRequest request)
        throws IllegalVariableEvaluationException, HttpClientException {
        var httpRequest = HttpRequest.builder()
            .method(request.getMethod())
            .uri(URI.create(endpoint(runContext) + request.getUri()))
            .headers(request.getHeaders())
            .body(request.getBody())
            .build();

        return client.request(httpRequest, String.class);
    }

    private String endpoint(RunContext runContext) throws IllegalVariableEvaluationException {
        var rEndpoint = runContext.render(getEndpoint()).as(String.class).orElse("");
        // a query would end up before the request path, so only scheme, host, port and an optional path prefix are allowed
        if (!rEndpoint.matches("(?i)https?://(\\[[^]]+]|[^/?#@:\\s\\[\\]]+)(:\\d+)?(/[^?#\\s]*)?")) {
            throw new IllegalArgumentException("The Vespa endpoint must be an http or https URL, like http://localhost:8080, got: '%s'.".formatted(rEndpoint));
        }
        return StringUtils.stripEnd(rEndpoint, "/");
    }
}
