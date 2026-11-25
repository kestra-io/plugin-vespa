package io.kestra.plugin.vespa;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.HttpClientResponseException;
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
import java.io.InputStream;
import java.net.URI;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

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
        return client(runContext, timeout.build());
    }

    /**
     * Same client with its own time limits, for a request that must not wait as long as the task's.
     */
    default HttpClient client(RunContext runContext, TimeoutConfiguration timeout) throws IllegalVariableEvaluationException {
        var configuration = HttpConfiguration.builder()
            .timeout(timeout)
            // a redirect would carry the token to another host, and Vespa's APIs don't redirect
            .followRedirects(Property.ofValue(false))
            // every status reaches `request()`, so an error answer is read with the same cap as a successful one
            .allowFailed(Property.ofValue(true));
        var rToken = runContext.render(getToken()).as(String.class).orElse("");
        if (!rToken.isBlank()) {
            configuration.auth(BearerAuthConfiguration.builder().token(Property.ofValue(rToken)).build());
        }
        return new HttpClient(runContext, configuration.build());
    }

    /**
     * Sends a request whose URI is a path and query relative to the endpoint; any answer other than 2xx throws with its status and body.
     * An answer that is cut off, stops for longer than the read timeout or is larger than the cap leaves {@code client} closed.
     */
    default HttpResponse<String> request(RunContext runContext, HttpClient client, HttpRequest request)
        throws IllegalVariableEvaluationException, HttpClientException {
        var httpRequest = HttpRequest.builder()
            .method(request.getMethod())
            .uri(URI.create(endpoint(runContext) + request.getUri()))
            .headers(request.getHeaders())
            .body(request.getBody())
            .build();

        var body = new AtomicReference<byte[]>();
        var response = client.request(httpRequest, answer -> {
            try {
                body.set(read(answer.getBody(), httpRequest));
            } catch (IllegalStateException failed) {
                // closing the answer would read the rest of the body, which may never end, so the client is closed to drop the connection
                try {
                    client.close();
                } catch (IOException e) {
                    failed.addSuppressed(e);
                }
                throw failed;
            }
        });
        var code = response.getStatus().getCode();
        if (code < 200 || code >= 300) {
            throw failure(response, body.get());
        }
        return HttpResponse.<String>builder()
            .status(response.getStatus())
            .headers(response.getHeaders())
            .endpointDetail(response.getEndpointDetail())
            .request(response.getRequest())
            .body(body.get() == null ? null : new String(body.get(), StandardCharsets.UTF_8))
            .build();
    }

    // the same exception, message and byte[] body core builds for an error answer (`HttpResponseFailure`), from the capped body
    private static HttpClientResponseException failure(HttpResponse<Void> response, byte[] body) {
        var message = "Failed http request with response code '%d'".formatted(response.getStatus().getCode());
        if (body != null) {
            message += " and body:\n%s".formatted(new String(body, StandardCharsets.UTF_8));
        }
        return new HttpClientResponseException(message, HttpResponse.<byte[]>builder()
            .status(response.getStatus())
            .headers(response.getHeaders())
            .endpointDetail(response.getEndpointDetail())
            .request(response.getRequest())
            .body(body)
            .build());
    }

    // read with a cap, so a wrong endpoint can't fill the worker's memory
    private static byte[] read(InputStream body, HttpRequest request) {
        if (body == null) {
            return null;
        }
        var max = 32 * 1024 * 1024;
        var method = request.getMethod();
        var path = request.getUri().getRawPath();
        try {
            var bytes = body.readNBytes(max + 1);
            if (bytes.length > max) {
                throw new IllegalStateException("Vespa's answer to %s %s is larger than %d bytes. Check that `endpoint` points to Vespa, and for a query ask for fewer `hits` or summary fields.".formatted(method, path, max));
            }
            return bytes;
        } catch (SocketTimeoutException e) {
            throw new IllegalStateException("Vespa's answer to %s %s stopped for longer than `readTimeout`. Raise `readTimeout`, or for a query ask for fewer `hits`.".formatted(method, path), e);
        } catch (IOException e) {
            throw new IllegalStateException("Reading Vespa's answer to %s %s failed: %s. Check that `endpoint` points to Vespa and try again.".formatted(method, path, e.getMessage()), e);
        }
    }

    /**
     * Renders the endpoint and refuses anything but an http or https base URL, before any request.
     */
    default String endpoint(RunContext runContext) throws IllegalVariableEvaluationException {
        var rEndpoint = runContext.render(getEndpoint()).as(String.class).orElse("");
        // a query would end up before the request path, so only scheme, host, port and an optional path prefix are allowed
        var parts = Pattern.compile("(?i)https?://(\\[[^]]+]|[^/?#@:\\s\\[\\]]+)(?::(\\d{1,5}))?(/[^?#\\s]*)?").matcher(rEndpoint);
        if (!parts.matches() || !isPort(parts.group(2)) || !isParsable(rEndpoint)) {
            throw new IllegalArgumentException("The Vespa endpoint must be an http or https URL, like http://localhost:8080, got: '%s'.".formatted(rEndpoint));
        }
        return StringUtils.stripEnd(rEndpoint, "/");
    }

    // `URI` reads an out-of-range port as a registry authority (host and port unset), as it does a host with `_`, so the digits are checked here
    private static boolean isPort(String digits) {
        if (digits == null) {
            return true;
        }
        var port = Integer.parseInt(digits);
        return port >= 1 && port <= 65535;
    }

    private static boolean isParsable(String rEndpoint) {
        try {
            new URI(rEndpoint);
            return true;
        } catch (URISyntaxException e) {
            return false;
        }
    }
}
