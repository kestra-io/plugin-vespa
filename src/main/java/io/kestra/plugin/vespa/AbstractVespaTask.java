package io.kestra.plugin.vespa;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.serializers.JacksonMapper;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractVespaTask extends Task implements VespaConnectionInterface {
    // namespace, document type, an optional number or group, then the key, which may hold `:`
    private static final Pattern DOCUMENT_ID = Pattern.compile("id:([^:]+):([^:]+):(?:n=(\\d+)|g=([^:]+))?:(.+)");

    private Property<String> endpoint;

    @ToString.Exclude
    private Property<String> token;

    private Property<Duration> connectTimeout;

    private Property<Duration> readTimeout;

    // a part that is exactly `.` or `..` would reach the server as a path step, and Vespa refuses it even encoded
    static String documentPath(String documentId) {
        var parts = DOCUMENT_ID.matcher(documentId);
        if (!parts.matches() || Stream.of(parts.group(1), parts.group(2), parts.group(4), parts.group(5)).anyMatch(part -> ".".equals(part) || "..".equals(part))) {
            throw new IllegalArgumentException("The Vespa document ID must look like id:<namespace>:<document type>::<key>, with an optional n=<number> or g=<group> between the last two colons, got: '%s'.".formatted(documentId));
        }
        var location = "docid/";
        if (parts.group(3) != null) {
            location = "number/" + parts.group(3) + "/";
        } else if (parts.group(4) != null) {
            location = "group/" + encode(parts.group(4)) + "/";
        }
        return "/document/v1/" + encode(parts.group(1)) + "/" + encode(parts.group(2)) + "/" + location + encode(parts.group(5));
    }

    static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    // each task checks the shape it expects on top
    static JsonNode answer(String body, String operation) {
        if (body == null || body.isBlank()) {
            throw new IllegalStateException("Vespa sent an empty answer to the %s; check that `endpoint` is the URL of the Vespa container cluster.".formatted(operation));
        }
        try {
            return JacksonMapper.ofJson().readTree(body);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Vespa's answer to the %s isn't JSON; check that `endpoint` is the URL of the Vespa container cluster. Answer: %s".formatted(operation, StringUtils.abbreviate(body, 1000)), e);
        }
    }
}
