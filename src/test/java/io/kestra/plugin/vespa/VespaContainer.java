package io.kestra.plugin.vespa;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Rethrow;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@KestraTest
@Slf4j
public class VespaContainer {
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();
    static final GenericContainer<?> VESPA;

    static {
        VESPA = new GenericContainer<>(DockerImageName.parse("vespaengine/vespa:8.781.11"))
            .withExposedPorts(8080, 19071)
            // the image refuses to start under 4 GB of free memory, far more than the test data needs
            .withEnv("VESPA_IGNORE_NOT_ENOUGH_MEMORY", "true")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("vespa-container"))
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(19071).withStartupTimeout(Duration.ofSeconds(180)));

        VESPA.start();
        Runtime.getRuntime().addShutdownHook(new Thread(VESPA::stop));

        deploy(resource("vespa/app"));
        feed(resource("vespa/catalog.jsonl"));
    }

    @Inject
    protected RunContextFactory runContextFactory;

    protected static String endpoint() {
        return url(8080);
    }

    static void deploy(Path applicationDir) {
        var configServer = url(19071);
        var response = send(HttpRequest.newBuilder(URI.create(configServer + "/application/v2/tenant/default/prepareandactivate"))
            .header("Content-Type", "application/zip")
            .POST(HttpRequest.BodyPublishers.ofByteArray(zip(applicationDir)))
            .build());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("The config server refused the application package with status %d: %s".formatted(response.statusCode(), response.body()));
        }

        var converge = configServer + "/application/v2/tenant/default/application/default/environment/prod/region/default/instance/default/serviceconverge";
        await("application convergence", Duration.ofSeconds(120), answers(converge, r -> MAPPER.readTree(r.body()).path("converged").asBoolean()));
        await("query API", Duration.ofSeconds(120), answers(url(8080) + "/status.html", r -> r.statusCode() == 200));
        log.info("Deployed {}", applicationDir);
    }

    static void feed(Path catalogJsonl) {
        try (Stream<String> lines = Files.lines(catalogJsonl)) {
            var documents = lines.filter(line -> !line.isBlank()).toList();
            var putId = Pattern.compile("id:([^:]+):([^:]+)::(.+)");
            for (String line : documents) {
                var document = MAPPER.readTree(line);
                var id = document.path("put").asText();
                var parts = putId.matcher(id);
                if (!parts.matches()) {
                    throw new IllegalStateException("Only put lines with an id:<namespace>:<type>::<local id> id can be fed, got: %s".formatted(line));
                }
                var path = "/document/v1/" + parts.group(1) + "/" + parts.group(2) + "/docid/" + URLEncoder.encode(parts.group(3), StandardCharsets.UTF_8);
                var body = MAPPER.writeValueAsString(MAPPER.createObjectNode().set("fields", document.path("fields")));

                var response = send(HttpRequest.newBuilder(URI.create(url(8080) + path))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build());
                if (response.statusCode() != 200) {
                    throw new IllegalStateException("Vespa refused document %s with status %d: %s".formatted(id, response.statusCode(), response.body()));
                }
            }
            log.info("Fed {} documents from {}", documents.size(), catalogJsonl);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read the catalog %s.".formatted(catalogJsonl), e);
        }
    }

    private static String url(int containerPort) {
        return "http://" + VESPA.getHost() + ":" + VESPA.getMappedPort(containerPort);
    }

    private static Path resource(String name) {
        try {
            return Path.of(VespaContainer.class.getClassLoader().getResource(name).toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] zip(Path dir) {
        var bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes); Stream<Path> files = Files.walk(dir)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                zip.putNextEntry(new ZipEntry(dir.relativize(file).toString().replace(File.separatorChar, '/')));
                Files.copy(file, zip);
                zip.closeEntry();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot zip the application package %s.".formatted(dir), e);
        }
        return bytes.toByteArray();
    }

    private static HttpRequest get(String url) {
        return HttpRequest.newBuilder(URI.create(url)).GET().build();
    }

    private static HttpResponse<String> send(HttpRequest request) {
        try {
            return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException("%s %s failed.".formatted(request.method(), request.uri()), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** Ready means {@code attempt} returned without throwing; on timeout, the error carries its last failure. */
    static void await(String what, Duration timeout, Callable<?> attempt) {
        var deadline = Instant.now().plus(timeout);
        Exception last = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                attempt.call();
                return;
            } catch (Exception notReadyYet) {
                last = notReadyYet;
            }
            try {
                Thread.sleep(Duration.ofSeconds(1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new IllegalStateException("Timed out after %d s waiting for %s, last answer: %s".formatted(
            timeout.toSeconds(), what, last == null ? "none" : last.getMessage()), last);
    }

    /** Throws the response status and body while {@code ready} is false. */
    static Callable<Void> answers(String url, Rethrow.PredicateChecked<HttpResponse<String>, Exception> ready) {
        return () -> {
            var response = send(get(url));
            if (!ready.test(response)) {
                throw new IllegalStateException("%d %s".formatted(response.statusCode(), response.body()));
            }
            return null;
        };
    }
}
