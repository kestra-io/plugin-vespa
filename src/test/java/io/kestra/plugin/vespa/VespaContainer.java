package io.kestra.plugin.vespa;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Shared Vespa for the plugin tests: one container per test JVM, with the test application and the music catalog,
 * so every task test asserts against the same known data. The memory check is skipped because the image refuses to
 * start under 4 GB free, far more than the test data needs.
 */
@KestraTest
public class VespaContainer {
    private static final Logger LOG = LoggerFactory.getLogger(VespaContainer.class);
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson();
    private static final GenericContainer<?> VESPA;

    static {
        VESPA = new GenericContainer<>(DockerImageName.parse("vespaengine/vespa:8.781.11"))
            .withExposedPorts(8080, 19071)
            .withEnv("VESPA_IGNORE_NOT_ENOUGH_MEMORY", "true")
            .withCreateContainerCmdModifier(cmd -> cmd
                .withHostName("vespa-container")
                .getHostConfig()
                .withPortBindings(List.of(
                    new PortBinding(Ports.Binding.bindPort(18080), new ExposedPort(8080)),
                    new PortBinding(Ports.Binding.empty(), new ExposedPort(19071))
                ))
            )
            .waitingFor(Wait.forHttp("/state/v1/health").forPort(19071).withStartupTimeout(Duration.ofSeconds(180)));

        VESPA.start();
        Runtime.getRuntime().addShutdownHook(new Thread(VESPA::stop));

        deploy(resource("vespa/app"));
        feed(resource("vespa/catalog.jsonl"));
    }

    @Inject
    protected RunContextFactory runContextFactory;

    @Value("${vespa-endpoint}")
    protected String endpoint;

    /** Deploys an application package through the config server API, as no Vespa CLI runs in CI. */
    static void deploy(Path applicationDir) {
        String configServer = url(19071);
        HttpResponse<String> response = send(HttpRequest.newBuilder(URI.create(configServer + "/application/v2/tenant/default/prepareandactivate"))
            .header("Content-Type", "application/zip")
            .POST(HttpRequest.BodyPublishers.ofByteArray(zip(applicationDir)))
            .build());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Deploy failed: " + response.statusCode() + " " + response.body());
        }

        String converge = configServer + "/application/v2/tenant/default/application/default/environment/prod/region/default/instance/default/serviceconverge";
        await("application convergence", () -> MAPPER.readTree(send(get(converge)).body()).path("converged").asBoolean());
        await("query API", () -> send(get(url(8080) + "/status.html")).statusCode() == 200);
        LOG.info("Deployed {}", applicationDir);
    }

    /** Feeds a JSON Lines file of Vespa put operations through the document API, one document per request. */
    static void feed(Path catalogJsonl) {
        try (Stream<String> lines = Files.lines(catalogJsonl)) {
            List<String> documents = lines.filter(line -> !line.isBlank()).toList();
            for (String line : documents) {
                JsonNode document = MAPPER.readTree(line);
                String id = document.path("put").asText();
                String[] namespaceTypeAndLocalId = id.split(":", 5);
                String path = "/document/v1/" + namespaceTypeAndLocalId[1] + "/" + namespaceTypeAndLocalId[2] + "/docid/" + URLEncoder.encode(namespaceTypeAndLocalId[4], StandardCharsets.UTF_8);
                String body = MAPPER.writeValueAsString(MAPPER.createObjectNode().set("fields", document.path("fields")));

                HttpResponse<String> response = send(HttpRequest.newBuilder(URI.create(url(8080) + path))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build());
                if (response.statusCode() != 200) {
                    throw new IllegalStateException("Feed failed for " + id + ": " + response.statusCode() + " " + response.body());
                }
            }
            LOG.info("Fed {} documents from {}", documents.size(), catalogJsonl);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read " + catalogJsonl, e);
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
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes); Stream<Path> files = Files.walk(dir)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                zip.putNextEntry(new ZipEntry(dir.relativize(file).toString()));
                Files.copy(file, zip);
                zip.closeEntry();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot zip " + dir, e);
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
            throw new IllegalStateException(request.method() + " " + request.uri() + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void await(String what, Callable<Boolean> ready) {
        Instant deadline = Instant.now().plusSeconds(120);
        while (Instant.now().isBefore(deadline)) {
            try {
                if (ready.call()) {
                    return;
                }
            } catch (Exception notReadyYet) {
                LOG.debug("Waiting for {}: {}", what, notReadyYet.getMessage());
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new IllegalStateException("Timed out after 120 s waiting for " + what);
    }
}
