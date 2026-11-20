package io.kestra.plugin.vespa;

import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.Callback;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

// For what the test container can't show: data-plane tokens are Vespa Cloud only, a slow answer, a redirect, a hand-made answer.
// HTTP/1.1 for Kestra's HTTP client and HTTP/2 without TLS for Vespa's feed client, on one port.
record VespaStub(Server server, List<Seen> seen) implements AutoCloseable {
    private static final String HEALTH = "/state/v1/health";
    private static final Answer HEALTHY = new Answer(200, "{}");

    static VespaStub start() throws Exception {
        return start(Duration.ZERO, null, HEALTHY, List.of(new Answer(200, "{}")));
    }

    static VespaStub answering(String body) throws Exception {
        return answering(200, body);
    }

    // one answer per request in order, the last one repeated
    static VespaStub answering(List<String> bodies) throws Exception {
        return answering(bodies.stream().map(body -> new Answer(200, body)).toArray(Answer[]::new));
    }

    static VespaStub answering(int status, String body) throws Exception {
        return answering(new Answer(status, body));
    }

    static VespaStub answering(Answer... answers) throws Exception {
        return start(Duration.ZERO, null, HEALTHY, List.of(answers));
    }

    // `Feed` checks the health path before it feeds; its answer is kept out of the ordered answers so they still start at the handshake
    static VespaStub withHealth(Answer health, Answer... answers) throws Exception {
        return start(Duration.ZERO, null, health, List.of(answers));
    }

    static VespaStub start(Duration delay, int status, String location) throws Exception {
        return start(delay, location, HEALTHY, List.of(new Answer(status, "{}")));
    }

    private static VespaStub start(Duration delay, String location, Answer health, List<Answer> answers) throws Exception {
        var seen = new CopyOnWriteArrayList<Seen>();
        var answered = new AtomicInteger();
        var server = new Server();
        // Vespa takes an encoded `/` in a document ID, which Jetty refuses by default
        var configuration = new HttpConfiguration();
        configuration.setUriCompliance(UriCompliance.UNSAFE);
        var connector = new ServerConnector(server, new HttpConnectionFactory(configuration), new HTTP2CServerConnectionFactory(configuration));
        connector.setHost("localhost");
        server.addConnector(connector);
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(Request request, Response response, Callback callback) throws Exception {
                var authorization = request.getHeaders().getValuesList("Authorization");
                seen.add(new Seen(
                    Request.getRemotePort(request),
                    request.getMethod(),
                    request.getHttpURI().getPath(),
                    request.getHttpURI().getQuery(),
                    authorization.isEmpty() ? null : authorization,
                    request.getHeaders().get("X-Probe"),
                    Content.Source.asString(request, StandardCharsets.UTF_8)
                ));
                var answer = HEALTH.equals(request.getHttpURI().getPath()) ? health : answers.get(Math.min(answered.incrementAndGet(), answers.size()) - 1);
                try {
                    Thread.sleep(delay.plus(answer.delay()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (location != null) {
                    response.getHeaders().add("Location", location);
                }
                response.setStatus(answer.status());
                response.getHeaders().add("Content-Type", "application/json");
                Content.Sink.write(response, true, answer.body(), callback);
                return true;
            }
        });
        server.start();
        return new VespaStub(server, seen);
    }

    String endpoint() {
        return "http://localhost:" + ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }

    @Override
    public void close() throws Exception {
        server.stop();
    }

    record Answer(int status, String body, Duration delay) {
        Answer(int status, String body) {
            this(status, body, Duration.ZERO);
        }
    }

    record Seen(int clientPort, String method, String path, String query, List<String> authorization, String probeHeader, String body) {
    }
}
