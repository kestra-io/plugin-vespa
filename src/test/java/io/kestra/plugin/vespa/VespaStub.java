package io.kestra.plugin.vespa;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

// For what the test container can't show: data-plane tokens are Vespa Cloud only, a slow answer, a redirect, a hand-made answer.
record VespaStub(HttpServer server, List<Seen> seen) implements AutoCloseable {
    static VespaStub start() throws IOException {
        return start(Duration.ZERO, null, List.of(new Answer(200, "{}")));
    }

    static VespaStub answering(String body) throws IOException {
        return answering(200, body);
    }

    // one answer per request in order, the last one repeated
    static VespaStub answering(List<String> bodies) throws IOException {
        return answering(bodies.stream().map(body -> new Answer(200, body)).toArray(Answer[]::new));
    }

    static VespaStub answering(int status, String body) throws IOException {
        return answering(new Answer(status, body));
    }

    static VespaStub answering(Answer... answers) throws IOException {
        return start(Duration.ZERO, null, List.of(answers));
    }

    static VespaStub start(Duration delay, int status, String location) throws IOException {
        return start(delay, location, List.of(new Answer(status, "{}")));
    }

    private static VespaStub start(Duration delay, String location, List<Answer> answers) throws IOException {
        var seen = new CopyOnWriteArrayList<Seen>();
        var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", exchange -> {
            seen.add(new Seen(
                exchange.getRemoteAddress().getPort(),
                exchange.getRequestMethod(),
                exchange.getRequestURI().getRawPath(),
                exchange.getRequestURI().getRawQuery(),
                exchange.getRequestHeaders().get("Authorization"),
                exchange.getRequestHeaders().getFirst("X-Probe"),
                new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)
            ));
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (location != null) {
                exchange.getResponseHeaders().add("Location", location);
            }
            var answer = answers.get(Math.min(seen.size(), answers.size()) - 1);
            var body = answer.body().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(answer.status(), body.length == 0 ? -1 : body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        return new VespaStub(server, seen);
    }

    String endpoint() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    record Answer(int status, String body) {
    }

    record Seen(int clientPort, String method, String path, String query, List<String> authorization, String probeHeader, String body) {
    }
}
