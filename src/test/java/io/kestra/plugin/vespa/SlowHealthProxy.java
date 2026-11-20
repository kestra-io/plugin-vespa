package io.kestra.plugin.vespa;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

// Feed's health request gets its answer one byte every 2 s, which no idle limit catches; later connections reach the stub.
final class SlowHealthProxy implements AutoCloseable {
    private final ServerSocket server;
    private final ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean healthSeen = new AtomicBoolean();

    SlowHealthProxy(String target) throws IOException {
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        var targetUri = URI.create(target);
        threads.submit(() -> {
            var first = true;
            while (!server.isClosed()) {
                var socket = server.accept();
                if (first) {
                    first = false;
                    threads.submit(() -> trickle(socket));
                } else {
                    threads.submit(() -> pipe(socket, targetUri));
                }
            }
            return null;
        });
    }

    String endpoint() {
        return "http://127.0.0.1:" + server.getLocalPort();
    }

    boolean healthSeen() {
        return healthSeen.get();
    }

    private Void trickle(Socket socket) throws IOException, InterruptedException {
        try (socket) {
            var in = socket.getInputStream();
            var request = new StringBuilder();
            while (!request.toString().endsWith("\r\n\r\n")) {
                var b = in.read();
                if (b < 0) {
                    return null;
                }
                request.append((char) b);
            }
            healthSeen.set(true);
            var out = socket.getOutputStream();
            out.write("HTTP/1.1 200 OK\r\nX-Slow: ".getBytes(StandardCharsets.US_ASCII));
            for (var i = 0; i < 60; i++) {
                out.write('a');
                out.flush();
                Thread.sleep(Duration.ofSeconds(2));
            }
        } catch (IOException closedByTheClient) {
            // the client gave up, which is what the tests wait for
        }
        return null;
    }

    private Void pipe(Socket client, URI target) throws IOException {
        try (client; var upstream = new Socket(target.getHost(), target.getPort())) {
            var back = threads.submit(() -> copy(upstream.getInputStream(), client.getOutputStream()));
            copy(client.getInputStream(), upstream.getOutputStream());
            back.cancel(true);
        }
        return null;
    }

    private static Void copy(InputStream in, OutputStream out) {
        try {
            in.transferTo(out);
        } catch (IOException closed) {
            // either side closed the connection
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        server.close();
        threads.shutdownNow();
    }
}
