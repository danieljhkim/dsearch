package com.danieljhkim.dsearch.common.health;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.logging.Logger;

public final class HealthHttpServer {

    private static final Logger LOGGER = Logger.getLogger(HealthHttpServer.class.getName());

    private HealthHttpServer() {}

    /**
     * Starts the conventional health server. {@code /health} remains a liveness alias for
     * compatibility; callers that need traffic admission must use {@code /readyz}.
     */
    public static HttpServer start(int port, String serviceName) throws IOException {
        return start(port, serviceName, Readiness::up);
    }

    /**
     * Starts liveness and readiness endpoints.
     *
     * <p>{@code GET /health} and {@code GET /livez} return {@code 200} while this HTTP server is
     * running and never invoke the readiness supplier. {@code GET /readyz} returns {@code 200}
     * only when the supplied check is ready; it returns {@code 503} with a machine-readable
     * {@code reason} otherwise. Any non-GET request returns {@code 405}.
     */
    public static HttpServer start(int port, String serviceName, Supplier<Readiness> readiness) throws IOException {
        return start(port, serviceName, readiness, null);
    }

    /**
     * Starts liveness and readiness endpoints, with an optional process-local detail on {@code /health}.
     *
     * <p>The detail supplier is not the readiness probe. Liveness must stay independent of disk and
     * dependency checks, so {@code /health} remains HTTP 200. When the supplier returns a non-blank
     * string, the body adds {@code readiness=DOWN} and the same {@code reason} an operator would
     * read from {@code /readyz}. {@code /livez} never includes it.
     */
    public static HttpServer start(
            int port, String serviceName, Supplier<Readiness> readiness, Supplier<String> healthDetail)
            throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", new HealthHandler(serviceName, null, healthDetail));
        server.createContext("/livez", new HealthHandler(serviceName, null, null));
        server.createContext("/readyz", new HealthHandler(serviceName, readiness, null));
        server.createContext("/metrics", new MetricsHandler());
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, serviceName + "-health-http");
            t.setDaemon(true);
            return t;
        });
        server.setExecutor(executor);
        server.start();
        LOGGER.info(() -> "Health HTTP server for " + serviceName + " started on port " + port);
        return server;
    }

    public record Readiness(boolean ready, String reason) {
        public Readiness {
            if (!ready && (reason == null || reason.isBlank())) {
                throw new IllegalArgumentException("A not-ready response must include a reason");
            }
        }

        public static Readiness up() {
            return new Readiness(true, null);
        }

        public static Readiness notReady(String reason) {
            return new Readiness(false, reason);
        }
    }

    private static class HealthHandler implements HttpHandler {

        private final String serviceName;
        private final Supplier<Readiness> readiness;
        private final Supplier<String> healthDetail;

        private HealthHandler(String serviceName, Supplier<Readiness> readiness, Supplier<String> healthDetail) {
            this.serviceName = serviceName;
            this.readiness = readiness;
            this.healthDetail = healthDetail;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
                exchange.close();
                return;
            }
            if (readiness != null) {
                writeReadiness(exchange, safelyCheckReadiness());
                return;
            }
            writeLiveness(exchange, readHealthDetail());
        }

        private void writeReadiness(HttpExchange exchange, Readiness result) throws IOException {
            String body = String.format(
                    "{\"status\":\"%s\",\"service\":\"%s\",\"timestamp\":\"%s\"%s}",
                    result.ready() ? "UP" : "DOWN",
                    serviceName,
                    Instant.now(),
                    result.ready() ? "" : ",\"reason\":\"" + escapeJson(result.reason()) + "\"");
            write(exchange, result.ready() ? 200 : 503, body);
        }

        private void writeLiveness(HttpExchange exchange, String detail) throws IOException {
            String extra = "";
            if (detail != null && !detail.isBlank()) {
                extra = ",\"readiness\":\"DOWN\",\"reason\":\"" + escapeJson(detail) + "\"";
            }
            String body = String.format(
                    "{\"status\":\"UP\",\"service\":\"%s\",\"timestamp\":\"%s\"%s}", serviceName, Instant.now(), extra);
            write(exchange, 200, body);
        }

        private static void write(HttpExchange exchange, int status, String body) throws IOException {
            byte[] responseBytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }

        private Readiness safelyCheckReadiness() {
            try {
                Readiness result = readiness.get();
                return result == null ? Readiness.notReady("readiness_check_returned_null") : result;
            } catch (RuntimeException e) {
                LOGGER.warning(() -> "Readiness check failed for " + serviceName + ": " + e);
                return Readiness.notReady(
                        "readiness_check_failed:" + e.getClass().getSimpleName());
            }
        }

        private String readHealthDetail() {
            if (healthDetail == null) {
                return null;
            }
            try {
                return healthDetail.get();
            } catch (RuntimeException e) {
                LOGGER.warning(() -> "Health detail failed for " + serviceName + ": " + e);
                return "health_detail_failed:" + e.getClass().getSimpleName();
            }
        }

        private static String escapeJson(String value) {
            return value.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t");
        }
    }

    /** Exposes the process-wide Prometheus registry alongside health without changing gRPC ports. */
    private static class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                exchange.close();
                return;
            }
            exchange.getResponseHeaders().set("Content-Type", TextFormat.CONTENT_TYPE_004);
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream output = exchange.getResponseBody();
                    OutputStreamWriter writer = new OutputStreamWriter(output, StandardCharsets.UTF_8)) {
                TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
            }
        }
    }
}
