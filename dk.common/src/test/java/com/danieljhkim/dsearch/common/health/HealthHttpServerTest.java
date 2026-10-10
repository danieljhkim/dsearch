package com.danieljhkim.dsearch.common.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class HealthHttpServerTest {

    @Test
    void healthKeepsLivenessUpWhilePublishingAPermanentDetail() throws Exception {
        AtomicReference<String> detail = new AtomicReference<>("index_format_unsupported:shard-0 path=\"/data\"");
        HttpServer server = HealthHttpServer.start(
                0,
                "index-node",
                () -> HealthHttpServer.Readiness.notReady("index_format_unsupported:shard-0"),
                detail::get);
        try {
            int port = server.getAddress().getPort();
            HttpResponse<String> health = get(port, "/health");
            assertEquals(200, health.statusCode());
            assertTrue(health.body().contains("\"status\":\"UP\""));
            assertTrue(health.body().contains("\"readiness\":\"DOWN\""));
            assertTrue(health.body().contains("index_format_unsupported:shard-0"));
            assertTrue(health.body().contains("path=\\\"/data\\\""));

            HttpResponse<String> liveness = get(port, "/livez");
            assertEquals(200, liveness.statusCode());
            assertFalse(liveness.body().contains("readiness"));

            HttpResponse<String> readiness = get(port, "/readyz");
            assertEquals(503, readiness.statusCode());
            assertTrue(readiness.body().contains("\"reason\":\"index_format_unsupported:shard-0\""));

            detail.set(" ");
            HttpResponse<String> cleared = get(port, "/health");
            assertEquals(200, cleared.statusCode());
            assertFalse(cleared.body().contains("readiness"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void healthDetailFailureStaysLiveWithAFallbackReason() throws Exception {
        HttpServer server = HealthHttpServer.start(0, "index-node", HealthHttpServer.Readiness::up, () -> {
            throw new IllegalStateException("detail unavailable");
        });
        try {
            HttpResponse<String> health = get(server.getAddress().getPort(), "/health");
            assertEquals(200, health.statusCode());
            assertTrue(health.body().contains("\"status\":\"UP\""));
            assertTrue(health.body().contains("health_detail_failed:IllegalStateException"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void readinessFailureAndNullResultAreReportedWithoutFailingLiveness() throws Exception {
        HttpServer server = HealthHttpServer.start(0, "index-node", () -> {
            throw new IllegalStateException("probe failed");
        });
        try {
            int port = server.getAddress().getPort();
            HttpResponse<String> readiness = get(port, "/readyz");
            assertEquals(503, readiness.statusCode());
            assertTrue(readiness.body().contains("readiness_check_failed:IllegalStateException"));

            HttpResponse<String> health = get(port, "/health");
            assertEquals(200, health.statusCode());
            assertFalse(health.body().contains("readiness_check_failed"));
        } finally {
            server.stop(0);
        }

        HttpServer nullResult = HealthHttpServer.start(0, "index-node", () -> null);
        try {
            HttpResponse<String> readiness = get(nullResult.getAddress().getPort(), "/readyz");
            assertEquals(503, readiness.statusCode());
            assertTrue(readiness.body().contains("readiness_check_returned_null"));
        } finally {
            nullResult.stop(0);
        }
    }

    private static HttpResponse<String> get(int port, String path) throws Exception {
        return HttpClient.newHttpClient()
                .send(
                        HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
    }
}
