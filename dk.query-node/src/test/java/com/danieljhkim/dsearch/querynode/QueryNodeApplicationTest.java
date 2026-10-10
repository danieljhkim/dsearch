package com.danieljhkim.dsearch.querynode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.dsearch.common.config.AppConfig;
import com.danieljhkim.dsearch.common.enums.RoutingStrategy;
import com.danieljhkim.dsearch.common.health.HealthHttpServer;
import com.danieljhkim.dsearch.querynode.QueryNodeApplication.QueryNodeRuntime;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class QueryNodeApplicationTest {

    @Test
    void composesRuntimeStartsHealthAndClosesOwnedResources() throws Exception {
        int grpcPort = freePortWithFreeMetricsPort();
        int healthPort = freePort();
        QueryNodeRuntime runtime = QueryNodeApplication.createRuntime(grpcPort, healthPort, validConfig());
        ExecutorService starter = Executors.newSingleThreadExecutor();
        Future<?> started = starter.submit(() -> start(runtime));
        try {
            HttpResponse<String> health = awaitStatus(healthPort, "/health", 200);
            assertEquals(200, health.statusCode());
            assertTrue(health.body().contains("\"service\":\"query-node\""));
            assertEquals(200, awaitStatus(healthPort, "/readyz", 200).statusCode());

            Field managerField = QueryNodeRuntime.class.getDeclaredField("nodeClientManager");
            managerField.setAccessible(true);
            var manager = (com.danieljhkim.dsearch.common.grpc.NodeClientManager<?>) managerField.get(runtime);
            Field activeClients = manager.getClass().getDeclaredField("activeClients");
            activeClients.setAccessible(true);
            activeClients.set(manager, List.of());

            HttpResponse<String> dependencyLoss = awaitStatus(healthPort, "/readyz", 503);
            assertEquals(503, dependencyLoss.statusCode());
            assertTrue(dependencyLoss.body().contains("index_model_or_topology_not_ready"));
            assertEquals(200, awaitStatus(healthPort, "/livez", 200).statusCode());

            activeClients.set(manager, List.copyOf(manager.getClientMap().values()));
            assertEquals(200, awaitStatus(healthPort, "/readyz", 200).statusCode());
        } finally {
            runtime.close();
            started.get(5, TimeUnit.SECONDS);
            starter.shutdownNow();
        }

        assertTrue(allChannelsShutdown(runtime));
        assertTrue(executorShutdown(runtime));
    }

    @Test
    void healthStartupFailureRollsBackClientAndExecutorResources() throws Exception {
        int grpcPort = freePortWithFreeMetricsPort();
        int healthPort = freePort();
        try (var occupied = new ServerSocket(healthPort)) {
            QueryNodeRuntime runtime = QueryNodeApplication.createRuntime(grpcPort, healthPort, validConfig());

            assertThrows(IOException.class, runtime::start);
            assertTrue(allChannelsShutdown(runtime));
            assertTrue(executorShutdown(runtime));
            runtime.close();
        }
    }

    @Test
    void malformedConfigurationAndPortSettingsFailClosed() {
        AppConfig malformed = validConfig();
        malformed.setIndexNodes(null);

        assertThrows(NullPointerException.class, () -> QueryNodeApplication.createRuntime(1, 2, malformed));
        assertThrows(
                IllegalArgumentException.class, () -> QueryNodeApplication.parsePort("QUERY_NODE_PORT", "not-a-port"));
        assertThrows(IllegalArgumentException.class, () -> QueryNodeApplication.parsePort("QUERY_NODE_PORT", ""));
        assertEquals(6000, QueryNodeApplication.parsePort("QUERY_NODE_PORT", "6000"));
    }

    @Test
    void healthServerHandlesGetAndRejectsOtherMethods() throws Exception {
        int port = freePort();
        var server = HealthHttpServer.start(port, "query-node-test");
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpResponse<String> get = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/health"))
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            HttpResponse<String> post = client.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/health"))
                            .POST(HttpRequest.BodyPublishers.noBody())
                            .build(),
                    HttpResponse.BodyHandlers.ofString());

            assertEquals(200, get.statusCode());
            assertEquals(405, post.statusCode());
        } finally {
            server.stop(0);
        }
    }

    private static AppConfig validConfig() {
        AppConfig config = new AppConfig();
        config.getGrpcSecurity().setProfile("local");
        AppConfig.ServiceDiscoveryConfig discovery = new AppConfig.ServiceDiscoveryConfig();
        discovery.setEnabled(false);
        config.setServiceDiscovery(discovery);
        config.setIndexNodes(group("index-1", 5000));
        config.setQueryNodes(group("query-1", 6000));
        config.setCoordinatorNodes(group("coordinator-1", 7000));
        return config;
    }

    private static AppConfig.NodeGroupConfig group(String id, int port) {
        AppConfig.NodeGroupConfig group = new AppConfig.NodeGroupConfig();
        group.setRoutingStrategy(RoutingStrategy.ROUND_ROBIN);
        AppConfig.NodeConfig node = new AppConfig.NodeConfig();
        node.setId(id);
        node.setHost("localhost");
        node.setPort(port);
        node.setHealthPort(port + 100);
        group.setNodes(List.of(node));
        return group;
    }

    private static void start(QueryNodeRuntime runtime) {
        try {
            runtime.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Polls until the endpoint answers with {@code expected}. The health server starts before the
     * runtime finishes wiring, so an early answer may legitimately be 503.
     */
    private static HttpResponse<String> awaitStatus(int port, String path, int expected) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .GET()
                .build();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        HttpResponse<String> last = null;
        IOException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                last = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (last.statusCode() == expected) {
                    return last;
                }
            } catch (IOException e) {
                lastFailure = e;
            }
            Thread.sleep(20);
        }
        if (last != null) {
            return last;
        }
        throw lastFailure;
    }

    private static int freePortWithFreeMetricsPort() throws IOException {
        while (true) {
            int port = freePort();
            if (port > 65535 - 2000) {
                continue;
            }
            try (ServerSocket ignored = new ServerSocket(port + 2000)) {
                return port;
            } catch (IOException e) {
                // Choose another ephemeral pair when the derived metrics port is occupied.
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static boolean allChannelsShutdown(QueryNodeRuntime runtime) throws Exception {
        Field managerField = QueryNodeRuntime.class.getDeclaredField("nodeClientManager");
        managerField.setAccessible(true);
        var manager = (com.danieljhkim.dsearch.common.grpc.NodeClientManager<?>) managerField.get(runtime);
        return manager.getClientMap().values().stream()
                .allMatch(client -> client.getChannel().isShutdown());
    }

    private static boolean executorShutdown(QueryNodeRuntime runtime) throws Exception {
        Field executorField = QueryNodeRuntime.class.getDeclaredField("searchExecutor");
        executorField.setAccessible(true);
        Field shardExecutorField = executorField.getType().getDeclaredField("shardExecutor");
        shardExecutorField.setAccessible(true);
        ExecutorService executor = (ExecutorService) shardExecutorField.get(executorField.get(runtime));
        return executor.isShutdown();
    }
}
