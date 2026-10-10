package com.danieljhkim.dsearch.coordinator.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.dsearch.common.cluster.NodeMembershipAgent;
import com.danieljhkim.dsearch.proto.cluster.ClusterServiceGrpc;
import com.danieljhkim.dsearch.proto.cluster.GetShardMapRequest;
import com.danieljhkim.dsearch.proto.cluster.GetShardMapResponse;
import com.danieljhkim.dsearch.proto.cluster.NodeRole;
import com.danieljhkim.dsearch.proto.cluster.RegisterNodeRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CoordinatorProcessIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void separateCoordinatorProcessPreservesAndAdvancesAuthoritativeTopology() throws Exception {
        int grpcPort = freePort();
        int healthPort = freePort();
        while (healthPort == grpcPort) {
            healthPort = freePort();
        }
        Path stateFile = tempDir.resolve("process-coordinator.properties");
        Path logFile = tempDir.resolve("process-coordinator.log");
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", grpcPort)
                .usePlaintext()
                .build();
        ClusterServiceGrpc.ClusterServiceBlockingStub client = ClusterServiceGrpc.newBlockingStub(channel);

        Process first = null;
        Process recovered = null;
        NodeMembershipAgent firstNode = null;
        NodeMembershipAgent restartedNode = null;
        try {
            first = startCoordinator(grpcPort, healthPort, stateFile, logFile);
            waitFor(client, response -> true, Duration.ofSeconds(10));
            firstNode = membershipAgent(grpcPort);
            firstNode.start();
            GetShardMapResponse beforeRestart =
                    waitFor(client, response -> response.getShardLocationsCount() == 1, Duration.ofSeconds(10));
            waitForRegistration(firstNode, Duration.ofSeconds(5));
            assertEquals(
                    "index/process-index-0", beforeRestart.getShardLocations(0).getShardId());

            stop(first);
            first = null;
            firstNode.close();
            firstNode = null;
            StatusRuntimeException unavailable = assertThrows(
                    StatusRuntimeException.class,
                    () -> client.withDeadlineAfter(250, TimeUnit.MILLISECONDS)
                            .getShardMap(GetShardMapRequest.getDefaultInstance()));
            assertEquals(Status.Code.UNAVAILABLE, unavailable.getStatus().getCode());

            Thread.sleep(1200);

            recovered = startCoordinator(grpcPort, healthPort, stateFile, logFile);
            GetShardMapResponse afterExpiry = waitFor(
                    client,
                    response -> response.getShardLocationsCount() == 1
                            && !response.getShardLocations(0).getEligible()
                            && response.getUnderReplicatedShards() == 1
                            && response.getTopologyVersion() > beforeRestart.getTopologyVersion(),
                    Duration.ofSeconds(10));
            assertEquals(beforeRestart.getTopologyEpoch(), afterExpiry.getTopologyEpoch());

            restartedNode = membershipAgent(grpcPort);
            restartedNode.start();
            GetShardMapResponse afterRejoin = waitFor(
                    client,
                    response -> response.getShardLocationsCount() == 1
                            && response.getTopologyVersion() > afterExpiry.getTopologyVersion(),
                    Duration.ofSeconds(10));
            waitForRegistration(restartedNode, Duration.ofSeconds(5));
            assertEquals(
                    "index/process-index-0", afterRejoin.getShardLocations(0).getShardId());

            long observedVersion = afterRejoin.getTopologyVersion();
            client.registerNode(RegisterNodeRequest.newBuilder()
                    .setNodeId("process-index-1")
                    .setHost("localhost")
                    .setPort(5001)
                    .setHealthPort(5101)
                    .setRole(NodeRole.NODE_ROLE_INDEX)
                    .setObservedTopologyEpoch(afterRejoin.getTopologyEpoch())
                    .setObservedTopologyVersion(observedVersion)
                    .build());

            GetShardMapResponse afterControlledAdvance = client.getShardMap(GetShardMapRequest.newBuilder()
                    .setMinTopologyVersion(observedVersion)
                    .build());
            assertTrue(afterControlledAdvance.getTopologyVersion() > observedVersion);

            StatusRuntimeException stale = assertThrows(
                    StatusRuntimeException.class,
                    () -> client.getShardMap(GetShardMapRequest.newBuilder()
                            // The generated Java API represents uint64 as long; use its highest
                            // non-negative value so live topology mutations cannot satisfy this request.
                            .setMinTopologyVersion(Long.MAX_VALUE)
                            .build()));
            assertEquals(Status.Code.FAILED_PRECONDITION, stale.getStatus().getCode());
        } finally {
            if (firstNode != null) {
                firstNode.close();
            }
            if (restartedNode != null) {
                restartedNode.close();
            }
            if (first != null) {
                stop(first);
            }
            if (recovered != null) {
                stop(recovered);
            }
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private Process startCoordinator(int grpcPort, int healthPort, Path stateFile, Path logFile) throws IOException {
        String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        ProcessBuilder builder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                classPath,
                "com.danieljhkim.dsearch.coordinator.CoordinatorApplication");
        builder.environment().put("APP_CONFIG_PATH", "process-app-config.yaml");
        builder.environment().put("DSEARCH_GRPC_PROFILE", "local");
        builder.environment().put("COORDINATOR_PORT", Integer.toString(grpcPort));
        builder.environment().put("COORDINATOR_HEALTH_PORT", Integer.toString(healthPort));
        builder.environment().put("COORDINATOR_STATE_FILE", stateFile.toString());
        builder.redirectErrorStream(true);
        builder.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile.toFile()));
        return builder.start();
    }

    private static GetShardMapResponse waitFor(
            ClusterServiceGrpc.ClusterServiceBlockingStub client,
            Predicate<GetShardMapResponse> predicate,
            Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        StatusRuntimeException lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                GetShardMapResponse response = client.withDeadlineAfter(500, TimeUnit.MILLISECONDS)
                        .getShardMap(GetShardMapRequest.getDefaultInstance());
                if (predicate.test(response)) {
                    return response;
                }
            } catch (StatusRuntimeException e) {
                lastFailure = e;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("Coordinator did not reach expected topology before timeout", lastFailure);
    }

    private static NodeMembershipAgent membershipAgent(int coordinatorPort) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", coordinatorPort)
                .usePlaintext()
                .build();
        NodeMembershipAgent.Settings settings = new NodeMembershipAgent.Settings(
                "localhost",
                coordinatorPort,
                Duration.ofMillis(100),
                Duration.ofMillis(25),
                Duration.ofMillis(250),
                Duration.ofMillis(250),
                Duration.ofMillis(100));
        NodeMembershipAgent.NodeIdentity identity = new NodeMembershipAgent.NodeIdentity(
                "process-index-0", "localhost", 5000, 5100, NodeRole.NODE_ROLE_INDEX);
        return new NodeMembershipAgent(identity, settings, ClusterServiceGrpc.newBlockingStub(channel), channel);
    }

    private static void waitForRegistration(NodeMembershipAgent agent, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (agent.isRegistered()) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Membership agent did not observe registration before timeout");
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void stop(Process process) throws InterruptedException {
        process.destroy();
        if (!process.waitFor(5, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            assertTrue(process.waitFor(5, TimeUnit.SECONDS));
        }
    }
}
