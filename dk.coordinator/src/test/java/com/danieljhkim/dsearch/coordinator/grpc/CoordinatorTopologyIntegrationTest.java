package com.danieljhkim.dsearch.coordinator.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.dsearch.common.cluster.NodeMembershipAgent;
import com.danieljhkim.dsearch.common.config.AppConfig;
import com.danieljhkim.dsearch.common.enums.RoutingStrategy;
import com.danieljhkim.dsearch.common.grpc.GrpcPeerIdentityInterceptor;
import com.danieljhkim.dsearch.common.grpc.GrpcTransportSecurity;
import com.danieljhkim.dsearch.coordinator.cluster.ClusterMembershipService;
import com.danieljhkim.dsearch.proto.cluster.ClusterServiceGrpc;
import com.danieljhkim.dsearch.proto.cluster.GetShardMapRequest;
import com.danieljhkim.dsearch.proto.cluster.GetShardMapResponse;
import com.danieljhkim.dsearch.proto.cluster.NodeRole;
import com.danieljhkim.dsearch.proto.cluster.RegisterNodeRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CoordinatorTopologyIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void transportContractSurvivesRestartExpiresAndReregistersNodesAndRejectsStaleState() throws Exception {
        Path stateFile = tempDir.resolve("coordinator.properties");
        MutableClock clock = new MutableClock(Instant.parse("2026-08-29T00:00:00Z"));
        AppConfig config = appConfig();

        ClusterMembershipService firstMembership = new ClusterMembershipService(config, stateFile, clock);
        RunningCoordinator first = start(firstMembership);
        ClusterServiceGrpc.ClusterServiceBlockingStub firstClient = first.stub();
        firstClient.registerNode(registerRequest());
        GetShardMapResponse beforeRestart = firstClient.getShardMap(GetShardMapRequest.getDefaultInstance());
        assertEquals(
                List.of("index/index-0"),
                beforeRestart.getShardLocationsList().stream()
                        .map(location -> location.getShardId())
                        .toList());
        first.close();

        ClusterMembershipService recoveredMembership = new ClusterMembershipService(config, stateFile, clock);
        RunningCoordinator recovered = start(recoveredMembership);
        ClusterServiceGrpc.ClusterServiceBlockingStub recoveredClient = recovered.stub();
        GetShardMapResponse afterRestart = recoveredClient.getShardMap(GetShardMapRequest.getDefaultInstance());
        assertEquals(beforeRestart.getTopologyEpoch(), afterRestart.getTopologyEpoch());
        assertEquals(beforeRestart.getTopologyVersion(), afterRestart.getTopologyVersion());
        assertEquals(beforeRestart.getShardLocationsList(), afterRestart.getShardLocationsList());

        clock.advanceSeconds(6);
        assertEquals(List.of("NODE_ROLE_INDEX/index-0"), recoveredMembership.expireNodes());
        GetShardMapResponse afterExpiry = recoveredClient.getShardMap(GetShardMapRequest.getDefaultInstance());
        assertEquals(1, afterExpiry.getShardLocationsCount());
        assertFalse(afterExpiry.getShardLocations(0).getEligible());
        assertEquals("unavailable", afterExpiry.getShardLocations(0).getState());
        assertEquals(1, afterExpiry.getUnderReplicatedShards());
        assertTrue(afterExpiry.getTopologyVersion() > afterRestart.getTopologyVersion());

        long expiredVersion = afterExpiry.getTopologyVersion();
        long reregisteredVersion =
                recoveredClient.registerNode(registerRequest()).getTopologyVersion();
        assertTrue(reregisteredVersion > expiredVersion);

        StatusRuntimeException stale = assertThrows(
                StatusRuntimeException.class,
                () -> recoveredClient.getShardMap(GetShardMapRequest.newBuilder()
                        .setMinTopologyVersion(reregisteredVersion + 1)
                        .build()));
        assertEquals(Status.Code.FAILED_PRECONDITION, stale.getStatus().getCode());

        ClusterServiceGrpc.ClusterServiceBlockingStub unavailableClient = recovered.stub();
        recovered.stopServer();
        StatusRuntimeException unavailable = assertThrows(
                StatusRuntimeException.class,
                () -> unavailableClient
                        .withDeadlineAfter(250, TimeUnit.MILLISECONDS)
                        .getShardMap(GetShardMapRequest.getDefaultInstance()));
        assertEquals(Status.Code.UNAVAILABLE, unavailable.getStatus().getCode());
        recovered.closeChannel();
    }

    @Test
    void runningNodeAutomaticallyRejoinsSameIdentityAfterCoordinatorDowntimeExceedsLease() throws Exception {
        Path stateFile = tempDir.resolve("membership-agent-coordinator.properties");
        MutableClock clock = new MutableClock(Instant.parse("2026-08-29T00:00:00Z"));
        AppConfig config = appConfig();
        ClusterMembershipService firstMembership = new ClusterMembershipService(config, stateFile, clock);
        RunningCoordinator first = start(firstMembership);
        int coordinatorPort = first.server().getPort();
        NodeMembershipAgent agent = membershipAgent(coordinatorPort);
        RunningCoordinator recovered = null;
        try {
            agent.start();
            awaitMember(firstMembership, "index-agent-0", Duration.ofSeconds(5));
            long firstRegistrationVersion = firstMembership.getTopologyVersion();

            first.close();
            clock.advanceSeconds(6);
            ClusterMembershipService recoveredMembership = new ClusterMembershipService(config, stateFile, clock);
            assertEquals(List.of("NODE_ROLE_INDEX/index-agent-0"), recoveredMembership.expireNodes());
            long expiredVersion = recoveredMembership.getTopologyVersion();

            recovered = start(recoveredMembership, coordinatorPort);
            awaitMember(recoveredMembership, "index-agent-0", Duration.ofSeconds(5));

            assertTrue(expiredVersion > firstRegistrationVersion);
            assertTrue(recoveredMembership.getTopologyVersion() > expiredVersion);
            assertEquals(
                    "localhost",
                    recoveredMembership.getIndexGroup().getNode("index-agent-0").getHost());
        } finally {
            agent.close();
            if (!first.server().isShutdown()) {
                first.close();
            }
            if (recovered != null) {
                recovered.close();
            }
        }
    }

    private static RunningCoordinator start(ClusterMembershipService membershipService) throws IOException {
        return start(membershipService, 0);
    }

    private static RunningCoordinator start(ClusterMembershipService membershipService, int port) throws IOException {
        AppConfig transportConfig = new AppConfig();
        transportConfig.getGrpcSecurity().setProfile("local");
        GrpcTransportSecurity transportSecurity = GrpcTransportSecurity.from(transportConfig);
        Server server = ServerBuilder.forPort(port)
                .addService(new ClusterServiceImpl(membershipService))
                .intercept(new GrpcPeerIdentityInterceptor(transportSecurity))
                .build()
                .start();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        return new RunningCoordinator(server, channel);
    }

    private static NodeMembershipAgent membershipAgent(int coordinatorPort) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", coordinatorPort)
                .usePlaintext()
                .build();
        NodeMembershipAgent.Settings settings = new NodeMembershipAgent.Settings(
                "localhost",
                coordinatorPort,
                Duration.ofMillis(25),
                Duration.ofMillis(10),
                Duration.ofMillis(100),
                Duration.ofMillis(250),
                Duration.ofMillis(100));
        NodeMembershipAgent.NodeIdentity identity = new NodeMembershipAgent.NodeIdentity(
                "index-agent-0", "localhost", 5000, 5100, NodeRole.NODE_ROLE_INDEX);
        return new NodeMembershipAgent(identity, settings, ClusterServiceGrpc.newBlockingStub(channel), channel);
    }

    private static void awaitMember(ClusterMembershipService membershipService, String nodeId, Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (membershipService.getIndexGroup().getNode(nodeId) != null) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Node did not register before timeout: " + nodeId);
    }

    private static RegisterNodeRequest registerRequest() {
        return RegisterNodeRequest.newBuilder()
                .setNodeId("index-0")
                .setHost("localhost")
                .setPort(5000)
                .setHealthPort(5100)
                .setRole(NodeRole.NODE_ROLE_INDEX)
                .build();
    }

    private static AppConfig appConfig() {
        AppConfig config = new AppConfig();
        AppConfig.ServiceDiscoveryConfig discovery = new AppConfig.ServiceDiscoveryConfig();
        discovery.setEnabled(true);
        discovery.setNodeExpirySeconds(5);
        discovery.setMaxStalenessSeconds(5);
        config.setServiceDiscovery(discovery);
        config.setIndexNodes(nodeGroup("index-nodes", RoutingStrategy.ROUND_ROBIN));
        config.setQueryNodes(nodeGroup("query-nodes", RoutingStrategy.ROUND_ROBIN));
        config.setCoordinatorNodes(nodeGroup("coordinator-nodes", RoutingStrategy.ROUND_ROBIN));
        return config;
    }

    private static AppConfig.NodeGroupConfig nodeGroup(String label, RoutingStrategy strategy) {
        AppConfig.NodeGroupConfig group = new AppConfig.NodeGroupConfig();
        group.setComponentLabel(label);
        group.setRoutingStrategy(strategy);
        group.setNodes(List.of());
        return group;
    }

    private record RunningCoordinator(Server server, ManagedChannel channel) implements AutoCloseable {
        ClusterServiceGrpc.ClusterServiceBlockingStub stub() {
            return ClusterServiceGrpc.newBlockingStub(channel);
        }

        void stopServer() throws InterruptedException {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }

        void closeChannel() throws InterruptedException {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws InterruptedException {
            stopServer();
            closeChannel();
        }
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        private void advanceSeconds(long seconds) {
            instant = instant.plusSeconds(seconds);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
