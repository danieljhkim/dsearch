package com.danieljhkim.dsearch.coordinator.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.dsearch.common.cluster.NodeGroup;
import com.danieljhkim.dsearch.common.config.AppConfig;
import com.danieljhkim.dsearch.common.enums.RoutingStrategy;
import com.danieljhkim.dsearch.proto.cluster.NodeRole;
import com.danieljhkim.dsearch.proto.cluster.ReplicaRepairState;
import com.danieljhkim.dsearch.proto.cluster.ReplicaRepairStatus;
import com.danieljhkim.dsearch.proto.index.BeginReplicaRepairRequest;
import com.danieljhkim.dsearch.proto.index.BeginReplicaRepairResponse;
import com.danieljhkim.dsearch.proto.index.FinishReplicaRepairRequest;
import com.danieljhkim.dsearch.proto.index.FinishReplicaRepairResponse;
import com.danieljhkim.dsearch.proto.index.IndexServiceGrpc;
import com.danieljhkim.dsearch.proto.index.ListReplicaManifestsRequest;
import com.danieljhkim.dsearch.proto.index.ListReplicaManifestsResponse;
import com.danieljhkim.dsearch.proto.index.OpenReplicaSnapshotRequest;
import com.danieljhkim.dsearch.proto.index.OpenReplicaSnapshotResponse;
import com.danieljhkim.dsearch.proto.index.ReadReplicaSnapshotChunkRequest;
import com.danieljhkim.dsearch.proto.index.ReadReplicaSnapshotChunkResponse;
import com.danieljhkim.dsearch.proto.index.ReplicaManifest;
import com.danieljhkim.dsearch.proto.index.WriteReplicaRepairChunkRequest;
import com.danieljhkim.dsearch.proto.index.WriteReplicaRepairChunkResponse;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ReplicaRepairCoordinatorTest {

    @Test
    void classifiesEveryDivergenceWithoutTrustingProcessHealth() {
        ReplicaManifest source = manifest(9, 42, "good");

        assertEquals(ReplicaRepairState.REPLICA_REPAIR_STATE_MISSING, ReplicaRepairCoordinator.classify(source, null));
        assertEquals(
                ReplicaRepairState.REPLICA_REPAIR_STATE_WRONG_GENERATION,
                ReplicaRepairCoordinator.classify(source, manifest(8, 42, "good")));
        assertEquals(
                ReplicaRepairState.REPLICA_REPAIR_STATE_LAGGING,
                ReplicaRepairCoordinator.classify(source, manifest(9, 41, "old")));
        assertEquals(
                ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKSUM_DIVERGENT,
                ReplicaRepairCoordinator.classify(source, manifest(9, 42, "corrupt")));
        assertEquals(
                ReplicaRepairState.REPLICA_REPAIR_STATE_READY,
                ReplicaRepairCoordinator.classify(source, manifest(9, 42, "good")));
    }

    @Test
    void failedManifestInspectionCannotBootstrapAnEmptyLookingCluster() throws Exception {
        try (TestCluster cluster = new TestCluster(null, null, new byte[0])) {
            cluster.source.failManifestInspection = true;

            cluster.coordinator.reconcile();

            assertEquals(
                    ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKING, cluster.membership.replicaRepairState("source"));
            assertEquals(
                    ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKING, cluster.membership.replicaRepairState("target"));
            assertFalse(cluster.membership.isReplicaEligible("source"));
            assertFalse(cluster.membership.isReplicaEligible("target"));
        }
    }

    @Test
    void fullyInspectedEmptyClusterCanBootstrapItsNodes() throws Exception {
        try (TestCluster cluster = new TestCluster(null, null, new byte[0])) {
            cluster.coordinator.reconcile();

            assertEquals(
                    ReplicaRepairState.REPLICA_REPAIR_STATE_READY, cluster.membership.replicaRepairState("source"));
            assertEquals(
                    ReplicaRepairState.REPLICA_REPAIR_STATE_READY, cluster.membership.replicaRepairState("target"));
            assertTrue(cluster.membership.isReplicaEligible("source"));
            assertTrue(cluster.membership.isReplicaEligible("target"));
        }
    }

    @Test
    void transfersBoundedChunksAndRequiresACompleteRecheckBeforeEligibility() throws Exception {
        byte[] snapshot = "snapshot".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (TestCluster cluster = new TestCluster(manifest(9, 42, "good"), null, snapshot)) {
            cluster.coordinator.reconcile();

            ReplicaRepairStatus completed = onlyRepair(cluster.membership);
            assertEquals(ReplicaRepairState.REPLICA_REPAIR_STATE_READY, completed.getState());
            assertEquals(1, completed.getAttempts());
            assertEquals(snapshot.length, completed.getBytesTransferred());
            assertEquals(snapshot.length, completed.getTotalBytes());
            assertEquals(List.of(0L, 3L, 6L), cluster.source.chunkOffsets);
            assertTrue(cluster.source.maxChunkBytes.stream().allMatch(maxBytes -> maxBytes == 3));
            assertEquals(64, cluster.source.maxSnapshotBytes);
            assertArrayEquals(snapshot, cluster.target.received.toByteArray());
            assertEquals(1, cluster.target.finishCalls);

            assertFalse(cluster.membership.isReplicaEligible("target"));
            assertEquals(
                    ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKING, cluster.membership.replicaRepairState("target"));

            cluster.coordinator.reconcile();

            assertTrue(cluster.membership.isReplicaEligible("source"));
            assertTrue(cluster.membership.isReplicaEligible("target"));
            assertEquals(1, cluster.target.finishCalls);
        }
    }

    @Test
    void failedVerificationStaysIneligibleAndAReconciliationRetryConverges() throws Exception {
        byte[] snapshot = "retryable-snapshot".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (TestCluster cluster = new TestCluster(manifest(9, 42, "good"), null, snapshot)) {
            cluster.target.finishOverride = manifest(9, 42, "wrong-checksum");

            cluster.coordinator.reconcile();

            ReplicaRepairStatus failed = onlyRepair(cluster.membership);
            assertEquals(ReplicaRepairState.REPLICA_REPAIR_STATE_FAILED, failed.getState());
            assertEquals(1, failed.getAttempts());
            assertTrue(failed.getLastError().contains("did not converge"));
            assertEquals(
                    ReplicaRepairState.REPLICA_REPAIR_STATE_FAILED, cluster.membership.replicaRepairState("target"));
            assertFalse(cluster.membership.isReplicaEligible("target"));

            cluster.target.finishOverride = null;
            cluster.coordinator.reconcile();

            ReplicaRepairStatus retried = onlyRepair(cluster.membership);
            assertEquals(ReplicaRepairState.REPLICA_REPAIR_STATE_READY, retried.getState());
            assertEquals(2, retried.getAttempts());
            assertEquals(2, cluster.target.beginCalls);
            assertEquals(2, cluster.target.finishCalls);
            assertFalse(cluster.membership.isReplicaEligible("target"));

            cluster.coordinator.reconcile();

            assertTrue(cluster.membership.isReplicaEligible("target"));
            assertEquals(2, cluster.target.finishCalls);
        }
    }

    @Test
    void equalPerDocumentMaximumCannotOverwriteTheOnlyCompleteCopy() throws Exception {
        ReplicaManifest incompletePrimary =
                manifest(9, 1, "document-A").toBuilder().setDocumentCount(1).build();
        ReplicaManifest completeFollower = manifest(9, 1, "documents-A-and-B").toBuilder()
                .setDocumentCount(2)
                .build();
        byte[] completeSnapshot = "A,B".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (TestCluster cluster = new TestCluster(incompletePrimary, completeFollower, "A".getBytes())) {
            cluster.target.snapshot = completeSnapshot;

            cluster.coordinator.reconcile();
            cluster.coordinator.reconcile();

            assertEquals(0, cluster.source.beginCalls);
            assertEquals(0, cluster.target.beginCalls);
            assertArrayEquals(completeSnapshot, cluster.target.snapshot);
            assertEquals(completeFollower, cluster.target.manifest);
            assertEquals(
                    ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKSUM_DIVERGENT,
                    cluster.membership.replicaRepairState("source"));
            assertEquals(
                    ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKSUM_DIVERGENT,
                    cluster.membership.replicaRepairState("target"));
            assertFalse(cluster.membership.isReplicaEligible("source"));
            assertFalse(cluster.membership.isReplicaEligible("target"));
            assertEquals(
                    ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKSUM_DIVERGENT,
                    onlyRepair(cluster.membership).getState());
        }
    }

    @Test
    void higherPerDocumentMaximumDoesNotProveShardHistoryOrdering() throws Exception {
        try (TestCluster cluster =
                new TestCluster(manifest(9, 2, "document-A"), manifest(9, 1, "documents-A-and-B"), "A".getBytes())) {
            cluster.coordinator.reconcile();

            assertEquals(0, cluster.source.beginCalls);
            assertEquals(0, cluster.target.beginCalls);
            assertEquals(
                    ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKSUM_DIVERGENT,
                    cluster.membership.replicaRepairState("source"));
            assertEquals(
                    ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKSUM_DIVERGENT,
                    cluster.membership.replicaRepairState("target"));
        }
    }

    private static ReplicaRepairStatus onlyRepair(ClusterMembershipService membership) {
        assertEquals(1, membership.repairStatuses().size());
        return membership.repairStatuses().getFirst();
    }

    private static ReplicaManifest manifest(long generation, long position, String checksum) {
        return ReplicaManifest.newBuilder()
                .setShardId("tenant_r1")
                .setLogicalPartitionId("tenant")
                .setPrimaryNodeId("source")
                .setPlacementGeneration(generation)
                .setCommittedPosition(position)
                .setContentChecksum(checksum)
                .build();
    }

    private static AppConfig config(int sourcePort, int targetPort) {
        AppConfig config = new AppConfig();
        config.getGrpcSecurity().setProfile("local");
        AppConfig.NodeGroupConfig indexNodes =
                group("index", List.of(node("source", sourcePort), node("target", targetPort)));
        indexNodes.setReplicationFactor(2);
        indexNodes.setDurabilityPolicy("all");
        config.setIndexNodes(indexNodes);
        config.setQueryNodes(group("query", List.of()));
        config.setCoordinatorNodes(group("coordinator", List.of()));
        config.getReplicaRepair().setRpcDeadlineMillis(2_000);
        config.getReplicaRepair().setChunkBytes(3);
        config.getReplicaRepair().setMaxSnapshotBytes(64);
        config.getReplicaRepair().setBandwidthBytesPerSecond(0);
        return config;
    }

    private static AppConfig.NodeGroupConfig group(String label, List<AppConfig.NodeConfig> nodes) {
        AppConfig.NodeGroupConfig group = new AppConfig.NodeGroupConfig();
        group.setComponentLabel(label);
        group.setRoutingStrategy(RoutingStrategy.ROUND_ROBIN);
        group.setNodes(nodes);
        return group;
    }

    private static AppConfig.NodeConfig node(String id, int port) {
        AppConfig.NodeConfig node = new AppConfig.NodeConfig();
        node.setId(id);
        node.setHost("localhost");
        node.setPort(port);
        node.setHealthPort(port);
        node.setRole("INDEX");
        return node;
    }

    private static NodeGroup.NodeInfo member(String id, int port) {
        return new NodeGroup.NodeInfo(id, "localhost", port, port, "NODE_ROLE_INDEX", true);
    }

    private static final class TestCluster implements AutoCloseable {
        private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-06T00:00:00Z"), ZoneOffset.UTC);

        private final RepairService source;
        private final RepairService target;
        private final RunningNode sourceNode;
        private final RunningNode targetNode;
        private final ClusterMembershipService membership;
        private final ReplicaRepairCoordinator coordinator;

        private TestCluster(ReplicaManifest sourceManifest, ReplicaManifest targetManifest, byte[] snapshot)
                throws IOException {
            source = new RepairService(sourceManifest, snapshot);
            target = new RepairService(targetManifest, new byte[0]);
            sourceNode = RunningNode.start(source);
            targetNode = RunningNode.start(target);
            AppConfig config = config(sourceNode.port(), targetNode.port());
            membership = new ClusterMembershipService(config);
            membership.registerNode(member("source", sourceNode.port()), NodeRole.NODE_ROLE_INDEX);
            membership.registerNode(member("target", targetNode.port()), NodeRole.NODE_ROLE_INDEX);
            coordinator = new ReplicaRepairCoordinator(membership, config, CLOCK);
        }

        @Override
        public void close() throws Exception {
            coordinator.close();
            sourceNode.close();
            targetNode.close();
        }
    }

    private static final class RepairService extends IndexServiceGrpc.IndexServiceImplBase {
        private ReplicaManifest manifest;
        private byte[] snapshot;
        private ReplicaManifest acceptedManifest;
        private ReplicaManifest finishOverride;
        private final ByteArrayOutputStream received = new ByteArrayOutputStream();
        private final List<Long> chunkOffsets = new ArrayList<>();
        private final List<Integer> maxChunkBytes = new ArrayList<>();
        private long maxSnapshotBytes;
        private int beginCalls;
        private int finishCalls;
        private boolean failManifestInspection;

        private RepairService(ReplicaManifest manifest, byte[] snapshot) {
            this.manifest = manifest;
            this.snapshot = snapshot;
        }

        @Override
        public void listReplicaManifests(
                ListReplicaManifestsRequest request, StreamObserver<ListReplicaManifestsResponse> observer) {
            if (failManifestInspection) {
                observer.onError(Status.UNAVAILABLE
                        .withDescription("manifest inspection unavailable")
                        .asRuntimeException());
                return;
            }
            ListReplicaManifestsResponse.Builder response = ListReplicaManifestsResponse.newBuilder();
            if (manifest != null) {
                response.addManifests(manifest);
            }
            observer.onNext(response.build());
            observer.onCompleted();
        }

        @Override
        public void openReplicaSnapshot(
                OpenReplicaSnapshotRequest request, StreamObserver<OpenReplicaSnapshotResponse> observer) {
            maxSnapshotBytes = request.getMaxSnapshotBytes();
            observer.onNext(OpenReplicaSnapshotResponse.newBuilder()
                    .setSnapshotId("snapshot-1")
                    .setTotalBytes(snapshot.length)
                    .setTransferChecksum("transfer-checksum")
                    .setManifest(manifest)
                    .build());
            observer.onCompleted();
        }

        @Override
        public void readReplicaSnapshotChunk(
                ReadReplicaSnapshotChunkRequest request, StreamObserver<ReadReplicaSnapshotChunkResponse> observer) {
            chunkOffsets.add(request.getOffset());
            maxChunkBytes.add(request.getMaxBytes());
            int offset = Math.toIntExact(request.getOffset());
            int length = Math.min(request.getMaxBytes(), snapshot.length - offset);
            observer.onNext(ReadReplicaSnapshotChunkResponse.newBuilder()
                    .setOffset(offset)
                    .setData(ByteString.copyFrom(snapshot, offset, length))
                    .setComplete(offset + length == snapshot.length)
                    .build());
            observer.onCompleted();
        }

        @Override
        public void beginReplicaRepair(
                BeginReplicaRepairRequest request, StreamObserver<BeginReplicaRepairResponse> observer) {
            beginCalls++;
            acceptedManifest = request.getManifest();
            received.reset();
            observer.onNext(
                    BeginReplicaRepairResponse.newBuilder().setAcceptedOffset(0).build());
            observer.onCompleted();
        }

        @Override
        public void writeReplicaRepairChunk(
                WriteReplicaRepairChunkRequest request, StreamObserver<WriteReplicaRepairChunkResponse> observer) {
            assertEquals(received.size(), request.getOffset());
            received.writeBytes(request.getData().toByteArray());
            observer.onNext(WriteReplicaRepairChunkResponse.newBuilder()
                    .setAcceptedOffset(received.size())
                    .build());
            observer.onCompleted();
        }

        @Override
        public void finishReplicaRepair(
                FinishReplicaRepairRequest request, StreamObserver<FinishReplicaRepairResponse> observer) {
            finishCalls++;
            ReplicaManifest finished = finishOverride == null ? acceptedManifest : finishOverride;
            if (finishOverride == null) {
                manifest = acceptedManifest;
            }
            observer.onNext(FinishReplicaRepairResponse.newBuilder()
                    .setManifest(finished)
                    .build());
            observer.onCompleted();
        }
    }

    private record RunningNode(Server server) implements AutoCloseable {
        private static RunningNode start(RepairService service) throws IOException {
            return new RunningNode(
                    ServerBuilder.forPort(0).addService(service).build().start());
        }

        private int port() {
            return server.getPort();
        }

        @Override
        public void close() throws InterruptedException {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
