package com.danieljhkim.dsearch.coordinator.cluster;

import com.danieljhkim.dsearch.common.config.AppConfig;
import com.danieljhkim.dsearch.common.grpc.GrpcTransportSecurity;
import com.danieljhkim.dsearch.common.shard.ReplicaPlacement;
import com.danieljhkim.dsearch.proto.cluster.ReplicaRepairState;
import com.danieljhkim.dsearch.proto.cluster.ReplicaRepairStatus;
import com.danieljhkim.dsearch.proto.index.BeginReplicaRepairRequest;
import com.danieljhkim.dsearch.proto.index.FinishReplicaRepairRequest;
import com.danieljhkim.dsearch.proto.index.IndexServiceGrpc;
import com.danieljhkim.dsearch.proto.index.ListReplicaManifestsRequest;
import com.danieljhkim.dsearch.proto.index.OpenReplicaSnapshotRequest;
import com.danieljhkim.dsearch.proto.index.ReadReplicaSnapshotChunkRequest;
import com.danieljhkim.dsearch.proto.index.ReplicaManifest;
import com.danieljhkim.dsearch.proto.index.WriteReplicaRepairChunkRequest;
import io.grpc.ManagedChannel;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Coordinator-owned replica comparison and bounded full-snapshot repair loop. */
public final class ReplicaRepairCoordinator implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(ReplicaRepairCoordinator.class.getName());
    private static final Counter OUTCOMES = Counter.build()
            .name("dsearch_replica_repair_outcomes_total")
            .help("Replica repair attempts by bounded outcome")
            .labelNames("outcome")
            .register();
    private static final Histogram DURATION = Histogram.build()
            .name("dsearch_replica_repair_duration_seconds")
            .help("End-to-end replica repair duration")
            .register();
    private static final Gauge ACTIVE = Gauge.build()
            .name("dsearch_replica_repairs_active")
            .help("Replica repairs currently transferring or verifying")
            .register();
    private static final Gauge REMAINING = Gauge.build()
            .name("dsearch_replica_repairs_remaining")
            .help("Replica copies that are missing, lagging, wrong-generation, or checksum-divergent")
            .register();

    private final ClusterMembershipService membership;
    private final AppConfig.ReplicaRepairConfig config;
    private final Map<String, NodeEndpoint> nodes = new HashMap<>();
    private final List<String> configuredNodeIds;
    private final Clock clock;

    public ReplicaRepairCoordinator(ClusterMembershipService membership, AppConfig appConfig) {
        this(membership, appConfig, Clock.systemUTC());
    }

    ReplicaRepairCoordinator(ClusterMembershipService membership, AppConfig appConfig, Clock clock) {
        this.membership = Objects.requireNonNull(membership, "membership");
        this.config = appConfig.getReplicaRepair() == null
                ? new AppConfig.ReplicaRepairConfig()
                : appConfig.getReplicaRepair();
        this.clock = Objects.requireNonNull(clock, "clock");
        GrpcTransportSecurity transport = GrpcTransportSecurity.from(appConfig);
        List<String> ids = new ArrayList<>();
        if (appConfig.getIndexNodes() != null && appConfig.getIndexNodes().getNodes() != null) {
            for (AppConfig.NodeConfig node : appConfig.getIndexNodes().getNodes()) {
                ManagedChannel channel = transport.newChannel(node.getHost(), node.getPort());
                nodes.put(node.getId(), new NodeEndpoint(channel, IndexServiceGrpc.newBlockingStub(channel)));
                ids.add(node.getId());
            }
        }
        this.configuredNodeIds = ids.stream().sorted().toList();
    }

    public int intervalSeconds() {
        return Math.max(1, config.getIntervalSeconds());
    }

    public void reconcile() {
        if (!config.isEnabled() || membership.getReplicationFactor() <= 1) {
            configuredNodeIds.forEach(
                    node -> membership.updateReplicaNodeState(node, ReplicaRepairState.REPLICA_REPAIR_STATE_READY));
            REMAINING.set(0);
            return;
        }
        if (membership.repairsPaused()) {
            return;
        }

        Map<String, Map<String, ReplicaManifest>> manifests = inspectHealthyNodes();
        Map<String, List<NodeManifest>> shards = groupByShard(manifests);
        if (shards.isEmpty()) {
            // Only a fully inspected empty cluster is known to be new. An unavailable node may
            // still hold the only acknowledged copy.
            configuredNodeIds.forEach(node -> membership.updateReplicaNodeState(
                    node,
                    manifests.size() == configuredNodeIds.size()
                            ? ReplicaRepairState.REPLICA_REPAIR_STATE_READY
                            : ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKING));
            REMAINING.set(0);
            return;
        }

        Map<String, ReplicaRepairState> nodeStates = new HashMap<>();
        configuredNodeIds.forEach(node -> nodeStates.put(
                node,
                manifests.containsKey(node)
                        ? ReplicaRepairState.REPLICA_REPAIR_STATE_READY
                        : ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKING));
        List<RepairCandidate> candidates = new ArrayList<>();
        int ambiguousCopies = 0;
        for (Map.Entry<String, List<NodeManifest>> entry : shards.entrySet()) {
            ReplicaManifest source = authoritative(entry.getValue());
            if (source == null) {
                // A per-document generation maximum is not a shard history. Neither a larger
                // maximum nor the declared primary proves that it contains the other copy's
                // acknowledged mutations. Preserve every existing copy for investigation.
                for (NodeManifest copy : entry.getValue()) {
                    nodeStates.put(copy.nodeId(), ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKSUM_DIVERGENT);
                }
                ambiguousCopies += entry.getValue().size();
                membership.recordRepairStatus(ReplicaRepairStatus.newBuilder()
                        .setRepairId("ambiguous-" + entry.getKey())
                        .setShardId(entry.getKey())
                        .setState(ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKSUM_DIVERGENT)
                        .setLastError(
                                "Replica histories cannot be ordered from their manifests; manual resolution required")
                        .setUpdatedAtEpochMillis(clock.millis())
                        .build());
                continue;
            }
            String sourceNode = sourceNode(entry.getValue(), source);
            Set<String> expected = expectedNodes(source);
            for (String target : expected) {
                if (!manifests.containsKey(target)) {
                    // A failed inspection is not evidence that this replica is missing.
                    continue;
                }
                ReplicaManifest actual =
                        manifests.getOrDefault(target, Map.of()).get(entry.getKey());
                ReplicaRepairState state = classify(source, actual);
                if (state != ReplicaRepairState.REPLICA_REPAIR_STATE_READY) {
                    nodeStates.computeIfPresent(
                            target,
                            (ignored, current) -> current == ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKSUM_DIVERGENT
                                    ? current
                                    : ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKING);
                    candidates.add(new RepairCandidate(entry.getKey(), sourceNode, target, source, state));
                }
            }
        }
        REMAINING.set(candidates.size() + ambiguousCopies);
        for (Map.Entry<String, ReplicaRepairState> entry : nodeStates.entrySet()) {
            membership.updateReplicaNodeState(entry.getKey(), entry.getValue());
        }

        int admitted = Math.max(1, config.getMaxConcurrentRepairs());
        for (RepairCandidate candidate : candidates.stream().limit(admitted).toList()) {
            repair(candidate);
        }
    }

    private Map<String, Map<String, ReplicaManifest>> inspectHealthyNodes() {
        Map<String, Map<String, ReplicaManifest>> result = new HashMap<>();
        Set<String> healthy = new HashSet<>(membership.getIndexGroup().getAllNodes().stream()
                .filter(com.danieljhkim.dsearch.common.cluster.NodeGroup.NodeInfo::isHealthy)
                .map(com.danieljhkim.dsearch.common.cluster.NodeGroup.NodeInfo::getNodeId)
                .toList());
        for (String nodeId : configuredNodeIds) {
            if (!healthy.contains(nodeId)) {
                membership.updateReplicaNodeState(nodeId, ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKING);
                continue;
            }
            try {
                var response = stub(nodeId).listReplicaManifests(ListReplicaManifestsRequest.getDefaultInstance());
                Map<String, ReplicaManifest> byShard = new HashMap<>();
                for (ReplicaManifest manifest : response.getManifestsList()) {
                    byShard.put(manifest.getShardId(), manifest);
                }
                result.put(nodeId, byShard);
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "Failed to inspect replica manifests on node " + nodeId, e);
                membership.updateReplicaNodeState(nodeId, ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKING);
                OUTCOMES.labels("inspection_failed").inc();
            }
        }
        return result;
    }

    private Map<String, List<NodeManifest>> groupByShard(Map<String, Map<String, ReplicaManifest>> manifests) {
        Map<String, List<NodeManifest>> result = new HashMap<>();
        manifests.forEach((node, byShard) -> byShard.forEach((shard, manifest) ->
                result.computeIfAbsent(shard, ignored -> new ArrayList<>()).add(new NodeManifest(node, manifest))));
        return result;
    }

    private ReplicaManifest authoritative(List<NodeManifest> candidates) {
        // Only identical observed histories can supply a missing copy. No manifest field
        // proves an ordering between distinct existing histories.
        ReplicaManifest first = candidates.getFirst().manifest();
        if (candidates.stream()
                .anyMatch(candidate -> !equivalent(first, candidate.manifest())
                        || !first.getLogicalPartitionId()
                                .equals(candidate.manifest().getLogicalPartitionId())
                        || !first.getPrimaryNodeId().equals(candidate.manifest().getPrimaryNodeId()))) {
            return null;
        }
        return candidates.stream()
                .sorted(Comparator.comparing((NodeManifest candidate) ->
                                !candidate.nodeId().equals(candidate.manifest().getPrimaryNodeId()))
                        .thenComparing(NodeManifest::nodeId))
                .findFirst()
                .orElseThrow()
                .manifest();
    }

    private String sourceNode(List<NodeManifest> candidates, ReplicaManifest source) {
        return candidates.stream()
                .filter(candidate -> equivalent(source, candidate.manifest()))
                .sorted(Comparator.comparing(
                                (NodeManifest candidate) -> !candidate.nodeId().equals(source.getPrimaryNodeId()))
                        .thenComparing(NodeManifest::nodeId))
                .findFirst()
                .orElseThrow()
                .nodeId();
    }

    private Set<String> expectedNodes(ReplicaManifest manifest) {
        if (manifest.getLogicalPartitionId().isBlank()
                || manifest.getPrimaryNodeId().isBlank()) {
            return Set.copyOf(configuredNodeIds);
        }
        return Set.copyOf(ReplicaPlacement.forPrimary(
                        manifest.getLogicalPartitionId(),
                        manifest.getPrimaryNodeId(),
                        configuredNodeIds,
                        membership.getReplicationFactor(),
                        Math.max(1L, manifest.getPlacementGeneration()),
                        membership.getDurabilityPolicy())
                .nodeIds());
    }

    static ReplicaRepairState classify(ReplicaManifest source, ReplicaManifest target) {
        if (target == null) {
            return ReplicaRepairState.REPLICA_REPAIR_STATE_MISSING;
        }
        if (target.getPlacementGeneration() != source.getPlacementGeneration()) {
            return ReplicaRepairState.REPLICA_REPAIR_STATE_WRONG_GENERATION;
        }
        if (target.getCommittedPosition() < source.getCommittedPosition()) {
            return ReplicaRepairState.REPLICA_REPAIR_STATE_LAGGING;
        }
        if (!target.getContentChecksum().equals(source.getContentChecksum())) {
            return ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKSUM_DIVERGENT;
        }
        return ReplicaRepairState.REPLICA_REPAIR_STATE_READY;
    }

    private void repair(RepairCandidate candidate) {
        if (candidate.sourceNodeId().equals(candidate.targetNodeId())) {
            return;
        }
        long started = clock.millis();
        String repairId = repairId(candidate);
        ReplicaRepairStatus.Builder status = ReplicaRepairStatus.newBuilder()
                .setRepairId(repairId)
                .setShardId(candidate.shardId())
                .setSourceNodeId(candidate.sourceNodeId())
                .setTargetNodeId(candidate.targetNodeId())
                .setState(candidate.reason())
                .setAttempts(nextAttempt(repairId))
                .setStartedAtEpochMillis(started)
                .setUpdatedAtEpochMillis(started);
        membership.recordRepairStatus(status.build());
        membership.updateReplicaNodeState(candidate.targetNodeId(), candidate.reason());
        ACTIVE.inc();
        Histogram.Timer timer = DURATION.startTimer();
        try {
            if (pauseIfRequested(candidate, status)) {
                return;
            }
            var snapshot = stub(candidate.sourceNodeId())
                    .openReplicaSnapshot(OpenReplicaSnapshotRequest.newBuilder()
                            .setShardId(candidate.shardId())
                            .setMaxSnapshotBytes(Math.max(1L, config.getMaxSnapshotBytes()))
                            .build());
            long offset = stub(candidate.targetNodeId())
                    .beginReplicaRepair(BeginReplicaRepairRequest.newBuilder()
                            .setRepairId(repairId)
                            .setManifest(snapshot.getManifest())
                            .setSnapshotId(snapshot.getSnapshotId())
                            .setTotalBytes(snapshot.getTotalBytes())
                            .setTransferChecksum(snapshot.getTransferChecksum())
                            .build())
                    .getAcceptedOffset();
            status.setState(ReplicaRepairState.REPLICA_REPAIR_STATE_TRANSFERRING)
                    .setBytesTransferred(offset)
                    .setTotalBytes(snapshot.getTotalBytes())
                    .setUpdatedAtEpochMillis(clock.millis());
            membership.recordRepairStatus(status.build());
            long transferStartedNanos = System.nanoTime();
            while (offset < snapshot.getTotalBytes()) {
                if (pauseIfRequested(candidate, status)) {
                    return;
                }
                int chunkBytes = Math.max(1, Math.min(1024 * 1024, config.getChunkBytes()));
                var chunk = stub(candidate.sourceNodeId())
                        .readReplicaSnapshotChunk(ReadReplicaSnapshotChunkRequest.newBuilder()
                                .setSnapshotId(snapshot.getSnapshotId())
                                .setOffset(offset)
                                .setMaxBytes(chunkBytes)
                                .build());
                if (chunk.getOffset() != offset || chunk.getData().isEmpty()) {
                    throw new IllegalStateException("source returned a non-progressing replica repair chunk");
                }
                offset = stub(candidate.targetNodeId())
                        .writeReplicaRepairChunk(WriteReplicaRepairChunkRequest.newBuilder()
                                .setRepairId(repairId)
                                .setOffset(offset)
                                .setData(chunk.getData())
                                .build())
                        .getAcceptedOffset();
                status.setBytesTransferred(offset).setUpdatedAtEpochMillis(clock.millis());
                membership.recordRepairStatus(status.build());
                throttle(offset, transferStartedNanos);
            }
            if (pauseIfRequested(candidate, status)) {
                return;
            }
            status.setState(ReplicaRepairState.REPLICA_REPAIR_STATE_VERIFYING).setUpdatedAtEpochMillis(clock.millis());
            membership.recordRepairStatus(status.build());
            var finished = stub(candidate.targetNodeId())
                    .finishReplicaRepair(FinishReplicaRepairRequest.newBuilder()
                            .setRepairId(repairId)
                            .build());
            if (!equivalent(snapshot.getManifest(), finished.getManifest())) {
                throw new IllegalStateException("target post-install manifest did not converge");
            }
            status.setState(ReplicaRepairState.REPLICA_REPAIR_STATE_READY).setUpdatedAtEpochMillis(clock.millis());
            membership.recordRepairStatus(status.build());
            // The node may host other divergent shards.  A subsequent complete comparison is the
            // only operation allowed to publish node-wide eligibility.
            membership.updateReplicaNodeState(
                    candidate.targetNodeId(), ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKING);
            OUTCOMES.labels("success").inc();
        } catch (RuntimeException e) {
            status.setState(ReplicaRepairState.REPLICA_REPAIR_STATE_FAILED)
                    .setLastError(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage())
                    .setUpdatedAtEpochMillis(clock.millis());
            membership.recordRepairStatus(status.build());
            membership.updateReplicaNodeState(candidate.targetNodeId(), ReplicaRepairState.REPLICA_REPAIR_STATE_FAILED);
            OUTCOMES.labels("failed").inc();
            LOGGER.log(Level.WARNING, "Replica repair failed: " + repairId, e);
        } finally {
            timer.observeDuration();
            ACTIVE.dec();
        }
    }

    private boolean pauseIfRequested(RepairCandidate candidate, ReplicaRepairStatus.Builder status) {
        if (!membership.repairsPaused()) {
            return false;
        }
        status.setState(ReplicaRepairState.REPLICA_REPAIR_STATE_PAUSED).setUpdatedAtEpochMillis(clock.millis());
        membership.recordRepairStatus(status.build());
        membership.updateReplicaNodeState(candidate.targetNodeId(), ReplicaRepairState.REPLICA_REPAIR_STATE_PAUSED);
        OUTCOMES.labels("paused").inc();
        return true;
    }

    private IndexServiceGrpc.IndexServiceBlockingStub stub(String nodeId) {
        NodeEndpoint endpoint = nodes.get(nodeId);
        if (endpoint == null) {
            throw new IllegalArgumentException("No configured index-node endpoint for " + nodeId);
        }
        return endpoint.stub().withDeadlineAfter(Math.max(1, config.getRpcDeadlineMillis()), TimeUnit.MILLISECONDS);
    }

    private int nextAttempt(String repairId) {
        return membership.repairStatuses().stream()
                        .filter(status -> status.getRepairId().equals(repairId))
                        .mapToInt(ReplicaRepairStatus::getAttempts)
                        .max()
                        .orElse(0)
                + 1;
    }

    private void throttle(long bytesTransferred, long startedNanos) {
        long bytesPerSecond = config.getBandwidthBytesPerSecond();
        if (bytesPerSecond <= 0) {
            return;
        }
        long expectedNanos = (long) ((bytesTransferred * 1_000_000_000.0) / bytesPerSecond);
        long remaining = expectedNanos - (System.nanoTime() - startedNanos);
        if (remaining <= 0) {
            return;
        }
        try {
            TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.SECONDS.toNanos(1)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("replica repair interrupted", e);
        }
    }

    private static boolean equivalent(ReplicaManifest left, ReplicaManifest right) {
        return left.getShardId().equals(right.getShardId())
                && left.getPlacementGeneration() == right.getPlacementGeneration()
                && left.getCommittedPosition() == right.getCommittedPosition()
                && left.getContentChecksum().equals(right.getContentChecksum());
    }

    private static String repairId(RepairCandidate candidate) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String value = candidate.shardId()
                    + '\0'
                    + candidate.sourceNodeId()
                    + '\0'
                    + candidate.targetNodeId()
                    + '\0'
                    + candidate.source().getContentChecksum();
            return "repair-" + HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    @Override
    public void close() {
        nodes.values().forEach(endpoint -> endpoint.channel().shutdown());
    }

    private record NodeEndpoint(ManagedChannel channel, IndexServiceGrpc.IndexServiceBlockingStub stub) {}

    private record NodeManifest(String nodeId, ReplicaManifest manifest) {}

    private record RepairCandidate(
            String shardId,
            String sourceNodeId,
            String targetNodeId,
            ReplicaManifest source,
            ReplicaRepairState reason) {}
}
