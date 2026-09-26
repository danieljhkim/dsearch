package com.danieljhkim.dsearch.coordinator.grpc;

import com.danieljhkim.dsearch.common.cluster.NodeGroup;
import com.danieljhkim.dsearch.common.grpc.GrpcPeerIdentity;
import com.danieljhkim.dsearch.common.grpc.GrpcPeerIdentityContext;
import com.danieljhkim.dsearch.coordinator.cluster.ClusterMembershipService;
import com.danieljhkim.dsearch.proto.cluster.ClusterServiceGrpc;
import com.danieljhkim.dsearch.proto.cluster.ControlReplicaRepairsRequest;
import com.danieljhkim.dsearch.proto.cluster.ControlReplicaRepairsResponse;
import com.danieljhkim.dsearch.proto.cluster.DeregisterNodeRequest;
import com.danieljhkim.dsearch.proto.cluster.DeregisterNodeResponse;
import com.danieljhkim.dsearch.proto.cluster.GetClusterInfoRequest;
import com.danieljhkim.dsearch.proto.cluster.GetClusterInfoResponse;
import com.danieljhkim.dsearch.proto.cluster.GetReplicaRepairsRequest;
import com.danieljhkim.dsearch.proto.cluster.GetReplicaRepairsResponse;
import com.danieljhkim.dsearch.proto.cluster.GetShardMapRequest;
import com.danieljhkim.dsearch.proto.cluster.GetShardMapResponse;
import com.danieljhkim.dsearch.proto.cluster.HeartbeatRequest;
import com.danieljhkim.dsearch.proto.cluster.HeartbeatResponse;
import com.danieljhkim.dsearch.proto.cluster.NodeInfo;
import com.danieljhkim.dsearch.proto.cluster.NodeRole;
import com.danieljhkim.dsearch.proto.cluster.RegisterNodeRequest;
import com.danieljhkim.dsearch.proto.cluster.RegisterNodeResponse;
import com.danieljhkim.dsearch.proto.cluster.ShardLocation;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class ClusterServiceImpl extends ClusterServiceGrpc.ClusterServiceImplBase {

    private static final Logger LOGGER = Logger.getLogger(ClusterServiceImpl.class.getName());
    private static final Pattern HOST_PATTERN = Pattern.compile("^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$");
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;

    private final ClusterMembershipService membershipService;

    public ClusterServiceImpl(ClusterMembershipService membershipService) {
        this.membershipService = membershipService;
    }

    @Override
    public void registerNode(RegisterNodeRequest request, StreamObserver<RegisterNodeResponse> responseObserver) {
        try {
            NodeRole role = validateRegisterNodeRequest(request);
            authorizeMutation(role, request.getNodeId());
            NodeGroup group = membershipService.resolveGroup(role);
            if (group == null) {
                responseObserver.onError(noGroupStatus(role).asRuntimeException());
                return;
            }

            membershipService.assertRegistrationTopology(
                    request.getObservedTopologyEpoch(), request.getObservedTopologyVersion());
            membershipService.registerNode(
                    new NodeGroup.NodeInfo(
                            request.getNodeId(),
                            request.getHost(),
                            request.getPort(),
                            request.getHealthPort(),
                            role.name(),
                            true),
                    role);
            RegisterNodeResponse resp = RegisterNodeResponse.newBuilder()
                    .setSuccess(true)
                    .setContractVersion(ClusterMembershipService.CONTRACT_VERSION)
                    .setTopologyEpoch(membershipService.getTopologyEpoch())
                    .setTopologyVersion(membershipService.getTopologyVersion())
                    .setLeaseDurationMillis(membershipService.getLeaseDurationMillis())
                    .build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
        } catch (ClusterMembershipService.StaleTopologyException e) {
            responseObserver.onError(staleTopologyStatus(e).asRuntimeException());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to register node: " + request.getNodeId(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to register node: " + request.getNodeId())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        try {
            NodeRole role = validateRole(request.getRole());
            if (request.getNodeId().isBlank()) {
                throw new IllegalArgumentException("node_id must not be empty");
            }
            authorizeMutation(role, request.getNodeId());
            long version = membershipService.heartbeat(
                    request.getNodeId(),
                    role,
                    request.getObservedTopologyEpoch(),
                    request.getObservedTopologyVersion());
            responseObserver.onNext(HeartbeatResponse.newBuilder()
                    .setSuccess(true)
                    .setContractVersion(ClusterMembershipService.CONTRACT_VERSION)
                    .setTopologyEpoch(membershipService.getTopologyEpoch())
                    .setTopologyVersion(version)
                    .setLeaseDurationMillis(membershipService.getLeaseDurationMillis())
                    .build());
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
        } catch (ClusterMembershipService.StaleTopologyException
                | ClusterMembershipService.StaleTopologyEpochException e) {
            responseObserver.onError(staleTopologyStatus(e).asRuntimeException());
        } catch (NoSuchElementException e) {
            responseObserver.onError(
                    Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to renew node heartbeat: " + request.getNodeId(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to renew node heartbeat: " + request.getNodeId())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void deregisterNode(DeregisterNodeRequest request, StreamObserver<DeregisterNodeResponse> responseObserver) {
        try {
            NodeRole role = validateRole(request.getRole());
            if (request.getNodeId().isBlank()) {
                throw new IllegalArgumentException("node_id must not be empty");
            }
            authorizeMutation(role, request.getNodeId());
            long version = membershipService.deregisterNode(
                    request.getNodeId(),
                    role,
                    request.getObservedTopologyEpoch(),
                    request.getObservedTopologyVersion());
            responseObserver.onNext(DeregisterNodeResponse.newBuilder()
                    .setSuccess(true)
                    .setContractVersion(ClusterMembershipService.CONTRACT_VERSION)
                    .setTopologyEpoch(membershipService.getTopologyEpoch())
                    .setTopologyVersion(version)
                    .build());
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
        } catch (ClusterMembershipService.StaleTopologyException
                | ClusterMembershipService.StaleTopologyEpochException e) {
            responseObserver.onError(staleTopologyStatus(e).asRuntimeException());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to deregister node: " + request.getNodeId(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to deregister node: " + request.getNodeId())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void getShardMap(GetShardMapRequest request, StreamObserver<GetShardMapResponse> responseObserver) {
        try {
            GetShardMapResponse response;
            synchronized (membershipService) {
                membershipService.assertVersionAvailable(request.getMinTopologyVersion());
                GetShardMapResponse.Builder builder = GetShardMapResponse.newBuilder()
                        .setContractVersion(ClusterMembershipService.CONTRACT_VERSION)
                        .setTopologyEpoch(membershipService.getTopologyEpoch())
                        .setTopologyVersion(membershipService.getTopologyVersion())
                        .setReplicationFactor(membershipService.getReplicationFactor())
                        .setDurabilityPolicy(
                                membershipService.getDurabilityPolicy().name().toLowerCase())
                        .setReadConsistency(
                                membershipService.getReadConsistency().name().toLowerCase())
                        .setUnderReplicatedShards(membershipService.underReplicatedShardCount());
                for (var replicaSet : membershipService.replicaSets()) {
                    for (String nodeId : replicaSet.nodeIds()) {
                        NodeGroup.NodeInfo node = membershipService.configuredPlacementNode(nodeId);
                        boolean eligible = membershipService.isReplicaEligible(nodeId);
                        builder.addShardLocations(ShardLocation.newBuilder()
                                .setShardId(replicaSet.shardId())
                                .setNodeId(node.getNodeId())
                                .setHost(node.getHost())
                                .setPort(node.getPort())
                                .setRole(NodeRole.NODE_ROLE_INDEX)
                                .setPrimaryNodeId(replicaSet.primaryNodeId())
                                .setGeneration(replicaSet.generation())
                                .setPrimary(nodeId.equals(replicaSet.primaryNodeId()))
                                .setEligible(eligible)
                                .setState(
                                        eligible
                                                ? nodeId.equals(replicaSet.primaryNodeId()) ? "primary" : "replica"
                                                : !membershipService.isIndexNodeHealthy(nodeId)
                                                        ? "unavailable"
                                                        : membershipService
                                                                .replicaRepairState(nodeId)
                                                                .name()
                                                                .substring("REPLICA_REPAIR_STATE_".length())
                                                                .toLowerCase())
                                .build());
                    }
                }
                builder.addAllRepairs(membershipService.repairStatuses());
                response = builder.build();
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (ClusterMembershipService.StaleTopologyException e) {
            responseObserver.onError(staleTopologyStatus(e).asRuntimeException());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to read authoritative shard map", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to read authoritative shard map")
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void getClusterInfo(GetClusterInfoRequest request, StreamObserver<GetClusterInfoResponse> responseObserver) {
        try {
            NodeRole role = validateRole(request.getRole());
            GetClusterInfoResponse response;
            synchronized (membershipService) {
                membershipService.assertVersionAvailable(request.getMinTopologyVersion());
                NodeGroup group = membershipService.resolveGroup(role);
                if (group == null) {
                    responseObserver.onError(noGroupStatus(role).asRuntimeException());
                    return;
                }
                GetClusterInfoResponse.Builder builder = GetClusterInfoResponse.newBuilder()
                        .setComponentLabel(group.getComponentLabel())
                        .setRoutingStrategy(group.getRoutingStrategy().name())
                        .setReplicationFactor(group.getReplicationFactor())
                        .setContractVersion(ClusterMembershipService.CONTRACT_VERSION)
                        .setTopologyEpoch(membershipService.getTopologyEpoch())
                        .setTopologyVersion(membershipService.getTopologyVersion())
                        .setDurabilityPolicy(group.getDurabilityPolicy().name().toLowerCase())
                        .setReadConsistency(group.getReadConsistency().name().toLowerCase());

                for (NodeGroup.NodeInfo ni : membershipService.healthyNodes(role)) {
                    builder.addNodes(NodeInfo.newBuilder()
                            .setNodeId(ni.getNodeId())
                            .setHost(ni.getHost())
                            .setPort(ni.getPort())
                            .setHealthPort(ni.getHealthPort())
                            .setRole(role)
                            .build());
                }
                response = builder.build();
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (ClusterMembershipService.StaleTopologyException e) {
            responseObserver.onError(staleTopologyStatus(e).asRuntimeException());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get cluster info for role: " + request.getRole(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get cluster info for role: " + request.getRole())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void getReplicaRepairs(
            GetReplicaRepairsRequest request, StreamObserver<GetReplicaRepairsResponse> responseObserver) {
        responseObserver.onNext(GetReplicaRepairsResponse.newBuilder()
                .addAllRepairs(membershipService.repairStatuses())
                .setPaused(membershipService.repairsPaused())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void controlReplicaRepairs(
            ControlReplicaRepairsRequest request, StreamObserver<ControlReplicaRepairsResponse> responseObserver) {
        try {
            GrpcPeerIdentity identity = GrpcPeerIdentityContext.current();
            if (identity == null) {
                throw Status.UNAUTHENTICATED
                        .withDescription("Replica repair control requires an authenticated identity")
                        .asRuntimeException();
            }
            if (identity.role() != GrpcPeerIdentity.IdentityRole.ADMIN) {
                throw Status.PERMISSION_DENIED
                        .withDescription("Replica repair control requires an admin identity")
                        .asRuntimeException();
            }
            boolean success =
                    switch (request.getAction().trim().toLowerCase()) {
                        case "pause" -> {
                            membershipService.setRepairsPaused(true);
                            yield true;
                        }
                        case "resume" -> {
                            membershipService.setRepairsPaused(false);
                            yield true;
                        }
                        case "retry" -> membershipService.retryRepair(request.getRepairId());
                        default -> throw new IllegalArgumentException("action must be pause, resume, or retry");
                    };
            responseObserver.onNext(ControlReplicaRepairsResponse.newBuilder()
                    .setSuccess(success)
                    .setPaused(membershipService.repairsPaused())
                    .build());
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    private NodeRole validateRegisterNodeRequest(RegisterNodeRequest request) {
        NodeRole role = validateRole(request.getRole());
        if (request.getNodeId().isBlank()) {
            throw new IllegalArgumentException("node_id must not be empty");
        }
        if (!isValidHost(request.getHost())) {
            throw new IllegalArgumentException("host must be a valid DNS name or IP literal");
        }
        validatePort("port", request.getPort());
        validatePort("health_port", request.getHealthPort());
        return role;
    }

    private void authorizeMutation(NodeRole role, String nodeId) {
        GrpcPeerIdentity identity = GrpcPeerIdentityContext.current();
        if (identity == null) {
            throw Status.UNAUTHENTICATED
                    .withDescription("Topology mutation requires an authenticated service identity")
                    .asRuntimeException();
        }
        if (!identity.authorizes(role, nodeId)) {
            throw Status.PERMISSION_DENIED
                    .withDescription("Authenticated identity is not authorized for this node and role")
                    .asRuntimeException();
        }
    }

    private NodeRole validateRole(NodeRole role) {
        if (role == NodeRole.NODE_ROLE_UNKNOWN || role == NodeRole.UNRECOGNIZED) {
            throw new IllegalArgumentException("role must be INDEX, QUERY, or COORDINATOR");
        }
        return role;
    }

    private boolean isValidHost(String host) {
        if (host == null || host.isBlank() || !host.equals(host.trim()) || host.length() > 253) {
            return false;
        }
        return HOST_PATTERN.matcher(host).matches();
    }

    private void validatePort(String fieldName, int port) {
        if (port < MIN_PORT || port > MAX_PORT) {
            throw new IllegalArgumentException(fieldName + " must be between " + MIN_PORT + " and " + MAX_PORT);
        }
    }

    private Status noGroupStatus(NodeRole role) {
        return Status.NOT_FOUND.withDescription("No node group registered for role: " + role);
    }

    private Status staleTopologyStatus(RuntimeException error) {
        return Status.FAILED_PRECONDITION.withDescription(error.getMessage());
    }
}
