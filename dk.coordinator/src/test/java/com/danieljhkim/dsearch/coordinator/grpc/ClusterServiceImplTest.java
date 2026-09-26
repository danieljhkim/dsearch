package com.danieljhkim.dsearch.coordinator.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.danieljhkim.dsearch.common.cluster.NodeGroup;
import com.danieljhkim.dsearch.common.config.AppConfig;
import com.danieljhkim.dsearch.common.enums.RoutingStrategy;
import com.danieljhkim.dsearch.common.grpc.GrpcPeerIdentity;
import com.danieljhkim.dsearch.common.grpc.GrpcPeerIdentityContext;
import com.danieljhkim.dsearch.coordinator.cluster.ClusterMembershipService;
import com.danieljhkim.dsearch.proto.cluster.ControlReplicaRepairsRequest;
import com.danieljhkim.dsearch.proto.cluster.ControlReplicaRepairsResponse;
import com.danieljhkim.dsearch.proto.cluster.DeregisterNodeRequest;
import com.danieljhkim.dsearch.proto.cluster.DeregisterNodeResponse;
import com.danieljhkim.dsearch.proto.cluster.GetClusterInfoRequest;
import com.danieljhkim.dsearch.proto.cluster.GetClusterInfoResponse;
import com.danieljhkim.dsearch.proto.cluster.GetShardMapRequest;
import com.danieljhkim.dsearch.proto.cluster.GetShardMapResponse;
import com.danieljhkim.dsearch.proto.cluster.HeartbeatRequest;
import com.danieljhkim.dsearch.proto.cluster.HeartbeatResponse;
import com.danieljhkim.dsearch.proto.cluster.NodeInfo;
import com.danieljhkim.dsearch.proto.cluster.NodeRole;
import com.danieljhkim.dsearch.proto.cluster.RegisterNodeRequest;
import com.danieljhkim.dsearch.proto.cluster.RegisterNodeResponse;
import com.danieljhkim.dsearch.proto.cluster.ReplicaRepairState;
import com.danieljhkim.dsearch.proto.cluster.ReplicaRepairStatus;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.junit.jupiter.api.Test;

class ClusterServiceImplTest {

    @Test
    void registerIndexNodeAndClusterInfoIncludesNodeAndGroupData() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig());
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);

        RegisterNodeResponse registerResponse = registerNode(
                service, registerRequest("index-live-0", "index-live.local", 5011, 5111, NodeRole.NODE_ROLE_INDEX));

        assertTrue(registerResponse.getSuccess());
        assertTrue(registerResponse.getLeaseDurationMillis() > 0);
        GetClusterInfoResponse clusterInfo = getClusterInfo(service, NodeRole.NODE_ROLE_INDEX);
        assertEquals("index-nodes", clusterInfo.getComponentLabel());
        assertEquals(RoutingStrategy.ROUND_ROBIN.name(), clusterInfo.getRoutingStrategy());
        assertEquals(1, clusterInfo.getReplicationFactor());
        assertNode(
                findNode(clusterInfo, "index-live-0"),
                "index-live-0",
                "index-live.local",
                5011,
                5111,
                NodeRole.NODE_ROLE_INDEX);
    }

    @Test
    void registerQueryNodeAndClusterInfoIncludesNodeAndGroupData() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig());
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);

        RegisterNodeResponse registerResponse = registerNode(
                service, registerRequest("query-live-0", "query-live.local", 6011, 6111, NodeRole.NODE_ROLE_QUERY));

        assertTrue(registerResponse.getSuccess());
        GetClusterInfoResponse clusterInfo = getClusterInfo(service, NodeRole.NODE_ROLE_QUERY);
        assertEquals("query-nodes", clusterInfo.getComponentLabel());
        assertEquals(RoutingStrategy.LEAST_LOADED.name(), clusterInfo.getRoutingStrategy());
        assertEquals(1, clusterInfo.getReplicationFactor());
        assertNode(
                findNode(clusterInfo, "query-live-0"),
                "query-live-0",
                "query-live.local",
                6011,
                6111,
                NodeRole.NODE_ROLE_QUERY);
    }

    @Test
    void duplicateRegistrationUpdatesExistingNode() {
        ClusterServiceImpl service = new ClusterServiceImpl(new ClusterMembershipService(appConfig()));

        RegisterNodeResponse firstResponse = registerNode(
                service, registerRequest("index-live-0", "old.local", 5011, 5111, NodeRole.NODE_ROLE_INDEX));
        RegisterNodeResponse secondResponse = registerNode(
                service, registerRequest("index-live-0", "new.local", 5022, 5122, NodeRole.NODE_ROLE_INDEX));

        assertTrue(firstResponse.getSuccess());
        assertTrue(secondResponse.getSuccess());
        GetClusterInfoResponse clusterInfo = getClusterInfo(service, NodeRole.NODE_ROLE_INDEX);
        assertEquals(1, clusterInfo.getNodesCount());
        assertNode(
                findNode(clusterInfo, "index-live-0"),
                "index-live-0",
                "new.local",
                5022,
                5122,
                NodeRole.NODE_ROLE_INDEX);
    }

    @Test
    void registrationRejectsOlderCoordinatorStateWithinSameEpochBeforeMutation() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig());
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);
        RegisterNodeResponse first =
                registerNode(service, validRegisterRequest().build());
        CapturingObserver<RegisterNodeResponse> observer = new CapturingObserver<>();

        asAdmin(() -> service.registerNode(
                registerRequest("node-b", "localhost", 5001, 5101, NodeRole.NODE_ROLE_INDEX).toBuilder()
                        .setObservedTopologyEpoch(first.getTopologyEpoch())
                        .setObservedTopologyVersion(first.getTopologyVersion() + 1)
                        .build(),
                observer));

        assertStatus(
                observer.error,
                Status.Code.FAILED_PRECONDITION,
                "Requested topology version " + (first.getTopologyVersion() + 1) + " but coordinator has "
                        + first.getTopologyVersion());
        assertNull(membershipService.getIndexGroup().getNode("node-b"));
    }

    @Test
    void registerNodeStoresValidNodeInMembership() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig());
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);
        CapturingObserver<RegisterNodeResponse> observer = new CapturingObserver<>();

        asAdmin(() -> service.registerNode(validRegisterRequest().build(), observer));

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertTrue(observer.value.getSuccess());
        assertNotNull(membershipService.getIndexGroup().getNode("node-a"));
    }

    @Test
    void topologyMutationRejectsUnauthenticatedCaller() {
        ClusterServiceImpl service = new ClusterServiceImpl(new ClusterMembershipService(appConfig()));
        CapturingObserver<RegisterNodeResponse> observer = new CapturingObserver<>();

        service.registerNode(validRegisterRequest().build(), observer);

        assertStatus(
                observer.error,
                Status.Code.UNAUTHENTICATED,
                "Topology mutation requires an authenticated service identity");
    }

    @Test
    void registrationRejectsAuthenticatedIdentityWithWrongRole() {
        ClusterServiceImpl service = new ClusterServiceImpl(new ClusterMembershipService(appConfig()));
        CapturingObserver<RegisterNodeResponse> observer = new CapturingObserver<>();

        GrpcPeerIdentityContext.runAs(
                GrpcPeerIdentity.node(NodeRole.NODE_ROLE_QUERY, "node-a"),
                () -> service.registerNode(validRegisterRequest().build(), observer));

        assertStatus(
                observer.error,
                Status.Code.PERMISSION_DENIED,
                "Authenticated identity is not authorized for this node and role");
    }

    @Test
    void matchingNodeIdentityCanMutateOnlyItsOwnLease() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig());
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);
        RegisterNodeRequest registration = validRegisterRequest().build();
        CapturingObserver<RegisterNodeResponse> registrationObserver = new CapturingObserver<>();
        GrpcPeerIdentity nodeIdentity = GrpcPeerIdentity.node(NodeRole.NODE_ROLE_INDEX, "node-a");
        GrpcPeerIdentityContext.runAs(nodeIdentity, () -> service.registerNode(registration, registrationObserver));
        assertNull(registrationObserver.error);

        CapturingObserver<HeartbeatResponse> unauthorizedHeartbeat = new CapturingObserver<>();
        GrpcPeerIdentityContext.runAs(
                GrpcPeerIdentity.node(NodeRole.NODE_ROLE_INDEX, "node-b"),
                () -> service.heartbeat(
                        HeartbeatRequest.newBuilder()
                                .setNodeId("node-a")
                                .setRole(NodeRole.NODE_ROLE_INDEX)
                                .build(),
                        unauthorizedHeartbeat));
        assertStatus(
                unauthorizedHeartbeat.error,
                Status.Code.PERMISSION_DENIED,
                "Authenticated identity is not authorized for this node and role");

        CapturingObserver<DeregisterNodeResponse> deregistrationObserver = new CapturingObserver<>();
        GrpcPeerIdentityContext.runAs(
                nodeIdentity,
                () -> service.deregisterNode(
                        DeregisterNodeRequest.newBuilder()
                                .setNodeId("node-a")
                                .setRole(NodeRole.NODE_ROLE_INDEX)
                                .build(),
                        deregistrationObserver));
        assertNull(deregistrationObserver.error);
        assertNull(membershipService.getIndexGroup().getNode("node-a"));
    }

    @Test
    void replicaRepairControlsRejectNodeIdentitiesWithoutChangingRepairState() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig());
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);
        ReplicaRepairStatus repair = repairStatus();
        membershipService.recordRepairStatus(repair);
        membershipService.updateReplicaNodeState("target-node", ReplicaRepairState.REPLICA_REPAIR_STATE_FAILED);

        for (NodeRole role : List.of(NodeRole.NODE_ROLE_INDEX, NodeRole.NODE_ROLE_QUERY)) {
            GrpcPeerIdentity nodeIdentity = GrpcPeerIdentity.node(role, "node-a");
            for (String action : List.of("pause", "resume", "retry")) {
                CapturingObserver<ControlReplicaRepairsResponse> observer = new CapturingObserver<>();
                GrpcPeerIdentityContext.runAs(
                        nodeIdentity, () -> service.controlReplicaRepairs(controlRequest(action), observer));

                assertStatus(
                        observer.error,
                        Status.Code.PERMISSION_DENIED,
                        "Replica repair control requires an admin identity");
                assertFalse(membershipService.repairsPaused());
                assertEquals(List.of(repair), membershipService.repairStatuses());
                assertEquals(
                        ReplicaRepairState.REPLICA_REPAIR_STATE_FAILED,
                        membershipService.replicaRepairState("target-node"));
            }
        }
    }

    @Test
    void adminIdentityCanPauseResumeAndRetryReplicaRepairs() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig());
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);
        membershipService.recordRepairStatus(repairStatus());
        membershipService.updateReplicaNodeState("target-node", ReplicaRepairState.REPLICA_REPAIR_STATE_FAILED);

        ControlReplicaRepairsResponse paused = controlReplicaRepairsAsAdmin(service, "pause");
        assertTrue(paused.getSuccess());
        assertTrue(paused.getPaused());
        assertTrue(membershipService.repairsPaused());

        ControlReplicaRepairsResponse resumed = controlReplicaRepairsAsAdmin(service, "resume");
        assertTrue(resumed.getSuccess());
        assertFalse(resumed.getPaused());
        assertFalse(membershipService.repairsPaused());
        assertEquals(
                ReplicaRepairState.REPLICA_REPAIR_STATE_FAILED, membershipService.replicaRepairState("target-node"));

        ControlReplicaRepairsResponse retried = controlReplicaRepairsAsAdmin(service, "retry");
        assertTrue(retried.getSuccess());
        ReplicaRepairStatus repair = membershipService.repairStatuses().getFirst();
        assertEquals(ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKING, repair.getState());
        assertEquals("", repair.getLastError());
        assertEquals(
                ReplicaRepairState.REPLICA_REPAIR_STATE_CHECKING, membershipService.replicaRepairState("target-node"));
    }

    @Test
    void replicaRepairControlsRejectUnauthenticatedCallerWithoutChangingState() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig());
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);
        ReplicaRepairStatus repair = repairStatus();
        membershipService.recordRepairStatus(repair);
        membershipService.updateReplicaNodeState("target-node", ReplicaRepairState.REPLICA_REPAIR_STATE_FAILED);
        CapturingObserver<ControlReplicaRepairsResponse> observer = new CapturingObserver<>();

        service.controlReplicaRepairs(controlRequest("pause"), observer);

        assertStatus(
                observer.error,
                Status.Code.UNAUTHENTICATED,
                "Replica repair control requires an authenticated identity");
        assertFalse(membershipService.repairsPaused());
        assertEquals(List.of(repair), membershipService.repairStatuses());
        assertEquals(
                ReplicaRepairState.REPLICA_REPAIR_STATE_FAILED, membershipService.replicaRepairState("target-node"));
    }

    @Test
    void registerNodeRejectsInvalidRole() {
        RegisterNodeRequest request =
                validRegisterRequest().setRole(NodeRole.NODE_ROLE_UNKNOWN).build();

        assertRegisterStatus(request, Status.Code.INVALID_ARGUMENT, "role must be INDEX, QUERY, or COORDINATOR");
    }

    @Test
    void registerNodeRejectsEmptyNodeId() {
        RegisterNodeRequest request = validRegisterRequest().setNodeId("").build();

        assertRegisterStatus(request, Status.Code.INVALID_ARGUMENT, "node_id must not be empty");
    }

    @Test
    void registerNodeRejectsInvalidHost() {
        RegisterNodeRequest request = validRegisterRequest().setHost("bad host").build();

        assertRegisterStatus(request, Status.Code.INVALID_ARGUMENT, "host must be a valid DNS name or IP literal");
    }

    @Test
    void registerNodeRejectsInvalidPort() {
        RegisterNodeRequest request = validRegisterRequest().setPort(0).build();

        assertRegisterStatus(request, Status.Code.INVALID_ARGUMENT, "port must be between 1 and 65535");
    }

    @Test
    void registerNodeRejectsInvalidHealthPort() {
        RegisterNodeRequest request =
                validRegisterRequest().setHealthPort(70000).build();

        assertRegisterStatus(request, Status.Code.INVALID_ARGUMENT, "health_port must be between 1 and 65535");
    }

    @Test
    void registerNodeReturnsNotFoundForMissingNodeGroup() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig()) {
            @Override
            public NodeGroup resolveGroup(NodeRole role) {
                return null;
            }

            @Override
            public void registerNode(NodeGroup.NodeInfo nodeInfo, NodeRole role) {
                fail("registerNode should not be called when the node group is missing");
            }
        };
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);
        CapturingObserver<RegisterNodeResponse> observer = new CapturingObserver<>();

        asAdmin(() -> service.registerNode(validRegisterRequest().build(), observer));

        assertStatus(observer.error, Status.Code.NOT_FOUND, "No node group registered for role: NODE_ROLE_INDEX");
    }

    @Test
    void getClusterInfoRejectsInvalidRole() {
        ClusterServiceImpl service = new ClusterServiceImpl(new ClusterMembershipService(appConfig()));
        CapturingObserver<GetClusterInfoResponse> observer = new CapturingObserver<>();
        GetClusterInfoRequest request = GetClusterInfoRequest.newBuilder()
                .setRole(NodeRole.NODE_ROLE_UNKNOWN)
                .build();

        service.getClusterInfo(request, observer);

        assertStatus(observer.error, Status.Code.INVALID_ARGUMENT, "role must be INDEX, QUERY, or COORDINATOR");
    }

    @Test
    void clusterInfoOmitsNodesMarkedUnhealthyWithoutSleeping() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig());
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);
        registerNode(
                service, registerRequest("query-live-0", "query-live.local", 6011, 6111, NodeRole.NODE_ROLE_QUERY));

        membershipService.updateNodeHealth("query-live-0", NodeRole.NODE_ROLE_QUERY, false);

        GetClusterInfoResponse clusterInfo = getClusterInfo(service, NodeRole.NODE_ROLE_QUERY);
        assertFalse(clusterInfo.getNodesList().stream()
                .anyMatch(node -> node.getNodeId().equals("query-live-0")));
    }

    @Test
    void heartbeatRenewsRegisteredNodeLeaseAndReturnsVersionedContract() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig());
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);
        CapturingObserver<HeartbeatResponse> observer = new CapturingObserver<>();
        registerNode(service, validRegisterRequest().build());
        long registeredVersion = membershipService.getTopologyVersion();

        asAdmin(() -> service.heartbeat(
                HeartbeatRequest.newBuilder()
                        .setNodeId("node-a")
                        .setRole(NodeRole.NODE_ROLE_INDEX)
                        .setObservedTopologyVersion(membershipService.getTopologyVersion())
                        .build(),
                observer));

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertTrue(observer.value.getSuccess());
        assertEquals(ClusterMembershipService.CONTRACT_VERSION, observer.value.getContractVersion());
        assertEquals(membershipService.getTopologyEpoch(), observer.value.getTopologyEpoch());
        assertEquals(registeredVersion, observer.value.getTopologyVersion());
        assertTrue(observer.value.getLeaseDurationMillis() > 0);
    }

    @Test
    void heartbeatRejectsStaleCoordinatorEpoch() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig());
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);
        CapturingObserver<HeartbeatResponse> observer = new CapturingObserver<>();
        registerNode(service, validRegisterRequest().build());

        asAdmin(() -> service.heartbeat(
                HeartbeatRequest.newBuilder()
                        .setNodeId("node-a")
                        .setRole(NodeRole.NODE_ROLE_INDEX)
                        .setObservedTopologyEpoch("stale-epoch")
                        .setObservedTopologyVersion(membershipService.getTopologyVersion())
                        .build(),
                observer));

        assertStatus(
                observer.error,
                Status.Code.FAILED_PRECONDITION,
                "Observed topology epoch stale-epoch but coordinator has " + membershipService.getTopologyEpoch());
    }

    @Test
    void gracefulDeregistrationIsIdempotentAndRemovesNodeImmediately() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig());
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);
        RegisterNodeResponse registration =
                registerNode(service, validRegisterRequest().build());

        DeregisterNodeResponse first = deregisterNode(
                service,
                DeregisterNodeRequest.newBuilder()
                        .setNodeId("node-a")
                        .setRole(NodeRole.NODE_ROLE_INDEX)
                        .setObservedTopologyEpoch(registration.getTopologyEpoch())
                        .setObservedTopologyVersion(registration.getTopologyVersion())
                        .build());
        DeregisterNodeResponse second = deregisterNode(
                service,
                DeregisterNodeRequest.newBuilder()
                        .setNodeId("node-a")
                        .setRole(NodeRole.NODE_ROLE_INDEX)
                        .setObservedTopologyEpoch(first.getTopologyEpoch())
                        .setObservedTopologyVersion(first.getTopologyVersion())
                        .build());

        assertTrue(first.getSuccess());
        assertTrue(second.getSuccess());
        assertNull(membershipService.getIndexGroup().getNode("node-a"));
        assertEquals(first.getTopologyVersion(), second.getTopologyVersion());
    }

    @Test
    void shardMapReturnsDeterministicVersionedIndexPlacement() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig());
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);
        CapturingObserver<GetShardMapResponse> observer = new CapturingObserver<>();
        registerNode(service, registerRequest("z-node", "z.local", 5002, 5102, NodeRole.NODE_ROLE_INDEX));
        registerNode(service, registerRequest("a-node", "a.local", 5001, 5101, NodeRole.NODE_ROLE_INDEX));

        service.getShardMap(GetShardMapRequest.getDefaultInstance(), observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertEquals(ClusterMembershipService.CONTRACT_VERSION, observer.value.getContractVersion());
        assertEquals(membershipService.getTopologyEpoch(), observer.value.getTopologyEpoch());
        assertEquals(membershipService.getTopologyVersion(), observer.value.getTopologyVersion());
        assertEquals(
                List.of("index/a-node", "index/z-node"),
                observer.value.getShardLocationsList().stream()
                        .map(location -> location.getShardId())
                        .toList());
    }

    @Test
    void shardMapRejectsVersionNewerThanDurableCoordinatorState() {
        ClusterMembershipService membershipService = new ClusterMembershipService(appConfig());
        ClusterServiceImpl service = new ClusterServiceImpl(membershipService);
        CapturingObserver<GetShardMapResponse> observer = new CapturingObserver<>();

        service.getShardMap(
                GetShardMapRequest.newBuilder()
                        .setMinTopologyVersion(membershipService.getTopologyVersion() + 1)
                        .build(),
                observer);

        assertStatus(
                observer.error,
                Status.Code.FAILED_PRECONDITION,
                "Requested topology version " + (membershipService.getTopologyVersion() + 1) + " but coordinator has "
                        + membershipService.getTopologyVersion());
    }

    private static RegisterNodeResponse registerNode(ClusterServiceImpl service, RegisterNodeRequest request) {
        CapturingObserver<RegisterNodeResponse> observer = new CapturingObserver<>();

        asAdmin(() -> service.registerNode(request, observer));

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertNotNull(observer.value);
        return observer.value;
    }

    private static GetClusterInfoResponse getClusterInfo(ClusterServiceImpl service, NodeRole role) {
        CapturingObserver<GetClusterInfoResponse> observer = new CapturingObserver<>();

        service.getClusterInfo(GetClusterInfoRequest.newBuilder().setRole(role).build(), observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertNotNull(observer.value);
        return observer.value;
    }

    private static DeregisterNodeResponse deregisterNode(ClusterServiceImpl service, DeregisterNodeRequest request) {
        CapturingObserver<DeregisterNodeResponse> observer = new CapturingObserver<>();

        asAdmin(() -> service.deregisterNode(request, observer));

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertNotNull(observer.value);
        return observer.value;
    }

    private static NodeInfo findNode(GetClusterInfoResponse response, String nodeId) {
        return response.getNodesList().stream()
                .filter(node -> node.getNodeId().equals(nodeId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected node missing from cluster info: " + nodeId));
    }

    private static void assertNode(NodeInfo node, String nodeId, String host, int port, int healthPort, NodeRole role) {
        assertEquals(nodeId, node.getNodeId());
        assertEquals(host, node.getHost());
        assertEquals(port, node.getPort());
        assertEquals(healthPort, node.getHealthPort());
        assertEquals(role, node.getRole());
    }

    private static void assertRegisterStatus(
            RegisterNodeRequest request, Status.Code expectedCode, String expectedDescription) {
        ClusterServiceImpl service = new ClusterServiceImpl(new ClusterMembershipService(appConfig()));
        CapturingObserver<RegisterNodeResponse> observer = new CapturingObserver<>();

        asAdmin(() -> service.registerNode(request, observer));

        assertStatus(observer.error, expectedCode, expectedDescription);
    }

    private static void assertStatus(Throwable error, Status.Code expectedCode, String expectedDescription) {
        assertNotNull(error);
        Status status = Status.fromThrowable(error);
        assertEquals(expectedCode, status.getCode());
        assertEquals(expectedDescription, status.getDescription());
    }

    private static void asAdmin(Runnable action) {
        GrpcPeerIdentityContext.runAs(GrpcPeerIdentity.admin("coordinator-test"), action);
    }

    private static ControlReplicaRepairsResponse controlReplicaRepairsAsAdmin(
            ClusterServiceImpl service, String action) {
        CapturingObserver<ControlReplicaRepairsResponse> observer = new CapturingObserver<>();
        asAdmin(() -> service.controlReplicaRepairs(controlRequest(action), observer));

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertNotNull(observer.value);
        return observer.value;
    }

    private static ControlReplicaRepairsRequest controlRequest(String action) {
        return ControlReplicaRepairsRequest.newBuilder()
                .setAction(action)
                .setRepairId("repair-1")
                .build();
    }

    private static ReplicaRepairStatus repairStatus() {
        return ReplicaRepairStatus.newBuilder()
                .setRepairId("repair-1")
                .setTargetNodeId("target-node")
                .setState(ReplicaRepairState.REPLICA_REPAIR_STATE_FAILED)
                .setLastError("initial failure")
                .build();
    }

    private static RegisterNodeRequest registerRequest(
            String nodeId, String host, int port, int healthPort, NodeRole role) {
        return RegisterNodeRequest.newBuilder()
                .setNodeId(nodeId)
                .setHost(host)
                .setPort(port)
                .setHealthPort(healthPort)
                .setRole(role)
                .build();
    }

    private static RegisterNodeRequest.Builder validRegisterRequest() {
        return RegisterNodeRequest.newBuilder()
                .setNodeId("node-a")
                .setHost("localhost")
                .setPort(5000)
                .setHealthPort(5100)
                .setRole(NodeRole.NODE_ROLE_INDEX);
    }

    private static AppConfig appConfig() {
        AppConfig config = new AppConfig();
        AppConfig.ServiceDiscoveryConfig discovery = new AppConfig.ServiceDiscoveryConfig();
        discovery.setNodeExpirySeconds(30);
        config.setServiceDiscovery(discovery);
        config.setIndexNodes(nodeGroupConfig("index-nodes", RoutingStrategy.ROUND_ROBIN));
        config.setQueryNodes(nodeGroupConfig("query-nodes", RoutingStrategy.LEAST_LOADED));
        config.setCoordinatorNodes(nodeGroupConfig("coordinator-nodes", RoutingStrategy.ROUND_ROBIN));
        return config;
    }

    private static AppConfig.NodeGroupConfig nodeGroupConfig(String componentLabel, RoutingStrategy routingStrategy) {
        AppConfig.NodeGroupConfig group = new AppConfig.NodeGroupConfig();
        group.setComponentLabel(componentLabel);
        group.setRoutingStrategy(routingStrategy);
        group.setReplicationFactor(1);
        group.setNodes(List.of());
        return group;
    }

    private static final class CapturingObserver<T> implements StreamObserver<T> {
        private T value;
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable error) {
            this.error = error;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }
}
