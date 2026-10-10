package com.danieljhkim.dsearch.gateway.api;

import com.danieljhkim.dsearch.common.grpc.NodeClientManager;
import com.danieljhkim.dsearch.common.validation.PartitionIdValidator;
import com.danieljhkim.dsearch.gateway.api.dto.AdminAuditResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.AliasSwapRequestDto;
import com.danieljhkim.dsearch.gateway.api.dto.AnalyzeRequestDto;
import com.danieljhkim.dsearch.gateway.api.dto.AnalyzeResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.CreateIndexRequestDto;
import com.danieljhkim.dsearch.gateway.api.dto.InspectSchemaResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.ReindexRequestDto;
import com.danieljhkim.dsearch.gateway.config.AdminAuthFilter;
import com.danieljhkim.dsearch.gateway.service.GatewayAdminIndexService;
import com.danieljhkim.dsearch.proto.cluster.ClusterServiceGrpc;
import com.danieljhkim.dsearch.proto.cluster.ControlReplicaRepairsRequest;
import com.danieljhkim.dsearch.proto.cluster.GetReplicaRepairsRequest;
import com.danieljhkim.dsearch.proto.cluster.ReplicaRepairStatus;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/v1/admin")
public class AdminIndexController {

    private final GatewayAdminIndexService adminIndexService;
    private final NodeClientManager<ClusterServiceGrpc.ClusterServiceBlockingStub> clusterNodeClientManager;

    public AdminIndexController(
            GatewayAdminIndexService adminIndexService,
            @Qualifier("clusterNodeClientManager") NodeClientManager<ClusterServiceGrpc.ClusterServiceBlockingStub> clusterNodeClientManager) {
        this.adminIndexService = adminIndexService;
        this.clusterNodeClientManager = clusterNodeClientManager;
    }

    @PostMapping(value = "/indexes", consumes = "application/json", produces = "application/json")
    public AdminAuditResponseDto createIndex(
            @Valid @RequestBody CreateIndexRequestDto request, HttpServletRequest httpRequest) {
        return adminIndexService.createIndex(request, actor(httpRequest));
    }

    @GetMapping(value = "/indexes/{name}/schema", produces = "application/json")
    public InspectSchemaResponseDto inspectSchema(@PathVariable("name") String name, HttpServletRequest httpRequest) {
        PartitionIdValidator.validate(name);
        return adminIndexService.inspectSchema(name, actor(httpRequest));
    }

    @PostMapping(value = "/indexes/{name}/analyze", consumes = "application/json", produces = "application/json")
    public AnalyzeResponseDto analyze(
            @PathVariable("name") String name,
            @Valid @RequestBody AnalyzeRequestDto request,
            HttpServletRequest httpRequest) {
        PartitionIdValidator.validate(name);
        return adminIndexService.analyzeText(name, request, actor(httpRequest));
    }

    @PostMapping(value = "/indexes/{name}/reindex", consumes = "application/json", produces = "application/json")
    public AdminAuditResponseDto reindex(
            @PathVariable("name") String name,
            @Valid @RequestBody(required = false) ReindexRequestDto request,
            HttpServletRequest httpRequest) {
        PartitionIdValidator.validate(name);
        ReindexRequestDto body = request == null ? new ReindexRequestDto() : request;
        return adminIndexService.reindex(name, body, actor(httpRequest));
    }

    @PostMapping(value = "/aliases/swap", consumes = "application/json", produces = "application/json")
    public AdminAuditResponseDto swapAlias(
            @Valid @RequestBody AliasSwapRequestDto request, HttpServletRequest httpRequest) {
        return adminIndexService.swapAlias(request, actor(httpRequest));
    }

    @PostMapping(value = "/aliases/{alias}/rollback", produces = "application/json")
    public AdminAuditResponseDto rollbackAlias(@PathVariable("alias") String alias, HttpServletRequest httpRequest) {
        PartitionIdValidator.validate(alias);
        return adminIndexService.rollbackAlias(alias, actor(httpRequest));
    }

    @GetMapping(value = "/repairs", produces = "application/json")
    public ReplicaRepairsResponse replicaRepairs() {
        var response = coordinator().getReplicaRepairs(GetReplicaRepairsRequest.getDefaultInstance());
        return new ReplicaRepairsResponse(
                response.getPaused(),
                response.getRepairsList().stream().map(RepairStatus::from).toList());
    }

    @PostMapping(value = "/repairs/pause", produces = "application/json")
    public ReplicaRepairControlResponse pauseRepairs() {
        return controlRepairs("pause", "");
    }

    @PostMapping(value = "/repairs/resume", produces = "application/json")
    public ReplicaRepairControlResponse resumeRepairs() {
        return controlRepairs("resume", "");
    }

    @PostMapping(value = "/repairs/{repairId}/retry", produces = "application/json")
    public ReplicaRepairControlResponse retryRepair(@PathVariable String repairId) {
        if (repairId == null || !repairId.matches("[A-Za-z0-9._-]{1,160}")) {
            throw new IllegalArgumentException("repairId contains unsupported characters");
        }
        return controlRepairs("retry", repairId);
    }

    private ReplicaRepairControlResponse controlRepairs(String action, String repairId) {
        var response = coordinator()
                .controlReplicaRepairs(ControlReplicaRepairsRequest.newBuilder()
                        .setAction(action)
                        .setRepairId(repairId)
                        .build());
        return new ReplicaRepairControlResponse(response.getSuccess(), response.getPaused());
    }

    private ClusterServiceGrpc.ClusterServiceBlockingStub coordinator() {
        return clusterNodeClientManager.nextClient().withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    private static String actor(HttpServletRequest request) {
        Object actor = request.getAttribute(AdminAuthFilter.ACTOR_ATTRIBUTE);
        if (!(actor instanceof String value) || value.isBlank()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "missing authenticated admin actor");
        }
        return value;
    }

    public record ReplicaRepairsResponse(boolean paused, List<RepairStatus> repairs) {}

    public record ReplicaRepairControlResponse(boolean success, boolean paused) {}

    public record RepairStatus(
            String repairId,
            String shardId,
            String sourceNodeId,
            String targetNodeId,
            String state,
            long bytesTransferred,
            long totalBytes,
            int attempts,
            String lastError,
            long startedAtEpochMillis,
            long updatedAtEpochMillis) {
        private static RepairStatus from(ReplicaRepairStatus status) {
            return new RepairStatus(
                    status.getRepairId(),
                    status.getShardId(),
                    status.getSourceNodeId(),
                    status.getTargetNodeId(),
                    status.getState()
                            .name()
                            .substring("REPLICA_REPAIR_STATE_".length())
                            .toLowerCase(),
                    status.getBytesTransferred(),
                    status.getTotalBytes(),
                    status.getAttempts(),
                    status.getLastError(),
                    status.getStartedAtEpochMillis(),
                    status.getUpdatedAtEpochMillis());
        }
    }
}
