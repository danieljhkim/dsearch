package com.danieljhkim.dsearch.gateway.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.danieljhkim.dsearch.common.grpc.NodeClientManager;
import com.danieljhkim.dsearch.gateway.api.dto.AdminAuditResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.AnalyzeResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.InspectSchemaResponseDto;
import com.danieljhkim.dsearch.gateway.config.AdminAuthFilter;
import com.danieljhkim.dsearch.gateway.service.GatewayAdminIndexService;
import com.danieljhkim.dsearch.proto.cluster.ClusterServiceGrpc;
import com.danieljhkim.dsearch.proto.cluster.ControlReplicaRepairsResponse;
import com.danieljhkim.dsearch.proto.cluster.GetReplicaRepairsResponse;
import com.danieljhkim.dsearch.proto.cluster.ReplicaRepairState;
import com.danieljhkim.dsearch.proto.cluster.ReplicaRepairStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(controllers = AdminIndexController.class)
@Import(GlobalExceptionHandler.class)
class AdminIndexControllerTest {

    @jakarta.annotation.Resource
    private MockMvc mockMvc;

    @MockBean
    private GatewayAdminIndexService adminIndexService;

    @MockBean(name = "clusterNodeClientManager")
    private NodeClientManager<ClusterServiceGrpc.ClusterServiceBlockingStub> clusterNodeClientManager;

    @Test
    void createInspectReindexSwapAndRollbackReturnAuditableJson() throws Exception {
        when(adminIndexService.createIndex(any(), any())).thenReturn(audit("create-index", "movies", "movies_1", null));
        when(adminIndexService.inspectSchema(eq("movies"), any())).thenReturn(schema());
        when(adminIndexService.analyzeText(eq("movies"), any(), any())).thenReturn(analyzed());
        when(adminIndexService.reindex(eq("movies"), any(), any()))
                .thenReturn(audit("reindex", "movies", "movies_2", "movies_1"));
        when(adminIndexService.swapAlias(any(), any()))
                .thenReturn(audit("alias-swap", "movies", "movies_2", "movies_1"));
        when(adminIndexService.rollbackAlias(eq("movies"), any()))
                .thenReturn(audit("alias-rollback", "movies", "movies_1", "movies_2"));

        mockMvc.perform(post("/api/v1/admin/indexes")
                        .requestAttr(AdminAuthFilter.ACTOR_ATTRIBUTE, AdminAuthFilter.ADMIN_ACTOR)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"indexName\":\"movies_1\",\"alias\":\"movies\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.auditId").exists())
                .andExpect(jsonPath("$.operation").value("create-index"))
                .andExpect(jsonPath("$.success").value(true));

        mockMvc.perform(get("/api/v1/admin/indexes/movies/schema")
                        .requestAttr(AdminAuthFilter.ACTOR_ATTRIBUTE, AdminAuthFilter.ADMIN_ACTOR))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.compatibilityVersion").value(1))
                .andExpect(jsonPath("$.embedding.dimension").value(384));

        mockMvc.perform(post("/api/v1/admin/indexes/movies/analyze")
                        .requestAttr(AdminAuthFilter.ACTOR_ATTRIBUTE, AdminAuthFilter.ADMIN_ACTOR)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"text\":\"Interstellar\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.analyzer").value("standard"))
                .andExpect(jsonPath("$.tokens[0].token").value("interstellar"))
                .andExpect(jsonPath("$.truncated").value(false));

        mockMvc.perform(post("/api/v1/admin/indexes/movies/reindex")
                        .requestAttr(AdminAuthFilter.ACTOR_ATTRIBUTE, AdminAuthFilter.ADMIN_ACTOR)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"targetIndex\":\"movies_2\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.operation").value("reindex"));

        mockMvc.perform(post("/api/v1/admin/aliases/swap")
                        .requestAttr(AdminAuthFilter.ACTOR_ATTRIBUTE, AdminAuthFilter.ADMIN_ACTOR)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"alias\":\"movies\",\"targetIndex\":\"movies_2\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.operation").value("alias-swap"));

        mockMvc.perform(post("/api/v1/admin/aliases/movies/rollback")
                        .requestAttr(AdminAuthFilter.ACTOR_ATTRIBUTE, AdminAuthFilter.ADMIN_ACTOR))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.operation").value("alias-rollback"));

        verify(adminIndexService).createIndex(any(), eq(AdminAuthFilter.ADMIN_ACTOR));
    }

    @Test
    void missingOrInvalidActorNeverReachesTheAuditService() throws Exception {
        mockMvc.perform(post("/api/v1/admin/indexes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"indexName\":\"movies_1\",\"alias\":\"movies\"}"))
                .andExpect(status().isUnauthorized());
        for (Object actor : new Object[] {"", "   ", 123}) {
            mockMvc.perform(post("/api/v1/admin/indexes")
                            .requestAttr(AdminAuthFilter.ACTOR_ATTRIBUTE, actor)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"indexName\":\"movies_1\",\"alias\":\"movies\"}"))
                    .andExpect(status().isUnauthorized());
        }
        verifyNoInteractions(adminIndexService);
    }

    @Test
    void repairEndpointsExposeProgressAndControlTheCoordinator() throws Exception {
        ClusterServiceGrpc.ClusterServiceBlockingStub stub = mock(ClusterServiceGrpc.ClusterServiceBlockingStub.class);
        when(clusterNodeClientManager.nextClient()).thenReturn(stub);
        when(stub.withDeadlineAfter(anyLong(), eq(TimeUnit.SECONDS))).thenReturn(stub);
        when(stub.getReplicaRepairs(any()))
                .thenReturn(GetReplicaRepairsResponse.newBuilder()
                        .setPaused(false)
                        .addRepairs(ReplicaRepairStatus.newBuilder()
                                .setRepairId("repair-1")
                                .setShardId("tenant_r1")
                                .setSourceNodeId("n0")
                                .setTargetNodeId("n1")
                                .setState(ReplicaRepairState.REPLICA_REPAIR_STATE_TRANSFERRING)
                                .setBytesTransferred(10)
                                .setTotalBytes(20)
                                .build())
                        .build());
        when(stub.controlReplicaRepairs(any()))
                .thenReturn(ControlReplicaRepairsResponse.newBuilder()
                        .setSuccess(true)
                        .setPaused(true)
                        .build());

        mockMvc.perform(get("/api/v1/admin/repairs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paused").value(false))
                .andExpect(jsonPath("$.repairs[0].state").value("transferring"))
                .andExpect(jsonPath("$.repairs[0].bytesTransferred").value(10));
        mockMvc.perform(post("/api/v1/admin/repairs/pause"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.paused").value(true));
    }

    private static AdminAuditResponseDto audit(String operation, String alias, String index, String previous) {
        return new AdminAuditResponseDto(
                "audit-1",
                Instant.parse("2026-08-31T00:00:00Z"),
                operation,
                AdminAuthFilter.ADMIN_ACTOR,
                true,
                alias,
                index,
                previous,
                "ok",
                Map.of());
    }

    private static AnalyzeResponseDto analyzed() {
        AnalyzeResponseDto dto = new AnalyzeResponseDto();
        dto.setIndexName("movies_1");
        dto.setAlias("movies");
        dto.setAnalyzer("standard");
        dto.setTruncated(false);
        AnalyzeResponseDto.AnalyzedTokenDto token = new AnalyzeResponseDto.AnalyzedTokenDto();
        token.setToken("interstellar");
        token.setPosition(0);
        token.setStartOffset(0);
        token.setEndOffset(12);
        dto.setTokens(List.of(token));
        return dto;
    }

    private static InspectSchemaResponseDto schema() {
        InspectSchemaResponseDto dto = new InspectSchemaResponseDto();
        dto.setAuditId("audit-schema");
        dto.setIndexName("movies_1");
        dto.setAlias("movies");
        dto.setCompatibilityVersion(1);
        dto.setAnalyzer("standard");
        dto.setFields(List.of());
        InspectSchemaResponseDto.EmbeddingIdentityDto embedding = new InspectSchemaResponseDto.EmbeddingIdentityDto();
        embedding.setModelId("djl://model");
        embedding.setEngine("PyTorch");
        embedding.setDigest("sha256:abc");
        embedding.setDimension(384);
        dto.setEmbedding(embedding);
        return dto;
    }
}
