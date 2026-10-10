package com.danieljhkim.dsearch.gateway.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.danieljhkim.dsearch.common.config.AppConfig;
import com.danieljhkim.dsearch.common.enums.RoutingStrategy;
import com.danieljhkim.dsearch.common.exception.NodeUnavailableException;
import com.danieljhkim.dsearch.common.grpc.NodeClient;
import com.danieljhkim.dsearch.common.grpc.NodeClientManager;
import com.danieljhkim.dsearch.common.shard.ReplicaPlacement;
import com.danieljhkim.dsearch.gateway.api.dto.BulkDeleteItemRequestDto;
import com.danieljhkim.dsearch.gateway.api.dto.BulkDeleteRequestDto;
import com.danieljhkim.dsearch.gateway.api.dto.BulkDeleteResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.BulkIndexRequestDto;
import com.danieljhkim.dsearch.gateway.api.dto.BulkIndexResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.DocumentCountResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.GetDocumentResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.IndexRequestDto;
import com.danieljhkim.dsearch.gateway.api.dto.IndexResponseDto;
import com.danieljhkim.dsearch.proto.cluster.NodeRole;
import com.danieljhkim.dsearch.proto.index.BulkDeleteDocumentRequest;
import com.danieljhkim.dsearch.proto.index.BulkDeleteDocumentResponse;
import com.danieljhkim.dsearch.proto.index.BulkDeleteDocumentResult;
import com.danieljhkim.dsearch.proto.index.BulkIndexDocumentRequest;
import com.danieljhkim.dsearch.proto.index.BulkIndexDocumentResponse;
import com.danieljhkim.dsearch.proto.index.BulkIndexDocumentResult;
import com.danieljhkim.dsearch.proto.index.DeleteDocumentRequest;
import com.danieljhkim.dsearch.proto.index.DeleteDocumentResponse;
import com.danieljhkim.dsearch.proto.index.Document;
import com.danieljhkim.dsearch.proto.index.Field;
import com.danieljhkim.dsearch.proto.index.GetDocumentCountRequest;
import com.danieljhkim.dsearch.proto.index.GetDocumentCountResponse;
import com.danieljhkim.dsearch.proto.index.GetDocumentRequest;
import com.danieljhkim.dsearch.proto.index.GetDocumentResponse;
import com.danieljhkim.dsearch.proto.index.IndexDocumentRequest;
import com.danieljhkim.dsearch.proto.index.IndexDocumentResponse;
import com.danieljhkim.dsearch.proto.index.IndexServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.prometheus.client.CollectorRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GatewayIndexServiceTest {

    @Mock
    private NodeClientManager<IndexServiceGrpc.IndexServiceBlockingStub> indexNodeClientManager;

    @Mock
    private IndexServiceGrpc.IndexServiceBlockingStub indexStub;

    private NodeClient<IndexServiceGrpc.IndexServiceBlockingStub> ownerClient;
    private GatewayIndexService service;

    @BeforeEach
    void setUp() {
        ownerClient = new NodeClient<>("1", indexStub, mock(ManagedChannel.class), "localhost", 5101);
        service = new GatewayIndexService(indexNodeClientManager);
        lenient()
                .when(indexStub.withDeadlineAfter(anyLong(), any(TimeUnit.class)))
                .thenReturn(indexStub);
    }

    @Test
    void indexRoutesToTheOwnerOfTheDocumentKey() {
        IndexRequestDto request = new IndexRequestDto();
        request.setId("doc-1");
        request.setPartitionId("tenant-a");
        request.setFields(Map.of("title", "Distributed Search", "category", "docs"));
        when(indexNodeClientManager.ownerClient("tenant-a", "doc-1")).thenReturn(ownerClient);
        when(indexStub.indexDocument(any(IndexDocumentRequest.class)))
                .thenReturn(IndexDocumentResponse.newBuilder()
                        .setId("doc-1")
                        .setSuccess(true)
                        .build());

        IndexResponseDto response = service.index(request);

        assertThat(response.getId()).isEqualTo("doc-1");
        assertThat(response.isSuccess()).isTrue();

        ArgumentCaptor<IndexDocumentRequest> requestCaptor = ArgumentCaptor.forClass(IndexDocumentRequest.class);
        verify(indexStub).indexDocument(requestCaptor.capture());
        IndexDocumentRequest grpcRequest = requestCaptor.getValue();
        assertThat(grpcRequest.getPartitionId()).isEqualTo("tenant-a");
        assertThat(grpcRequest.getDocument().getId()).isEqualTo("doc-1");
        assertThat(grpcRequest.getDocument().getFieldsList())
                .extracting(Field::getName, Field::getValue)
                .containsExactlyInAnyOrder(tuple("title", "Distributed Search"), tuple("category", "docs"));
    }

    @Test
    void documentCountUsesOneReadPlanTargetPerLogicalShardAndReportsPartialObservations() {
        ReplicaPlacement.ReadTarget selectedReplica =
                new ReplicaPlacement.ReadTarget("logical-1", "1", "tenant-a/primary-1", true);
        when(indexNodeClientManager.replicaReadPlan("tenant-a"))
                .thenReturn(new ReplicaPlacement.ReadPlan(List.of(selectedReplica), List.of("logical-2")));
        when(indexNodeClientManager.getClientMap()).thenReturn(Map.of("1", ownerClient));
        when(indexStub.getDocumentCount(any(GetDocumentCountRequest.class)))
                .thenReturn(GetDocumentCountResponse.newBuilder()
                        .setDocumentCount(7)
                        .build());

        DocumentCountResponseDto response = service.documentCount("tenant-a");

        assertThat(response.documentCount()).isEqualTo(7);
        assertThat(response.unavailableLogicalShards()).containsExactly("logical-2");
        assertThat(response.failedLogicalShards()).isEmpty();
        assertThat(response.isComplete()).isFalse();
        ArgumentCaptor<GetDocumentCountRequest> requestCaptor = ArgumentCaptor.forClass(GetDocumentCountRequest.class);
        verify(indexStub).getDocumentCount(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getPartitionId()).isEqualTo("tenant-a/primary-1");
    }

    @Test
    void indexUsesTheDefaultPartitionWhenNoneIsSupplied() {
        IndexRequestDto request = new IndexRequestDto();
        request.setId("doc-1");
        when(indexNodeClientManager.ownerClient("default", "doc-1")).thenReturn(ownerClient);
        when(indexStub.indexDocument(any(IndexDocumentRequest.class)))
                .thenReturn(IndexDocumentResponse.newBuilder()
                        .setId("doc-1")
                        .setSuccess(true)
                        .build());

        service.index(request);

        ArgumentCaptor<IndexDocumentRequest> requestCaptor = ArgumentCaptor.forClass(IndexDocumentRequest.class);
        verify(indexStub).indexDocument(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getPartitionId()).isEqualTo("default");
    }

    @Test
    void indexMintsTheDocumentIdSoOwnershipIsKnownBeforeTheCall() {
        IndexRequestDto request = new IndexRequestDto();
        request.setPartitionId("tenant-a");
        request.setFields(Map.of("title", "Generated"));
        ArgumentCaptor<String> documentIdCaptor = ArgumentCaptor.forClass(String.class);
        when(indexNodeClientManager.ownerClient(any(), documentIdCaptor.capture()))
                .thenReturn(ownerClient);
        when(indexStub.indexDocument(any(IndexDocumentRequest.class))).thenAnswer(invocation -> {
            IndexDocumentRequest req = invocation.getArgument(0);
            return IndexDocumentResponse.newBuilder()
                    .setId(req.getDocument().getId())
                    .setSuccess(true)
                    .build();
        });

        IndexResponseDto response = service.index(request);

        ArgumentCaptor<IndexDocumentRequest> requestCaptor = ArgumentCaptor.forClass(IndexDocumentRequest.class);
        verify(indexStub).indexDocument(requestCaptor.capture());
        String routedId = documentIdCaptor.getValue();
        assertThat(routedId).isNotBlank();
        // The id used for routing must be the id the node stores, otherwise a later update
        // of the same document could be routed to a different node.
        assertThat(requestCaptor.getValue().getDocument().getId()).isEqualTo(routedId);
        assertThat(response.getId()).isEqualTo(routedId);
    }

    @Test
    void indexDoesNotCountDocumentsTheOwnerRejected() {
        IndexRequestDto request = new IndexRequestDto();
        request.setId("doc-1");
        request.setPartitionId("tenant-a");
        when(indexNodeClientManager.ownerClient("tenant-a", "doc-1")).thenReturn(ownerClient);
        when(indexStub.indexDocument(any(IndexDocumentRequest.class)))
                .thenReturn(IndexDocumentResponse.newBuilder()
                        .setId("doc-1")
                        .setSuccess(false)
                        .build());

        IndexResponseDto response = service.index(request);

        assertThat(response.isSuccess()).isFalse();
    }

    @Test
    void indexPropagatesOwnerUnavailabilityInsteadOfWritingElsewhere() {
        IndexRequestDto request = new IndexRequestDto();
        request.setId("doc-1");
        request.setPartitionId("tenant-a");
        when(indexNodeClientManager.ownerClient("tenant-a", "doc-1"))
                .thenThrow(new NodeUnavailableException("1", "owner down"));

        assertThatThrownBy(() -> service.index(request)).isInstanceOf(NodeUnavailableException.class);

        verify(indexStub, never()).indexDocument(any(IndexDocumentRequest.class));
    }

    @Test
    void indexRejectsNullFieldValuesBeforeOwnerLookup() {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("title", null);
        IndexRequestDto request = indexRequest("doc-1", fields);

        assertThatThrownBy(() -> service.index(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("field value must not be null");

        verify(indexNodeClientManager, never()).ownerClient(any(), any());
        verify(indexStub, never()).indexDocument(any(IndexDocumentRequest.class));
    }

    @Test
    void deleteRoutesToTheOwner() {
        when(indexNodeClientManager.ownerClient("tenant-a", "doc-9")).thenReturn(ownerClient);
        when(indexStub.deleteDocument(any(DeleteDocumentRequest.class)))
                .thenReturn(DeleteDocumentResponse.newBuilder().setSuccess(true).build());

        IndexResponseDto response = service.delete("doc-9", "tenant-a");

        assertThat(response.getId()).isEqualTo("doc-9");
        assertThat(response.isSuccess()).isTrue();

        ArgumentCaptor<DeleteDocumentRequest> requestCaptor = ArgumentCaptor.forClass(DeleteDocumentRequest.class);
        verify(indexStub).deleteDocument(requestCaptor.capture());
        DeleteDocumentRequest grpcRequest = requestCaptor.getValue();
        assertThat(grpcRequest.getPartitionId()).isEqualTo("tenant-a");
        assertThat(grpcRequest.getId()).isEqualTo("doc-9");
    }

    @Test
    void deleteReturnsAnUnsuccessfulOwnerResponse() {
        when(indexNodeClientManager.ownerClient("tenant-a", "doc-9")).thenReturn(ownerClient);
        when(indexStub.deleteDocument(any(DeleteDocumentRequest.class)))
                .thenReturn(
                        DeleteDocumentResponse.newBuilder().setSuccess(false).build());

        IndexResponseDto response = service.delete("doc-9", "tenant-a");

        assertThat(response.isSuccess()).isFalse();
    }

    @Test
    void deletePropagatesCallFailure() {
        when(indexNodeClientManager.ownerClient("tenant-a", "doc-9")).thenReturn(ownerClient);
        when(indexStub.deleteDocument(any(DeleteDocumentRequest.class)))
                .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));

        assertThatThrownBy(() -> service.delete("doc-9", "tenant-a")).isInstanceOf(StatusRuntimeException.class);
    }

    @Test
    void deletePropagatesOwnerUnavailability() {
        when(indexNodeClientManager.ownerClient("tenant-a", "doc-9"))
                .thenThrow(new NodeUnavailableException("1", "owner down"));

        assertThatThrownBy(() -> service.delete("doc-9", "tenant-a"))
                .isInstanceOf(NodeUnavailableException.class)
                .hasMessageContaining("owner down");

        verify(indexStub, never()).deleteDocument(any(DeleteDocumentRequest.class));
    }

    @Test
    void deleteRejectsBlankIds() {
        assertThatThrownBy(() -> service.delete("  ", "tenant-a")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bulkUsesTheGrpcBulkContractAndRestoresInputOrder() {
        IndexRequestDto first = indexRequest("doc-1", Map.of("title", "first"));
        IndexRequestDto second = indexRequest("doc-2", Map.of("title", "second"));
        when(indexNodeClientManager.ownerClient("tenant-a", "doc-1")).thenReturn(ownerClient);
        when(indexNodeClientManager.ownerClient("tenant-a", "doc-2")).thenReturn(ownerClient);
        when(indexStub.bulkIndexDocument(any(BulkIndexDocumentRequest.class)))
                .thenReturn(BulkIndexDocumentResponse.newBuilder()
                        .setSuccess(true)
                        .addResults(BulkIndexDocumentResult.newBuilder()
                                .setRequestIndex(1)
                                .setId("doc-2")
                                .setSuccess(true))
                        .addResults(BulkIndexDocumentResult.newBuilder()
                                .setRequestIndex(0)
                                .setId("doc-1")
                                .setSuccess(true))
                        .build());

        BulkIndexResponseDto response = service.bulkIndex(bulkRequest("tenant-a", first, second));

        assertThat(response.isSuccess()).isTrue();
        assertThat(response.getItems()).extracting(item -> item.getId()).containsExactly("doc-1", "doc-2");
        assertThat(response.getItems()).extracting(item -> item.getStatus()).containsOnly("success");
        ArgumentCaptor<BulkIndexDocumentRequest> requestCaptor =
                ArgumentCaptor.forClass(BulkIndexDocumentRequest.class);
        verify(indexStub).bulkIndexDocument(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getPartitionId()).isEqualTo("tenant-a");
        assertThat(requestCaptor.getValue().getDocumentsList())
                .extracting(document -> document.getId())
                .containsExactly("doc-1", "doc-2");
    }

    @Test
    void bulkKeepsSuccessfulWritesWhenOtherItemsFailValidation() {
        IndexRequestDto missingId = indexRequest(null, Map.of("title", "invalid"));
        IndexRequestDto valid = indexRequest("doc-2", Map.of("title", "valid"));
        when(indexNodeClientManager.ownerClient("default", "doc-2")).thenReturn(ownerClient);
        when(indexStub.bulkIndexDocument(any(BulkIndexDocumentRequest.class)))
                .thenReturn(successfulBulkResult(0, "doc-2"));

        BulkIndexResponseDto response = service.bulkIndex(bulkRequest(null, missingId, valid));

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getItems())
                .extracting(item -> item.getStatus())
                .containsExactly("validation_failure", "success");
        verify(indexStub).bulkIndexDocument(any(BulkIndexDocumentRequest.class));
    }

    @Test
    void bulkReportsNullFieldValuesWithoutPreventingValidItems() {
        Map<String, String> invalidFields = new LinkedHashMap<>();
        invalidFields.put("title", null);
        IndexRequestDto invalid = indexRequest("doc-1", invalidFields);
        IndexRequestDto valid = indexRequest("doc-2", Map.of("title", "valid"));
        when(indexNodeClientManager.ownerClient("default", "doc-2")).thenReturn(ownerClient);
        when(indexStub.bulkIndexDocument(any(BulkIndexDocumentRequest.class)))
                .thenReturn(successfulBulkResult(0, "doc-2"));

        BulkIndexResponseDto response = service.bulkIndex(bulkRequest(null, invalid, valid));

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getItems())
                .extracting(item -> item.getStatus())
                .containsExactly("validation_failure", "success");
        assertThat(response.getItems().getFirst().getError()).isEqualTo("field value must not be null");
        verify(indexNodeClientManager, never()).ownerClient("default", "doc-1");
        ArgumentCaptor<BulkIndexDocumentRequest> requestCaptor =
                ArgumentCaptor.forClass(BulkIndexDocumentRequest.class);
        verify(indexStub).bulkIndexDocument(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getDocumentsList())
                .extracting(document -> document.getId())
                .containsExactly("doc-2");
    }

    @Test
    void bulkRejectsDuplicateIdsWithoutApplyingTheSecondItem() {
        IndexRequestDto first = indexRequest("doc-1", Map.of("title", "first"));
        IndexRequestDto duplicate = indexRequest("doc-1", Map.of("title", "second"));
        when(indexNodeClientManager.ownerClient("default", "doc-1")).thenReturn(ownerClient);
        when(indexStub.bulkIndexDocument(any(BulkIndexDocumentRequest.class)))
                .thenReturn(successfulBulkResult(0, "doc-1"));

        BulkIndexResponseDto response = service.bulkIndex(bulkRequest(null, first, duplicate));

        assertThat(response.getItems())
                .extracting(item -> item.getStatus())
                .containsExactly("success", "validation_failure");
        ArgumentCaptor<BulkIndexDocumentRequest> requestCaptor =
                ArgumentCaptor.forClass(BulkIndexDocumentRequest.class);
        verify(indexStub).bulkIndexDocument(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getDocumentsCount()).isOne();
    }

    @Test
    void bulkTimeoutIsRetryableWithTheSameId() {
        IndexRequestDto item = indexRequest("doc-1", Map.of("title", "retry"));
        when(indexNodeClientManager.ownerClient("default", "doc-1")).thenReturn(ownerClient);
        when(indexStub.bulkIndexDocument(any(BulkIndexDocumentRequest.class)))
                .thenThrow(new StatusRuntimeException(Status.DEADLINE_EXCEEDED))
                .thenReturn(successfulBulkResult(0, "doc-1"));

        BulkIndexResponseDto timedOut = service.bulkIndex(bulkRequest(null, item));
        BulkIndexResponseDto retried = service.bulkIndex(bulkRequest(null, item));

        assertThat(timedOut.getItems().getFirst().getStatus()).isEqualTo("retryable_failure");
        assertThat(retried.getItems().getFirst().getStatus()).isEqualTo("success");
        ArgumentCaptor<BulkIndexDocumentRequest> requestCaptor =
                ArgumentCaptor.forClass(BulkIndexDocumentRequest.class);
        verify(indexStub, org.mockito.Mockito.times(2)).bulkIndexDocument(requestCaptor.capture());
        assertThat(requestCaptor.getAllValues())
                .allSatisfy(
                        request -> assertThat(request.getDocuments(0).getId()).isEqualTo("doc-1"));
    }

    @Test
    void bulkReportsPermanentOwnerValidationFailuresPerItem() {
        IndexRequestDto item = indexRequest("doc-1", Map.of("title", "invalid downstream"));
        when(indexNodeClientManager.ownerClient("default", "doc-1")).thenReturn(ownerClient);
        when(indexStub.bulkIndexDocument(any(BulkIndexDocumentRequest.class)))
                .thenThrow(
                        new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("owner validation failed")));

        BulkIndexResponseDto response = service.bulkIndex(bulkRequest(null, item));

        assertThat(response.getItems().getFirst().getStatus()).isEqualTo("permanent_failure");
        assertThat(response.getItems().getFirst().getError()).isEqualTo("owner validation failed");
    }

    @Test
    void bulkRejectsItemAndEmbeddingWorkLimitsBeforeCallingOwners() {
        AppConfig config = new AppConfig();
        AppConfig.RequestLimitsConfig limits = new AppConfig.RequestLimitsConfig();
        limits.setMaxBulkItems(1);
        limits.setMaxBulkEmbeddingBytes(2);
        config.setRequestLimits(limits);
        service = new GatewayIndexService(indexNodeClientManager, config);

        assertThatThrownBy(() -> service.bulkIndex(
                        bulkRequest(null, indexRequest("doc-1", Map.of()), indexRequest("doc-2", Map.of()))))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class)
                .hasMessageContaining("Bulk item count");
        verify(indexNodeClientManager, never()).ownerClient(any(), any());

        limits.setMaxBulkItems(2);
        assertThatThrownBy(() ->
                        service.bulkIndex(bulkRequest(null, indexRequest("doc-1", Map.of("title", "too long")))))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class)
                .hasMessageContaining("Bulk embedding bytes");
        verify(indexNodeClientManager, never()).ownerClient(any(), any());
    }

    @Test
    void bulkDeleteRoutesAcrossMultipleOwnersAndPreservesOrder() {
        IndexServiceGrpc.IndexServiceBlockingStub secondStub = mock(IndexServiceGrpc.IndexServiceBlockingStub.class);
        NodeClient<IndexServiceGrpc.IndexServiceBlockingStub> secondOwner =
                new NodeClient<>("2", secondStub, mock(ManagedChannel.class), "localhost", 5102);
        lenient()
                .when(secondStub.withDeadlineAfter(anyLong(), any(TimeUnit.class)))
                .thenReturn(secondStub);
        when(indexNodeClientManager.ownerClient("tenant-a", "doc-1")).thenReturn(ownerClient);
        when(indexNodeClientManager.ownerClient("tenant-a", "doc-2")).thenReturn(secondOwner);
        when(indexStub.bulkDeleteDocument(any(BulkDeleteDocumentRequest.class)))
                .thenReturn(successfulBulkDeleteResult(0, "doc-1"));
        when(secondStub.bulkDeleteDocument(any(BulkDeleteDocumentRequest.class)))
                .thenReturn(successfulBulkDeleteResult(0, "doc-2"));

        BulkDeleteResponseDto response = service.bulkDelete(bulkDeleteRequest("tenant-a", "doc-1", "doc-2"));

        assertThat(response.isSuccess()).isTrue();
        assertThat(response.getItems()).extracting(item -> item.getId()).containsExactly("doc-1", "doc-2");
        assertThat(response.getItems()).extracting(item -> item.getStatus()).containsOnly("success");
        ArgumentCaptor<BulkDeleteDocumentRequest> firstCaptor =
                ArgumentCaptor.forClass(BulkDeleteDocumentRequest.class);
        verify(indexStub).bulkDeleteDocument(firstCaptor.capture());
        assertThat(firstCaptor.getValue().getIdsList()).containsExactly("doc-1");
        ArgumentCaptor<BulkDeleteDocumentRequest> secondCaptor =
                ArgumentCaptor.forClass(BulkDeleteDocumentRequest.class);
        verify(secondStub).bulkDeleteDocument(secondCaptor.capture());
        assertThat(secondCaptor.getValue().getIdsList()).containsExactly("doc-2");
    }

    @Test
    void bulkDeletePropagatesOwnerUnavailabilityAsRetryableWithoutHidingOtherOutcomes() {
        when(indexNodeClientManager.ownerClient("tenant-a", "doc-1"))
                .thenThrow(new NodeUnavailableException("1", "owner down"));
        when(indexNodeClientManager.ownerClient("tenant-a", "doc-2")).thenReturn(ownerClient);
        when(indexStub.bulkDeleteDocument(any(BulkDeleteDocumentRequest.class)))
                .thenReturn(successfulBulkDeleteResult(0, "doc-2"));

        BulkDeleteResponseDto response = service.bulkDelete(bulkDeleteRequest("tenant-a", "doc-1", "doc-2"));

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getItems())
                .extracting(item -> item.getStatus())
                .containsExactly("retryable_failure", "success");
        assertThat(response.getItems().getFirst().getError()).contains("owner down");
        verify(indexStub, never()).deleteDocument(any(DeleteDocumentRequest.class));
    }

    @Test
    void bulkDeleteTimeoutIsRetryableWithTheSameIdAndRetryCommitsDurably() {
        when(indexNodeClientManager.ownerClient("default", "doc-1")).thenReturn(ownerClient);
        when(indexStub.bulkDeleteDocument(any(BulkDeleteDocumentRequest.class)))
                .thenThrow(new StatusRuntimeException(Status.DEADLINE_EXCEEDED))
                .thenReturn(successfulBulkDeleteResult(0, "doc-1"));

        BulkDeleteResponseDto timedOut = service.bulkDelete(bulkDeleteRequest(null, "doc-1"));
        BulkDeleteResponseDto retried = service.bulkDelete(bulkDeleteRequest(null, "doc-1"));

        assertThat(timedOut.getItems().getFirst().getStatus()).isEqualTo("retryable_failure");
        assertThat(timedOut.getItems().getFirst().getError()).contains("idempotent");
        assertThat(retried.getItems().getFirst().getStatus()).isEqualTo("success");
        verify(indexStub, org.mockito.Mockito.times(2)).bulkDeleteDocument(any(BulkDeleteDocumentRequest.class));
    }

    @Test
    void bulkDeletePreservesDuplicateIdsAsIndependentOrderedOutcomes() {
        when(indexNodeClientManager.ownerClient("default", "doc-1")).thenReturn(ownerClient);
        when(indexStub.bulkDeleteDocument(any(BulkDeleteDocumentRequest.class)))
                .thenReturn(BulkDeleteDocumentResponse.newBuilder()
                        .setSuccess(true)
                        .addResults(BulkDeleteDocumentResult.newBuilder()
                                .setRequestIndex(0)
                                .setId("doc-1")
                                .setSuccess(true))
                        .addResults(BulkDeleteDocumentResult.newBuilder()
                                .setRequestIndex(1)
                                .setId("doc-1")
                                .setSuccess(true))
                        .build());

        BulkDeleteResponseDto response = service.bulkDelete(bulkDeleteRequest(null, "doc-1", "doc-1"));

        assertThat(response.isSuccess()).isTrue();
        assertThat(response.getItems()).hasSize(2);
        assertThat(response.getItems()).extracting(item -> item.getStatus()).containsExactly("success", "success");
        ArgumentCaptor<BulkDeleteDocumentRequest> requestCaptor =
                ArgumentCaptor.forClass(BulkDeleteDocumentRequest.class);
        verify(indexStub).bulkDeleteDocument(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getIdsList()).containsExactly("doc-1", "doc-1");
    }

    @Test
    void bulkDeleteReportsBlankIdsAsValidationFailureWithoutHidingOtherOutcomes() {
        when(indexNodeClientManager.ownerClient("default", "doc-2")).thenReturn(ownerClient);
        when(indexStub.bulkDeleteDocument(any(BulkDeleteDocumentRequest.class)))
                .thenReturn(successfulBulkDeleteResult(0, "doc-2"));

        BulkDeleteResponseDto response = service.bulkDelete(bulkDeleteRequest(null, "  ", "doc-2"));

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getItems())
                .extracting(item -> item.getStatus())
                .containsExactly("validation_failure", "success");
        verify(indexNodeClientManager, never()).ownerClient("default", "  ");
    }

    @Test
    void bulkDeleteRejectsItemCountOverBudgetBeforeAnyOwnerCall() {
        AppConfig config = new AppConfig();
        AppConfig.RequestLimitsConfig limits = new AppConfig.RequestLimitsConfig();
        limits.setMaxBulkItems(1);
        config.setRequestLimits(limits);
        service = new GatewayIndexService(indexNodeClientManager, config);

        assertThatThrownBy(() -> service.bulkDelete(bulkDeleteRequest(null, "doc-1", "doc-2")))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class)
                .hasMessageContaining("Bulk item count");
        verify(indexNodeClientManager, never()).ownerClient(any(), any());
    }

    @Test
    void allPolicyWritesPrimaryThenReplicaWithStableIdentity() {
        ReplicatedFixture fixture = replicatedFixture();
        IndexRequestDto request = indexRequest("doc-replicated", Map.of("title", "replicated"));
        request.setPartitionId("tenant-a");
        request.setOperationId("operation-42");
        request.setGeneration(42L);

        IndexResponseDto response = fixture.service().index(request);

        assertThat(response.isSuccess()).isTrue();
        assertThat(response.getAcknowledgements()).isEqualTo(2);
        assertThat(response.getRequiredAcknowledgements()).isEqualTo(2);
        assertThat(response.getOperationId()).isEqualTo("operation-42");
        assertThat(fixture.requests())
                .extracting(requestValue -> requestValue.getMutation().getReplica())
                .containsExactly(false, true);
        fixture.stubs().values().forEach(stub -> {
            ArgumentCaptor<IndexDocumentRequest> captor = ArgumentCaptor.forClass(IndexDocumentRequest.class);
            verify(stub).indexDocument(captor.capture());
            assertThat(captor.getValue().getMutation().getOperationId()).isEqualTo("operation-42");
            assertThat(captor.getValue().getMutation().getOperationGeneration()).isEqualTo(42L);
        });
    }

    @Test
    void oneReturnsAfterPrimaryWhileBoundedFollowersRemainObservableAndStopOnShutdown() throws Exception {
        ReplicatedFixture fixture = replicatedFixture(3, ReplicaPlacement.DurabilityPolicy.ONE);
        IndexRequestDto request = indexRequest("doc-one", Map.of("title", "one"));
        request.setPartitionId("tenant-a");
        request.setOperationId("operation-one");
        request.setGeneration(11L);
        var followers = fixture.manager()
                .replicaWritePlan("tenant-a", "doc-one")
                .targets()
                .subList(1, 3);
        CountDownLatch followersStarted = new CountDownLatch(2);
        CountDownLatch releaseFollowers = new CountDownLatch(1);
        CountDownLatch followersInterrupted = new CountDownLatch(2);
        followers.forEach(target -> blockIndex(
                fixture.stubs().get(target.nodeId()), followersStarted, releaseFollowers, followersInterrupted));
        double cancelledBefore = metricValue("index", "cancelled");

        try {
            IndexResponseDto response = CompletableFuture.supplyAsync(
                            () -> fixture.service().index(request))
                    .get(1, TimeUnit.SECONDS);

            assertThat(response.getAcknowledgements()).isEqualTo(1);
            assertThat(response.getRequiredAcknowledgements()).isEqualTo(1);
            assertThat(followersStarted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(CollectorRegistry.defaultRegistry.getSampleValue("dsearch_replication_unacknowledged_replicas"))
                    .isEqualTo(2.0);

            fixture.service().shutdownReplication();

            assertThat(followersInterrupted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(metricValue("index", "cancelled") - cancelledBefore).isEqualTo(2.0);
        } finally {
            releaseFollowers.countDown();
            fixture.service().shutdownReplication();
        }
    }

    @Test
    void quorumReturnsAfterPrimaryAndOneFollowerWithoutWaitingForBlockedFollower() throws Exception {
        ReplicatedFixture fixture = replicatedFixture(3, ReplicaPlacement.DurabilityPolicy.QUORUM);
        IndexRequestDto request = indexRequest("doc-quorum", Map.of("title", "quorum"));
        request.setPartitionId("tenant-a");
        request.setOperationId("operation-quorum");
        request.setGeneration(21L);
        var plan = fixture.manager().replicaWritePlan("tenant-a", "doc-quorum");
        var blockedFollower = plan.targets().getLast();
        CountDownLatch blockedStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocked = new CountDownLatch(1);
        blockIndex(
                fixture.stubs().get(blockedFollower.nodeId()), blockedStarted, releaseBlocked, new CountDownLatch(0));

        try {
            IndexResponseDto response = CompletableFuture.supplyAsync(
                            () -> fixture.service().index(request))
                    .get(1, TimeUnit.SECONDS);

            assertThat(response.getAcknowledgements()).isEqualTo(2);
            assertThat(response.getRequiredAcknowledgements()).isEqualTo(2);
            assertThat(blockedStarted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(fixture.requests().getFirst().getMutation().getReplica()).isFalse();
        } finally {
            releaseBlocked.countDown();
            fixture.service().shutdownReplication();
        }
    }

    @Test
    void followerFailureAfterOneAcknowledgementLeavesObservableLagForRepair() throws Exception {
        ReplicatedFixture fixture = replicatedFixture(3, ReplicaPlacement.DurabilityPolicy.ONE);
        IndexRequestDto request = indexRequest("doc-lag", Map.of("title", "lag"));
        request.setPartitionId("tenant-a");
        request.setGeneration(25L);
        var followers = fixture.manager()
                .replicaWritePlan("tenant-a", "doc-lag")
                .targets()
                .subList(1, 3);
        CountDownLatch failureStarted = new CountDownLatch(1);
        CountDownLatch allowFailure = new CountDownLatch(1);
        failIndexAfterRelease(fixture.stubs().get(followers.getFirst().nodeId()), failureStarted, allowFailure);
        CountDownLatch blockedStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocked = new CountDownLatch(1);
        blockIndex(
                fixture.stubs().get(followers.getLast().nodeId()),
                blockedStarted,
                releaseBlocked,
                new CountDownLatch(0));
        double failuresBefore = metricValue("index", "retryable_failure");

        try {
            IndexResponseDto response = CompletableFuture.supplyAsync(
                            () -> fixture.service().index(request))
                    .get(1, TimeUnit.SECONDS);
            assertThat(response.getAcknowledgements()).isEqualTo(1);
            assertThat(failureStarted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(blockedStarted.await(1, TimeUnit.SECONDS)).isTrue();

            allowFailure.countDown();
            awaitMetric("index", "retryable_failure", failuresBefore + 1);
            assertThat(CollectorRegistry.defaultRegistry.getSampleValue("dsearch_replication_unacknowledged_replicas"))
                    .isEqualTo(2.0);

            releaseBlocked.countDown();
            awaitReplicaLag(1.0);
        } finally {
            allowFailure.countDown();
            releaseBlocked.countDown();
            fixture.service().shutdownReplication();
        }
    }

    @Test
    void allStillWaitsForEveryReplica() throws Exception {
        ReplicatedFixture fixture = replicatedFixture(3, ReplicaPlacement.DurabilityPolicy.ALL);
        IndexRequestDto request = indexRequest("doc-all", Map.of("title", "all"));
        request.setPartitionId("tenant-a");
        request.setOperationId("operation-all");
        request.setGeneration(31L);
        var plan = fixture.manager().replicaWritePlan("tenant-a", "doc-all");
        CountDownLatch blockedStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocked = new CountDownLatch(1);
        blockIndex(
                fixture.stubs().get(plan.targets().getLast().nodeId()),
                blockedStarted,
                releaseBlocked,
                new CountDownLatch(0));
        CompletableFuture<IndexResponseDto> response =
                CompletableFuture.supplyAsync(() -> fixture.service().index(request));

        try {
            assertThat(blockedStarted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(response.isDone()).isFalse();
            releaseBlocked.countDown();

            assertThat(response.get(1, TimeUnit.SECONDS).getAcknowledgements()).isEqualTo(3);
        } finally {
            releaseBlocked.countDown();
            fixture.service().shutdownReplication();
        }
    }

    @Test
    void deleteUsesTheSameQuorumCompletionPointAndStableIdentity() throws Exception {
        ReplicatedFixture fixture = replicatedFixture(3, ReplicaPlacement.DurabilityPolicy.QUORUM);
        var plan = fixture.manager().replicaWritePlan("tenant-a", "doc-delete-quorum");
        var blockedFollower = plan.targets().getLast();
        CountDownLatch blockedStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocked = new CountDownLatch(1);
        blockDelete(
                fixture.stubs().get(blockedFollower.nodeId()), blockedStarted, releaseBlocked, new CountDownLatch(0));

        try {
            IndexResponseDto response = CompletableFuture.supplyAsync(
                            () -> fixture.service().delete("doc-delete-quorum", "tenant-a", "delete-quorum", 41L))
                    .get(1, TimeUnit.SECONDS);

            assertThat(response.getAcknowledgements()).isEqualTo(2);
            assertThat(response.getRequiredAcknowledgements()).isEqualTo(2);
            assertThat(response.getOperationId()).isEqualTo("delete-quorum");
            assertThat(response.getGeneration()).isEqualTo(41L);
            assertThat(blockedStarted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(fixture.deleteRequests().getFirst().getMutation().getReplica())
                    .isFalse();
            assertThat(fixture.deleteRequests()).allSatisfy(delete -> {
                assertThat(delete.getMutation().getOperationId()).isEqualTo("delete-quorum");
                assertThat(delete.getMutation().getOperationGeneration()).isEqualTo(41L);
            });
        } finally {
            releaseBlocked.countDown();
            fixture.service().shutdownReplication();
        }
    }

    @Test
    void omittedGenerationUsesTheSharedPrimaryAcrossOlderAndRestartedGateways() {
        ReplicatedFixture fixture = replicatedFixture();
        GatewayIndexService olderGateway = fixture.service();
        GatewayIndexService restartedGateway = new GatewayIndexService(fixture.manager());

        IndexRequestDto first = indexRequest("doc-shared", Map.of("title", "first"));
        first.setPartitionId("tenant-a");
        first.setOperationId("operation-first");
        IndexRequestDto second = indexRequest("doc-shared", Map.of("title", "second"));
        second.setPartitionId("tenant-a");
        second.setOperationId("operation-second");
        IndexRequestDto third = indexRequest("doc-shared", Map.of("title", "third"));
        third.setPartitionId("tenant-a");
        third.setOperationId("operation-third");

        IndexResponseDto firstResponse = olderGateway.index(first);
        IndexResponseDto secondResponse = restartedGateway.index(second);
        IndexResponseDto thirdResponse = olderGateway.index(third);

        assertThat(List.of(
                        firstResponse.getGeneration(), secondResponse.getGeneration(), thirdResponse.getGeneration()))
                .containsExactly(1L, 2L, 3L);
        assertThat(fixture.requests())
                .filteredOn(requestValue -> !requestValue.getMutation().getReplica())
                .extracting(requestValue -> requestValue.getMutation().getOperationGeneration())
                .containsExactly(0L, 0L, 0L);
        assertThat(fixture.requests())
                .filteredOn(requestValue -> requestValue.getMutation().getReplica())
                .extracting(requestValue -> requestValue.getMutation().getOperationGeneration())
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    void replicationFactorOneStillAllocatesAndReturnsThePrimaryGeneration() {
        ReplicatedFixture fixture = replicatedFixture(1);
        IndexRequestDto request = indexRequest("doc-single-copy", Map.of("title", "single"));
        request.setPartitionId("tenant-a");
        request.setOperationId("operation-single");

        IndexResponseDto response = fixture.service().index(request);

        assertThat(response.getGeneration()).isEqualTo(1L);
        assertThat(response.getAcknowledgements()).isEqualTo(1);
        assertThat(response.getRequiredAcknowledgements()).isEqualTo(1);
        assertThat(fixture.requests()).hasSize(1);
        assertThat(fixture.requests().getFirst().getMutation().getReplica()).isFalse();
        assertThat(fixture.requests().getFirst().getMutation().getOperationGeneration())
                .isZero();
    }

    @Test
    void callerProvidedGenerationMustBePositive() {
        ReplicatedFixture fixture = replicatedFixture();
        IndexRequestDto request = indexRequest("doc-invalid-generation", Map.of("title", "invalid"));
        request.setGeneration(0L);

        assertThatThrownBy(() -> fixture.service().index(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("generation must be positive");
        assertThat(fixture.requests()).isEmpty();
    }

    @Test
    void singleDeleteForwardsCallerIdentityAndGeneration() {
        ReplicatedFixture fixture = replicatedFixture();

        IndexResponseDto response = fixture.service().delete("doc-delete", "tenant-a", "delete-42", 42L);

        assertThat(response.getOperationId()).isEqualTo("delete-42");
        assertThat(response.getGeneration()).isEqualTo(42L);
        assertThat(fixture.deleteRequests())
                .extracting(request -> request.getMutation().getOperationId())
                .containsExactly("delete-42", "delete-42");
        assertThat(fixture.deleteRequests())
                .extracting(request -> request.getMutation().getOperationGeneration())
                .containsExactly(42L, 42L);
    }

    @Test
    void replicationFactorOneBulkDeletePreservesIdentityContract() {
        ReplicatedFixture fixture = replicatedFixture(1);
        BulkDeleteItemRequestDto item = new BulkDeleteItemRequestDto("doc-delete", "delete-one", 9L);

        BulkDeleteResponseDto response = fixture.service().bulkDelete(bulkDeleteItemRequest("tenant-a", item));

        assertThat(response.isSuccess()).isTrue();
        assertThat(response.getItems().getFirst().getOperationId()).isEqualTo("delete-one");
        assertThat(response.getItems().getFirst().getGeneration()).isEqualTo(9L);
        assertThat(fixture.deleteRequests()).hasSize(1);
        assertThat(fixture.deleteRequests().getFirst().getMutation().getReplica())
                .isFalse();
    }

    @Test
    void oldDeleteRetryCannotRemoveANewerAcknowledgedUpsert() {
        ReplicatedFixture fixture = replicatedFixture();
        IndexRequestDto original = indexRequest("doc-race", Map.of("title", "original"));
        original.setPartitionId("tenant-a");
        original.setOperationId("upsert-original");
        original.setGeneration(1L);
        fixture.service().index(original);

        fixture.failNextFollowerDelete().set(true);
        BulkDeleteResponseDto uncertain = fixture.service().bulkDelete(bulkDeleteRequest("tenant-a", "doc-race"));
        var uncertainItem = uncertain.getItems().getFirst();
        assertThat(uncertainItem.getStatus()).isEqualTo("retryable_failure");
        assertThat(uncertainItem.getOperationId()).isNotBlank();
        assertThat(uncertainItem.getGeneration()).isEqualTo(2L);

        IndexRequestDto newer = indexRequest("doc-race", Map.of("title", "newer"));
        newer.setPartitionId("tenant-a");
        newer.setOperationId("upsert-newer");
        newer.setGeneration(3L);
        IndexResponseDto newerResponse = fixture.service().index(newer);
        assertThat(newerResponse.getAcknowledgements()).isEqualTo(2);

        BulkDeleteItemRequestDto retry =
                new BulkDeleteItemRequestDto("doc-race", uncertainItem.getOperationId(), uncertainItem.getGeneration());
        BulkDeleteResponseDto retried = fixture.service().bulkDelete(bulkDeleteItemRequest("tenant-a", retry));

        assertThat(retried.getItems().getFirst().getStatus()).isEqualTo("permanent_failure");
        assertThat(retried.getItems().getFirst().getOperationId()).isEqualTo(uncertainItem.getOperationId());
        assertThat(retried.getItems().getFirst().getGeneration()).isEqualTo(2L);
        assertThat(fixture.replicaValues("doc-race")).containsOnly("newer");
    }

    @Test
    void allPolicyDoesNotAcknowledgeReplicaOutageAndRetryCanFinishTheSameOperation() {
        ReplicatedFixture fixture = replicatedFixture();
        var plan = fixture.manager().replicaWritePlan("tenant-a", "doc-replicated");
        var follower = plan.targets().stream()
                .filter(target -> !target.primary())
                .findFirst()
                .orElseThrow();
        follower.client().setActive(false);
        IndexRequestDto request = indexRequest("doc-replicated", Map.of("title", "replicated"));
        request.setPartitionId("tenant-a");
        request.setOperationId("operation-7");
        request.setGeneration(7L);

        assertThatThrownBy(() -> fixture.service().index(request))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("requires 2");

        follower.client().setActive(true);
        IndexResponseDto retried = fixture.service().index(request);
        assertThat(retried.isSuccess()).isTrue();
        assertThat(retried.getAcknowledgements()).isEqualTo(2);
    }

    @Test
    void primaryCrashRejectsWritesButReadPlanFailsOverWithoutDoubleCountingShard() {
        ReplicatedFixture fixture = replicatedFixture();
        var plan = fixture.manager().replicaWritePlan("tenant-a", "doc-replicated");
        plan.targets().getFirst().client().setActive(false);
        IndexRequestDto request = indexRequest("doc-replicated", Map.of("title", "x"));
        request.setPartitionId("tenant-a");

        assertThatThrownBy(() -> fixture.service().index(request))
                .isInstanceOf(NodeUnavailableException.class)
                .hasMessageContaining("writes are never promoted");

        var readTargets = fixture.manager().replicaReadPlan("tenant-a").targets();
        assertThat(readTargets)
                .extracting(ReplicaPlacement.ReadTarget::logicalShardId)
                .doesNotHaveDuplicates();
        assertThat(readTargets).anyMatch(ReplicaPlacement.ReadTarget::failover);
    }

    @Test
    void getUsesTheEligibleReplicaForTheExactOwnerAndNeverConvertsUnavailableToAbsent() {
        ReplicatedFixture fixture = replicatedFixture();
        String partitionId = "tenant-a";
        String id = "doc:[* TO *]";
        String owner = fixture.manager().ownerNodeId(partitionId, id);
        var selected = fixture.manager().replicaReadPlan(partitionId).targets().stream()
                .filter(target -> target.logicalShardId().equals(ReplicaPlacement.logicalShardId(owner)))
                .findFirst()
                .orElseThrow();
        when(fixture.stubs().get(selected.nodeId()).getDocument(any(GetDocumentRequest.class)))
                .thenReturn(GetDocumentResponse.newBuilder()
                        .setDocument(Document.newBuilder()
                                .setId(id)
                                .addFields(Field.newBuilder().setName("title").setValue("exact")))
                        .build());

        GetDocumentResponseDto response = fixture.service().get(id, partitionId);
        assertThat(response.partitionId()).isEqualTo(partitionId);
        assertThat(response.id()).isEqualTo(id);
        assertThat(response.fields()).containsEntry("title", "exact");
        ArgumentCaptor<GetDocumentRequest> request = ArgumentCaptor.forClass(GetDocumentRequest.class);
        verify(fixture.stubs().get(selected.nodeId())).getDocument(request.capture());
        assertThat(request.getValue().getPartitionId()).isEqualTo(selected.storagePartitionId());

        fixture.manager().getClientMap().values().forEach(client -> client.setActive(false));
        assertThatThrownBy(() -> fixture.service().get(id, partitionId))
                .isInstanceOf(NodeUnavailableException.class)
                .hasMessageContaining("exact lookup is unavailable");
    }

    private static ReplicatedFixture replicatedFixture() {
        return replicatedFixture(2);
    }

    private static ReplicatedFixture replicatedFixture(int replicationFactor) {
        return replicatedFixture(replicationFactor, ReplicaPlacement.DurabilityPolicy.ALL);
    }

    private static ReplicatedFixture replicatedFixture(
            int replicationFactor, ReplicaPlacement.DurabilityPolicy durabilityPolicy) {
        Map<String, IndexServiceGrpc.IndexServiceBlockingStub> stubs = new LinkedHashMap<>();
        Map<String, NodeClient<IndexServiceGrpc.IndexServiceBlockingStub>> clients = new LinkedHashMap<>();
        Map<String, AtomicLong> primaryGenerations = new ConcurrentHashMap<>();
        Map<String, Map<String, ReplicaMutationState>> states = new LinkedHashMap<>();
        List<IndexDocumentRequest> requests = Collections.synchronizedList(new ArrayList<>());
        List<DeleteDocumentRequest> deleteRequests = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean failNextFollowerDelete = new AtomicBoolean();
        List<String> nodeIds = List.of("n0", "n1", "n2").subList(0, Math.max(2, replicationFactor));
        for (String nodeId : nodeIds) {
            IndexServiceGrpc.IndexServiceBlockingStub stub = mock(IndexServiceGrpc.IndexServiceBlockingStub.class);
            states.put(nodeId, new LinkedHashMap<>());
            lenient()
                    .when(stub.withDeadlineAfter(anyLong(), any(TimeUnit.class)))
                    .thenReturn(stub);
            lenient().when(stub.indexDocument(any(IndexDocumentRequest.class))).thenAnswer(invocation -> {
                IndexDocumentRequest request = invocation.getArgument(0);
                requests.add(request);
                MutationOutcome outcome = applyFixtureMutation(
                        states.get(nodeId),
                        primaryGenerations,
                        request.getDocument().getId(),
                        request.getMutation().getOperationId(),
                        request.getMutation().getOperationGeneration(),
                        !request.getMutation().getReplica(),
                        "index",
                        request.getDocument().getFieldsList().stream()
                                .filter(field -> "title".equals(field.getName()))
                                .map(Field::getValue)
                                .findFirst()
                                .orElse("indexed"));
                return IndexDocumentResponse.newBuilder()
                        .setId(request.getDocument().getId())
                        .setSuccess(true)
                        .setDuplicate(outcome.duplicate())
                        .setCommittedGeneration(outcome.generation())
                        .build();
            });
            lenient()
                    .when(stub.deleteDocument(any(DeleteDocumentRequest.class)))
                    .thenAnswer(invocation -> {
                        DeleteDocumentRequest request = invocation.getArgument(0);
                        deleteRequests.add(request);
                        if (request.getMutation().getReplica() && failNextFollowerDelete.compareAndSet(true, false)) {
                            throw Status.UNAVAILABLE
                                    .withDescription("simulated follower delete failure")
                                    .asRuntimeException();
                        }
                        MutationOutcome outcome = applyFixtureMutation(
                                states.get(nodeId),
                                primaryGenerations,
                                request.getId(),
                                request.getMutation().getOperationId(),
                                request.getMutation().getOperationGeneration(),
                                !request.getMutation().getReplica(),
                                "delete",
                                null);
                        return DeleteDocumentResponse.newBuilder()
                                .setSuccess(true)
                                .setDuplicate(outcome.duplicate())
                                .setCommittedGeneration(outcome.generation())
                                .build();
                    });
            stubs.put(nodeId, stub);
            clients.put(nodeId, new NodeClient<>(nodeId, stub, mock(ManagedChannel.class), "localhost", 5100));
        }
        NodeClientManager<IndexServiceGrpc.IndexServiceBlockingStub> manager = new NodeClientManager<>(
                clients,
                RoutingStrategy.ROUND_ROBIN,
                NodeRole.NODE_ROLE_INDEX,
                IndexServiceGrpc::newBlockingStub,
                replicationFactor,
                durabilityPolicy,
                durabilityPolicy == ReplicaPlacement.DurabilityPolicy.ALL
                        ? ReplicaPlacement.ReadConsistency.ACKNOWLEDGED
                        : ReplicaPlacement.ReadConsistency.AVAILABLE);
        return new ReplicatedFixture(
                manager,
                new GatewayIndexService(manager),
                stubs,
                requests,
                deleteRequests,
                states,
                failNextFollowerDelete);
    }

    private static void blockIndex(
            IndexServiceGrpc.IndexServiceBlockingStub stub,
            CountDownLatch started,
            CountDownLatch release,
            CountDownLatch interrupted) {
        when(stub.indexDocument(any(IndexDocumentRequest.class))).thenAnswer(invocation -> {
            IndexDocumentRequest request = invocation.getArgument(0);
            started.countDown();
            awaitRelease(release, interrupted);
            return IndexDocumentResponse.newBuilder()
                    .setId(request.getDocument().getId())
                    .setSuccess(true)
                    .setCommittedGeneration(request.getMutation().getOperationGeneration())
                    .build();
        });
    }

    private static void blockDelete(
            IndexServiceGrpc.IndexServiceBlockingStub stub,
            CountDownLatch started,
            CountDownLatch release,
            CountDownLatch interrupted) {
        when(stub.deleteDocument(any(DeleteDocumentRequest.class))).thenAnswer(invocation -> {
            DeleteDocumentRequest request = invocation.getArgument(0);
            started.countDown();
            awaitRelease(release, interrupted);
            return DeleteDocumentResponse.newBuilder()
                    .setSuccess(true)
                    .setCommittedGeneration(request.getMutation().getOperationGeneration())
                    .build();
        });
    }

    private static void failIndexAfterRelease(
            IndexServiceGrpc.IndexServiceBlockingStub stub, CountDownLatch started, CountDownLatch release) {
        when(stub.indexDocument(any(IndexDocumentRequest.class))).thenAnswer(invocation -> {
            started.countDown();
            awaitRelease(release, new CountDownLatch(0));
            throw Status.UNAVAILABLE
                    .withDescription("test follower failed after acknowledgement")
                    .asRuntimeException();
        });
    }

    private static void awaitRelease(CountDownLatch release, CountDownLatch interrupted) {
        try {
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw Status.DEADLINE_EXCEEDED
                        .withDescription("test follower remained blocked")
                        .asRuntimeException();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            interrupted.countDown();
            throw Status.CANCELLED
                    .withDescription("test follower interrupted")
                    .withCause(e)
                    .asRuntimeException();
        }
    }

    private static double metricValue(String mutation, String outcome) {
        Double value = CollectorRegistry.defaultRegistry.getSampleValue(
                "dsearch_replication_replica_attempts_total",
                new String[] {"mutation", "outcome"},
                new String[] {mutation, outcome});
        return value == null ? 0.0 : value;
    }

    private static void awaitMetric(String mutation, String outcome, double expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (metricValue(mutation, outcome) < expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(metricValue(mutation, outcome)).isGreaterThanOrEqualTo(expected);
    }

    private static void awaitReplicaLag(double expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (!Double.valueOf(expected)
                        .equals(CollectorRegistry.defaultRegistry.getSampleValue(
                                "dsearch_replication_unacknowledged_replicas"))
                && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(CollectorRegistry.defaultRegistry.getSampleValue("dsearch_replication_unacknowledged_replicas"))
                .isEqualTo(expected);
    }

    private record ReplicatedFixture(
            NodeClientManager<IndexServiceGrpc.IndexServiceBlockingStub> manager,
            GatewayIndexService service,
            Map<String, IndexServiceGrpc.IndexServiceBlockingStub> stubs,
            List<IndexDocumentRequest> requests,
            List<DeleteDocumentRequest> deleteRequests,
            Map<String, Map<String, ReplicaMutationState>> states,
            AtomicBoolean failNextFollowerDelete) {

        private List<String> replicaValues(String id) {
            return states.values().stream()
                    .map(state -> state.get(id))
                    .map(ReplicaMutationState::value)
                    .toList();
        }
    }

    private static MutationOutcome applyFixtureMutation(
            Map<String, ReplicaMutationState> state,
            Map<String, AtomicLong> primaryGenerations,
            String id,
            String operationId,
            long requestedGeneration,
            boolean primary,
            String type,
            String value) {
        ReplicaMutationState previous = state.get(id);
        long generation = requestedGeneration;
        if (primary && generation == 0) {
            if (previous != null
                    && previous.operationId().equals(operationId)
                    && previous.type().equals(type)) {
                return new MutationOutcome(true, previous.generation());
            }
            long floor = previous == null ? 0 : previous.generation();
            AtomicLong allocator = primaryGenerations.computeIfAbsent(id, ignored -> new AtomicLong(floor));
            allocator.accumulateAndGet(floor, Math::max);
            generation = allocator.incrementAndGet();
        }
        if (previous != null) {
            if (generation < previous.generation()) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("operation generation " + generation + " is older than committed generation "
                                + previous.generation())
                        .asRuntimeException();
            }
            if (generation == previous.generation()) {
                if (previous.operationId().equals(operationId)
                        && previous.type().equals(type)) {
                    return new MutationOutcome(true, generation);
                }
                throw Status.FAILED_PRECONDITION
                        .withDescription("operation generation was already committed with a different identity")
                        .asRuntimeException();
            }
        }
        state.put(id, new ReplicaMutationState(operationId, generation, type, value));
        return new MutationOutcome(false, generation);
    }

    private record ReplicaMutationState(String operationId, long generation, String type, String value) {}

    private record MutationOutcome(boolean duplicate, long generation) {}

    private static BulkDeleteRequestDto bulkDeleteRequest(String partitionId, String... ids) {
        BulkDeleteRequestDto request = new BulkDeleteRequestDto();
        request.setPartitionId(partitionId);
        request.setIds(List.of(ids));
        return request;
    }

    private static BulkDeleteRequestDto bulkDeleteItemRequest(String partitionId, BulkDeleteItemRequestDto... items) {
        BulkDeleteRequestDto request = new BulkDeleteRequestDto();
        request.setPartitionId(partitionId);
        request.setItems(List.of(items));
        return request;
    }

    private static BulkDeleteDocumentResponse successfulBulkDeleteResult(int requestIndex, String id) {
        return BulkDeleteDocumentResponse.newBuilder()
                .setSuccess(true)
                .addResults(BulkDeleteDocumentResult.newBuilder()
                        .setRequestIndex(requestIndex)
                        .setId(id)
                        .setSuccess(true))
                .build();
    }

    private static IndexRequestDto indexRequest(String id, Map<String, String> fields) {
        IndexRequestDto request = new IndexRequestDto();
        request.setId(id);
        request.setFields(fields);
        return request;
    }

    private static BulkIndexRequestDto bulkRequest(String partitionId, IndexRequestDto... items) {
        BulkIndexRequestDto request = new BulkIndexRequestDto();
        request.setPartitionId(partitionId);
        request.setItems(List.of(items));
        return request;
    }

    private static BulkIndexDocumentResponse successfulBulkResult(int requestIndex, String id) {
        return BulkIndexDocumentResponse.newBuilder()
                .setSuccess(true)
                .addResults(BulkIndexDocumentResult.newBuilder()
                        .setRequestIndex(requestIndex)
                        .setId(id)
                        .setSuccess(true))
                .build();
    }
}
