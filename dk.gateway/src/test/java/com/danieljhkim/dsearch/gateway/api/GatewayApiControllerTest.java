package com.danieljhkim.dsearch.gateway.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.danieljhkim.dsearch.common.cluster.NodeGroup;
import com.danieljhkim.dsearch.common.cluster.NodeGroupManager;
import com.danieljhkim.dsearch.common.config.AppConfig;
import com.danieljhkim.dsearch.gateway.api.dto.BulkDeleteItemResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.BulkDeleteRequestDto;
import com.danieljhkim.dsearch.gateway.api.dto.BulkDeleteResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.BulkIndexItemResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.BulkIndexRequestDto;
import com.danieljhkim.dsearch.gateway.api.dto.BulkIndexResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.GetDocumentResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.IndexRequestDto;
import com.danieljhkim.dsearch.gateway.api.dto.IndexResponseDto;
import com.danieljhkim.dsearch.gateway.api.dto.SearchRequestDto;
import com.danieljhkim.dsearch.gateway.api.dto.SearchResponseDto;
import com.danieljhkim.dsearch.gateway.config.MetricsConfig;
import com.danieljhkim.dsearch.gateway.service.GatewayIndexService;
import com.danieljhkim.dsearch.gateway.service.GatewaySearchService;
import com.danieljhkim.dsearch.gateway.tracing.CorrelationIdFilter;
import com.danieljhkim.dsearch.proto.cluster.NodeRole;
import com.danieljhkim.dsearch.proto.common.FilterOperator;
import com.danieljhkim.dsearch.proto.common.FusionStrategy;
import com.danieljhkim.dsearch.proto.common.SearchType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.client.RestTemplate;

@WebMvcTest(controllers = {IndexController.class, SearchController.class, HealthController.class})
@Import({GlobalExceptionHandler.class, CorrelationIdFilter.class, GatewayApiControllerTest.TestMetricsConfig.class})
class GatewayApiControllerTest {

    private static final String REQUEST_ID_HEADER = CorrelationIdFilter.HEADER_NAME;

    @jakarta.annotation.Resource
    private MockMvc mockMvc;

    @jakarta.annotation.Resource
    private MeterRegistry meterRegistry;

    @MockitoBean
    private GatewayIndexService indexService;

    @MockitoBean
    private GatewaySearchService searchService;

    @MockitoBean
    private RestTemplate restTemplate;

    @MockitoBean
    private NodeGroupManager nodeGroupManager;

    @Test
    void indexDocumentReturnsSuccessAndMapsRequestBody() throws Exception {
        when(indexService.index(any(IndexRequestDto.class))).thenReturn(new IndexResponseDto("doc-1", true));

        mockMvc.perform(post("/api/v1/index")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "id": "doc-1",
                                  "partitionId": "tenant-a",
                                  "fields": {
                                    "title": "Distributed Search",
                                    "category": "docs"
                                  }
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(header().exists(REQUEST_ID_HEADER))
                .andExpect(jsonPath("$.id").value("doc-1"))
                .andExpect(jsonPath("$.success").value(true));

        ArgumentCaptor<IndexRequestDto> requestCaptor = ArgumentCaptor.forClass(IndexRequestDto.class);
        verify(indexService).index(requestCaptor.capture());
        IndexRequestDto mappedRequest = requestCaptor.getValue();
        assertThat(mappedRequest.getId()).isEqualTo("doc-1");
        assertThat(mappedRequest.getPartitionId()).isEqualTo("tenant-a");
        assertThat(mappedRequest.getFields())
                .containsEntry("title", "Distributed Search")
                .containsEntry("category", "docs");
    }

    @Test
    void indexDocumentMapsNullFieldValueValidationToBadRequest() throws Exception {
        when(indexService.index(any(IndexRequestDto.class)))
                .thenThrow(new IllegalArgumentException("field value must not be null"));

        mockMvc.perform(post("/api/v1/index")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "id": "doc-1",
                                  "fields": {"title": null}
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("Bad Request"))
                .andExpect(jsonPath("$.message").value("field value must not be null"))
                .andExpect(jsonPath("$.path").value("/api/v1/index"));

        verify(indexService).index(any(IndexRequestDto.class));
    }

    @Test
    void bulkIndexPreservesOrderedItemOutcomesAtTheVersionedEndpoint() throws Exception {
        when(indexService.bulkIndex(any(BulkIndexRequestDto.class)))
                .thenReturn(new BulkIndexResponseDto(
                        false,
                        List.of(
                                new BulkIndexItemResponseDto(0, "doc-1", "success", null),
                                new BulkIndexItemResponseDto(1, "doc-2", "retryable_failure", "retry same id"))));

        mockMvc.perform(post("/api/v1/index/bulk")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "partitionId": "tenant-a",
                                  "items": [
                                    {"id": "doc-1", "fields": {"title": "first"}},
                                    {"id": "doc-2", "fields": {"title": "second"}}
                                  ]
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.items[0].requestIndex").value(0))
                .andExpect(jsonPath("$.items[0].id").value("doc-1"))
                .andExpect(jsonPath("$.items[0].status").value("success"))
                .andExpect(jsonPath("$.items[1].requestIndex").value(1))
                .andExpect(jsonPath("$.items[1].status").value("retryable_failure"));

        ArgumentCaptor<BulkIndexRequestDto> requestCaptor = ArgumentCaptor.forClass(BulkIndexRequestDto.class);
        verify(indexService).bulkIndex(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getPartitionId()).isEqualTo("tenant-a");
        assertThat(requestCaptor.getValue().getItems())
                .extracting(IndexRequestDto::getId)
                .containsExactly("doc-1", "doc-2");
    }

    @Test
    void bulkDeletePreservesOrderedItemOutcomesAtTheVersionedEndpoint() throws Exception {
        when(indexService.bulkDelete(any(BulkDeleteRequestDto.class)))
                .thenReturn(new BulkDeleteResponseDto(
                        false,
                        List.of(
                                new BulkDeleteItemResponseDto(0, "doc-1", "success", null),
                                new BulkDeleteItemResponseDto(1, "doc-2", "retryable_failure", "retry same id"),
                                new BulkDeleteItemResponseDto(2, "doc-1", "success", null))));

        mockMvc.perform(post("/api/v1/index/bulk-delete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "partitionId": "tenant-a",
                                  "ids": ["doc-1", "doc-2", "doc-1"]
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.items[0].requestIndex").value(0))
                .andExpect(jsonPath("$.items[0].id").value("doc-1"))
                .andExpect(jsonPath("$.items[0].status").value("success"))
                .andExpect(jsonPath("$.items[1].requestIndex").value(1))
                .andExpect(jsonPath("$.items[1].status").value("retryable_failure"))
                .andExpect(jsonPath("$.items[2].requestIndex").value(2))
                .andExpect(jsonPath("$.items[2].id").value("doc-1"))
                .andExpect(jsonPath("$.items[2].status").value("success"));

        ArgumentCaptor<BulkDeleteRequestDto> requestCaptor = ArgumentCaptor.forClass(BulkDeleteRequestDto.class);
        verify(indexService).bulkDelete(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getPartitionId()).isEqualTo("tenant-a");
        assertThat(requestCaptor.getValue().getIds()).containsExactly("doc-1", "doc-2", "doc-1");
    }

    @Test
    void bulkDeleteAcceptsAndReturnsStableOperationIdentity() throws Exception {
        when(indexService.bulkDelete(any(BulkDeleteRequestDto.class)))
                .thenReturn(new BulkDeleteResponseDto(
                        false,
                        List.of(new BulkDeleteItemResponseDto(
                                0, "doc-1", "retryable_failure", "follower unavailable", "delete-1", 4L))));

        mockMvc.perform(post("/api/v1/index/bulk-delete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "partitionId": "tenant-a",
                                  "items": [
                                    {"id": "doc-1", "operationId": "delete-1", "generation": 4}
                                  ]
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].operationId").value("delete-1"))
                .andExpect(jsonPath("$.items[0].generation").value(4));

        ArgumentCaptor<BulkDeleteRequestDto> requestCaptor = ArgumentCaptor.forClass(BulkDeleteRequestDto.class);
        verify(indexService).bulkDelete(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getItems().getFirst().getId()).isEqualTo("doc-1");
        assertThat(requestCaptor.getValue().getItems().getFirst().getOperationId())
                .isEqualTo("delete-1");
        assertThat(requestCaptor.getValue().getItems().getFirst().getGeneration())
                .isEqualTo(4L);
    }

    @Test
    void deleteDocumentReturnsSuccessAndMapsPathAndPartition() throws Exception {
        when(indexService.delete("doc-7", "tenant-a", "delete-7", 7L))
                .thenReturn(new IndexResponseDto("doc-7", true, "delete-7", 7L, 2, 2));

        mockMvc.perform(delete("/api/v1/index/{id}", "doc-7")
                        .param("partitionId", "tenant-a")
                        .param("operationId", "delete-7")
                        .param("generation", "7")
                        .header(REQUEST_ID_HEADER, "client-request-7"))
                .andExpect(status().isOk())
                .andExpect(header().string(REQUEST_ID_HEADER, "client-request-7"))
                .andExpect(jsonPath("$.id").value("doc-7"))
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.operationId").value("delete-7"))
                .andExpect(jsonPath("$.generation").value(7));

        verify(indexService).delete("doc-7", "tenant-a", "delete-7", 7L);
    }

    @Test
    void getDocumentReturnsStoredFieldsAndMapsConfirmedAbsenceAndUnavailability() throws Exception {
        when(indexService.get("doc:[* TO *]", "tenant-a"))
                .thenReturn(new GetDocumentResponseDto(
                        "tenant-a", "doc:[* TO *]", Map.of("title", "Exact", "category", "guide")));

        mockMvc.perform(get("/api/v1/index/{id}", "doc:[* TO *]").param("partitionId", "tenant-a"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.partitionId").value("tenant-a"))
                .andExpect(jsonPath("$.id").value("doc:[* TO *]"))
                .andExpect(jsonPath("$.fields.title").value("Exact"));

        when(indexService.get("missing", "tenant-a"))
                .thenThrow(new StatusRuntimeException(Status.NOT_FOUND.withDescription("confirmed absence")));
        mockMvc.perform(get("/api/v1/index/{id}", "missing").param("partitionId", "tenant-a"))
                .andExpect(status().isNotFound());

        when(indexService.get("unavailable", "tenant-a"))
                .thenThrow(new com.danieljhkim.dsearch.common.exception.NodeUnavailableException(
                        "n0", "no eligible replica"));
        mockMvc.perform(get("/api/v1/index/{id}", "unavailable").param("partitionId", "tenant-a"))
                .andExpect(status().isServiceUnavailable());
    }

    @Test
    void searchReturnsHitsFacetsAndMapsSearchOptions() throws Exception {
        SearchResponseDto response = new SearchResponseDto(
                List.of(new SearchResponseDto.SearchHitDto(
                        "doc-2",
                        "Lucene Guide",
                        "Lucene search guide",
                        12.5,
                        Map.of("content", "<em>Lucene</em> search guide"),
                        Map.of("category", "docs"))),
                1,
                8,
                2);
        response.setPageSize(3);
        response.setFacets(List.of(new com.danieljhkim.dsearch.gateway.api.dto.FacetResponseDto(
                "category", List.of(new com.danieljhkim.dsearch.gateway.api.dto.FacetBucketDto("docs", 4L)))));
        response.setFanout(new SearchResponseDto.FanoutMetadataDto("PARTIAL_FAILURE", 2, 1, 1, 0));
        when(searchService.search(any(SearchRequestDto.class))).thenReturn(response);

        mockMvc.perform(post("/api/v1/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "query": "lucene vector",
                                  "partitionId": "tenant-a",
                                  "page": 2,
                                  "pageSize": 3,
                                  "searchType": "HYBRID",
                                  "fusionStrategy": "WEIGHTED",
                                  "highlight": false,
                                  "filters": [
                                    {
                                      "field": "category",
                                      "operator": "IN",
                                      "values": ["docs", "guides"]
                                    }
                                  ],
                                  "facets": [
                                    {
                                      "field": "author",
                                      "size": 5,
                                      "filters": [
                                        {
                                          "field": "year",
                                          "operator": "GTE",
                                          "values": ["2024"]
                                        }
                                      ],
                                      "nested": [
                                        {
                                          "field": "tag",
                                          "size": 3
                                        }
                                      ]
                                    }
                                  ]
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(header().exists(REQUEST_ID_HEADER))
                .andExpect(jsonPath("$.totalHits").value(1))
                .andExpect(jsonPath("$.tookMillis").value(8))
                .andExpect(jsonPath("$.page").value(2))
                .andExpect(jsonPath("$.pageSize").value(3))
                .andExpect(jsonPath("$.hits[0].docId").value("doc-2"))
                .andExpect(jsonPath("$.hits[0].highlightedFields.content").value("<em>Lucene</em> search guide"))
                .andExpect(jsonPath("$.hits[0].fields.category").value("docs"))
                .andExpect(jsonPath("$.facets[0].field").value("category"))
                .andExpect(jsonPath("$.facets[0].buckets[0].value").value("docs"))
                .andExpect(jsonPath("$.facets[0].buckets[0].count").value(4))
                .andExpect(jsonPath("$.fanout.status").value("PARTIAL_FAILURE"))
                .andExpect(jsonPath("$.fanout.attemptedNodes").value(2))
                .andExpect(jsonPath("$.fanout.succeededNodes").value(1))
                .andExpect(jsonPath("$.fanout.failedNodes").value(1))
                .andExpect(jsonPath("$.fanout.timedOutNodes").value(0));

        ArgumentCaptor<SearchRequestDto> requestCaptor = ArgumentCaptor.forClass(SearchRequestDto.class);
        verify(searchService).search(requestCaptor.capture());
        SearchRequestDto mappedRequest = requestCaptor.getValue();
        assertThat(mappedRequest.getQuery()).isEqualTo("lucene vector");
        assertThat(mappedRequest.getPartitionId()).isEqualTo("tenant-a");
        assertThat(mappedRequest.getPage()).isEqualTo(2);
        assertThat(mappedRequest.getPageSize()).isEqualTo(3);
        assertThat(mappedRequest.getSearchType()).isEqualTo(SearchType.HYBRID);
        assertThat(mappedRequest.getFusionStrategy()).isEqualTo(FusionStrategy.WEIGHTED);
        assertThat(mappedRequest.getHighlight()).isFalse();
        assertThat(mappedRequest.getFilters()).hasSize(1);
        assertThat(mappedRequest.getFilters().getFirst().getField()).isEqualTo("category");
        assertThat(mappedRequest.getFilters().getFirst().getOperator()).isEqualTo(FilterOperator.IN);
        assertThat(mappedRequest.getFilters().getFirst().getValues()).containsExactly("docs", "guides");
        assertThat(mappedRequest.getFacets()).hasSize(1);
        assertThat(mappedRequest.getFacets().getFirst().getField()).isEqualTo("author");
        assertThat(mappedRequest.getFacets().getFirst().getSize()).isEqualTo(5);
        assertThat(mappedRequest.getFacets().getFirst().getFilters().getFirst().getField())
                .isEqualTo("year");
        assertThat(mappedRequest.getFacets().getFirst().getNested().getFirst().getField())
                .isEqualTo("tag");
    }

    @Test
    void healthReturnsGatewayStatus() throws Exception {
        mockMvc.perform(get("/health"))
                .andExpect(status().isOk())
                .andExpect(header().exists(REQUEST_ID_HEADER))
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.service").value("gateway"))
                .andExpect(jsonPath("$.timestamp").isString());
    }

    @Test
    void clusterHealthReturnsAggregatedSuccessStatus() throws Exception {
        when(nodeGroupManager.getNodeGroup(NodeRole.NODE_ROLE_INDEX))
                .thenReturn(nodeGroup(NodeRole.NODE_ROLE_INDEX, "index-0", "127.0.0.1", 5000, 5100));
        when(nodeGroupManager.getNodeGroup(NodeRole.NODE_ROLE_QUERY))
                .thenReturn(nodeGroup(NodeRole.NODE_ROLE_QUERY, "query-0", "127.0.0.1", 6000, 6100));
        when(nodeGroupManager.getNodeGroup(NodeRole.NODE_ROLE_COORDINATOR))
                .thenReturn(nodeGroup(NodeRole.NODE_ROLE_COORDINATOR, "coordinator-0", "127.0.0.1", 7000, 7100));
        when(restTemplate.getForEntity("http://127.0.0.1:5100/readyz", String.class))
                .thenReturn(ResponseEntity.ok("{\"status\":\"UP\"}"));
        when(restTemplate.getForEntity("http://127.0.0.1:6100/readyz", String.class))
                .thenReturn(ResponseEntity.ok("{\"status\":\"UP\"}"));
        when(restTemplate.getForEntity("http://127.0.0.1:7100/readyz", String.class))
                .thenReturn(ResponseEntity.ok("{\"status\":\"UP\"}"));

        mockMvc.perform(get("/cluster/health").header(REQUEST_ID_HEADER, "health-request-1"))
                .andExpect(status().isOk())
                .andExpect(header().string(REQUEST_ID_HEADER, "health-request-1"))
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.gateway.status").value("UP"))
                .andExpect(jsonPath("$.indexNodes[0].id").value("index-0"))
                .andExpect(jsonPath("$.indexNodes[0].grpcPort").value(5000))
                .andExpect(jsonPath("$.indexNodes[0].healthPort").value(5100))
                .andExpect(jsonPath("$.indexNodes[0].status").value("UP"))
                .andExpect(jsonPath("$.queryNodes[0].id").value("query-0"))
                .andExpect(jsonPath("$.queryNodes[0].status").value("UP"))
                .andExpect(jsonPath("$.coordinatorNodes[0].id").value("coordinator-0"))
                .andExpect(jsonPath("$.coordinatorNodes[0].status").value("UP"))
                .andExpect(jsonPath("$.timestamp").isString());
    }

    @Test
    void clusterHealthReturnsDegradedWhenCoordinatorIsDown() throws Exception {
        when(nodeGroupManager.getNodeGroup(NodeRole.NODE_ROLE_INDEX))
                .thenReturn(nodeGroup(NodeRole.NODE_ROLE_INDEX, "index-0", "127.0.0.1", 5000, 5100));
        when(nodeGroupManager.getNodeGroup(NodeRole.NODE_ROLE_QUERY))
                .thenReturn(nodeGroup(NodeRole.NODE_ROLE_QUERY, "query-0", "127.0.0.1", 6000, 6100));
        when(nodeGroupManager.getNodeGroup(NodeRole.NODE_ROLE_COORDINATOR))
                .thenReturn(nodeGroup(NodeRole.NODE_ROLE_COORDINATOR, "coordinator-0", "127.0.0.1", 7000, 7100));
        when(restTemplate.getForEntity("http://127.0.0.1:5100/readyz", String.class))
                .thenReturn(ResponseEntity.ok("{\"status\":\"UP\"}"));
        when(restTemplate.getForEntity("http://127.0.0.1:6100/readyz", String.class))
                .thenReturn(ResponseEntity.ok("{\"status\":\"UP\"}"));
        when(restTemplate.getForEntity("http://127.0.0.1:7100/readyz", String.class))
                .thenThrow(new RuntimeException("coordinator unavailable"));

        mockMvc.perform(get("/cluster/health"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value("DEGRADED"))
                .andExpect(jsonPath("$.coordinatorNodes[0].status").value("DOWN"));
    }

    @Test
    void clusterHealthOnlyProbesTheAuthoritativeNodeSet() throws Exception {
        when(nodeGroupManager.getNodeGroup(NodeRole.NODE_ROLE_INDEX))
                .thenReturn(nodeGroup(NodeRole.NODE_ROLE_INDEX, "index-0", "127.0.0.1", 5000, 5100));
        when(nodeGroupManager.getNodeGroup(NodeRole.NODE_ROLE_QUERY))
                .thenReturn(nodeGroup(NodeRole.NODE_ROLE_QUERY, "query-0", "127.0.0.1", 6000, 6100));
        when(nodeGroupManager.getNodeGroup(NodeRole.NODE_ROLE_COORDINATOR))
                .thenReturn(nodeGroup(NodeRole.NODE_ROLE_COORDINATOR, "coordinator-0", "127.0.0.1", 7000, 7100));
        when(restTemplate.getForEntity(any(String.class), org.mockito.ArgumentMatchers.eq(String.class)))
                .thenReturn(ResponseEntity.ok("{\"status\":\"UP\"}"));

        mockMvc.perform(get("/cluster/health")).andExpect(status().isOk());

        verify(restTemplate).getForEntity("http://127.0.0.1:5100/readyz", String.class);
        verify(restTemplate).getForEntity("http://127.0.0.1:6100/readyz", String.class);
        verify(restTemplate).getForEntity("http://127.0.0.1:7100/readyz", String.class);
        verify(restTemplate, never()).getForEntity("http://127.0.0.1:5101/readyz", String.class);
        verify(restTemplate, never()).getForEntity("http://127.0.0.1:6101/readyz", String.class);
    }

    @Test
    void gatewayLivenessRemainsAvailableWhenAuthoritativeTopologyIsLost() throws Exception {
        when(nodeGroupManager.getNodeGroup(NodeRole.NODE_ROLE_INDEX))
                .thenThrow(new IllegalStateException("coordinator unavailable"));

        mockMvc.perform(get("/readyz"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value("DOWN"))
                .andExpect(jsonPath("$.reason").value("authoritative_topology_unavailable:IllegalStateException"));
        mockMvc.perform(get("/livez"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    void gatewayReadinessRejectsAnEmptyAuthoritativeIndexTopology() throws Exception {
        when(nodeGroupManager.getNodeGroup(NodeRole.NODE_ROLE_INDEX))
                .thenReturn(emptyNodeGroup(NodeRole.NODE_ROLE_INDEX));
        when(nodeGroupManager.getNodeGroup(NodeRole.NODE_ROLE_QUERY))
                .thenReturn(nodeGroup(NodeRole.NODE_ROLE_QUERY, "query-0", "127.0.0.1", 6000, 6100));
        when(nodeGroupManager.getNodeGroup(NodeRole.NODE_ROLE_COORDINATOR))
                .thenReturn(nodeGroup(NodeRole.NODE_ROLE_COORDINATOR, "coordinator-0", "127.0.0.1", 7000, 7100));

        mockMvc.perform(get("/readyz"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.reason").value("authoritative_index_topology_empty"));
    }

    @Test
    void searchValidationFailureReturnsExactErrorShape() throws Exception {
        mockMvc.perform(post("/api/v1/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "query": "",
                                  "page": 0,
                                  "pageSize": 10
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(header().exists(REQUEST_ID_HEADER))
                .andExpect(jsonPath("$.timestamp").isString())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("Bad Request"))
                .andExpect(jsonPath("$.message").value("query query must not be blank"))
                .andExpect(jsonPath("$.path").value("/api/v1/search"));
    }

    @Test
    void searchPageSizeValidationFailureReturnsExactErrorShape() throws Exception {
        mockMvc.perform(post("/api/v1/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "query": "lucene",
                                  "page": 0,
                                  "pageSize": 0
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.timestamp").isString())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("Bad Request"))
                .andExpect(jsonPath("$.message").value("pageSize pageSize must be > 0"))
                .andExpect(jsonPath("$.path").value("/api/v1/search"));
    }

    @Test
    void indexPartitionIdValidationRejectsInvalidValuesBeforeServiceCall() throws Exception {
        mockMvc.perform(post("/api/v1/index")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "partitionId": "tenant!",
                                  "fields": {"title": "Distributed Search"}
                                }
                                """))
                .andExpect(status().isBadRequest());

        verify(indexService, never()).index(any(IndexRequestDto.class));
    }

    @Test
    void searchLatencyMetersStayBoundedForDistinctPartitionIds() throws Exception {
        when(searchService.search(any(SearchRequestDto.class))).thenReturn(new SearchResponseDto(List.of(), 0, 0, 0));

        for (int i = 0; i < 250; i++) {
            mockMvc.perform(post("/api/v1/search")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"query\":\"lucene\",\"partitionId\":\"tenant-" + i + "\"}"))
                    .andExpect(status().isOk());
        }

        assertThat(meterRegistry.find("dsearch.gateway.search.latency").timers())
                .hasSizeLessThanOrEqualTo(100);
        assertThat(meterRegistry
                        .find("dsearch.gateway.search.latency")
                        .tag("partitionId", "tenant-0")
                        .timer())
                .isNotNull();
        assertThat(meterRegistry
                        .find("dsearch.gateway.search.latency")
                        .tag("partitionId", "__overflow__")
                        .timer())
                .isNotNull();
    }

    @Test
    void searchUnavailableFanoutFailureReturnsServiceUnavailable() throws Exception {
        when(searchService.search(any(SearchRequestDto.class)))
                .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE.withDescription(
                        "Search fanout failed: attemptedNodes=2 succeededNodes=0 failedNodes=2 timedOutNodes=0")));

        mockMvc.perform(post("/api/v1/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "query": "lucene",
                                  "partitionId": "tenant-a",
                                  "page": 0,
                                  "pageSize": 10,
                                  "searchType": "HYBRID",
                                  "fusionStrategy": "WEIGHTED"
                                }
                                """))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503))
                .andExpect(jsonPath("$.error").value("Service Unavailable"))
                .andExpect(jsonPath("$.message")
                        .value("Search fanout failed: attemptedNodes=2 succeededNodes=0 failedNodes=2 timedOutNodes=0"))
                .andExpect(jsonPath("$.path").value("/api/v1/search"));
    }

    @Test
    void searchDeadlineExceededFanoutFailureReturnsGatewayTimeout() throws Exception {
        when(searchService.search(any(SearchRequestDto.class)))
                .thenThrow(new StatusRuntimeException(Status.DEADLINE_EXCEEDED.withDescription(
                        "Search fanout failed: attemptedNodes=2 succeededNodes=0 failedNodes=0 timedOutNodes=2")));

        mockMvc.perform(post("/api/v1/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "query": "lucene",
                                  "partitionId": "tenant-a",
                                  "page": 0,
                                  "pageSize": 10,
                                  "searchType": "HYBRID",
                                  "fusionStrategy": "WEIGHTED"
                                }
                                """))
                .andExpect(status().isGatewayTimeout())
                .andExpect(jsonPath("$.status").value(504))
                .andExpect(jsonPath("$.error").value("Gateway Timeout"))
                .andExpect(jsonPath("$.message")
                        .value("Search fanout failed: attemptedNodes=2 succeededNodes=0 failedNodes=0 timedOutNodes=2"))
                .andExpect(jsonPath("$.path").value("/api/v1/search"));
    }

    @Test
    void downstreamGrpcExceptionReturnsExactErrorShape() throws Exception {
        when(indexService.index(any(IndexRequestDto.class)))
                .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE.withDescription("index node unavailable")));

        mockMvc.perform(post("/api/v1/index")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "id": "doc-1",
                                  "partitionId": "tenant-a",
                                  "fields": {
                                    "title": "Distributed Search"
                                  }
                                }
                                """))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.timestamp").isString())
                .andExpect(jsonPath("$.status").value(503))
                .andExpect(jsonPath("$.error").value("Service Unavailable"))
                .andExpect(jsonPath("$.message").value("index node unavailable"))
                .andExpect(jsonPath("$.path").value("/api/v1/index"));
    }

    @Test
    void generatedCorrelationIdWhenAbsent() throws Exception {
        MvcResult result = mockMvc.perform(get("/health"))
                .andExpect(status().isOk())
                .andExpect(header().exists(REQUEST_ID_HEADER))
                .andReturn();

        String requestId = result.getResponse().getHeader(REQUEST_ID_HEADER);
        assertThat(requestId).isNotBlank();
        assertThat(UUID.fromString(requestId).toString()).isEqualTo(requestId);
    }

    @Test
    void preservesSuppliedCorrelationId() throws Exception {
        mockMvc.perform(get("/health").header(REQUEST_ID_HEADER, "client-request-123"))
                .andExpect(status().isOk())
                .andExpect(header().string(REQUEST_ID_HEADER, "client-request-123"));
    }

    private static NodeGroup nodeGroup(NodeRole role, String id, String host, int grpcPort, int healthPort) {
        AppConfig.NodeConfig node = new AppConfig.NodeConfig();
        node.setId(id);
        node.setHost(host);
        node.setPort(grpcPort);
        node.setHealthPort(healthPort);

        com.danieljhkim.dsearch.common.config.AppConfig.NodeGroupConfig group =
                new com.danieljhkim.dsearch.common.config.AppConfig.NodeGroupConfig();
        group.setNodes(List.of(node));
        group.setRoutingStrategy(com.danieljhkim.dsearch.common.enums.RoutingStrategy.ROUND_ROBIN);
        return NodeGroup.fromConfig(role, group);
    }

    private static NodeGroup emptyNodeGroup(NodeRole role) {
        AppConfig.NodeGroupConfig group = new AppConfig.NodeGroupConfig();
        group.setNodes(List.of());
        group.setRoutingStrategy(com.danieljhkim.dsearch.common.enums.RoutingStrategy.ROUND_ROBIN);
        return NodeGroup.fromConfig(role, group);
    }

    @TestConfiguration
    static class TestMetricsConfig {
        @Bean
        MeterRegistry meterRegistry() {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            new MetricsConfig().metricsCardinality().customize(registry);
            return registry;
        }
    }
}
