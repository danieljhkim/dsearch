package com.danieljhkim.dsearch.proto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.danieljhkim.dsearch.proto.cluster.GetClusterInfoResponse;
import com.danieljhkim.dsearch.proto.cluster.NodeInfo;
import com.danieljhkim.dsearch.proto.common.FacetBucket;
import com.danieljhkim.dsearch.proto.common.FacetResponse;
import com.danieljhkim.dsearch.proto.common.Filter;
import com.danieljhkim.dsearch.proto.common.FilterOperator;
import com.danieljhkim.dsearch.proto.common.SearchType;
import com.danieljhkim.dsearch.proto.index.BulkIndexDocumentResult;
import com.danieljhkim.dsearch.proto.index.Document;
import com.danieljhkim.dsearch.proto.index.Field;
import com.danieljhkim.dsearch.proto.index.GetDocumentRequest;
import com.danieljhkim.dsearch.proto.index.IndexDocumentRequest;
import com.danieljhkim.dsearch.proto.index.IndexSearchRequest;
import com.danieljhkim.dsearch.proto.query.FanoutMetadata;
import com.danieljhkim.dsearch.proto.query.FanoutStatus;
import com.danieljhkim.dsearch.proto.query.QueryRequest;
import com.danieljhkim.dsearch.proto.query.QueryResponse;
import com.danieljhkim.dsearch.proto.query.SearchHit;
import com.danieljhkim.dsearch.proto.query.StoredFieldSelection;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ProtoWireContractTest {
    private static final Path DESCRIPTOR_SET =
            Path.of("target", "generated-resources", "protobuf", "descriptor-sets", "dsearch.pb");
    private static final int UNKNOWN_FIELD_NUMBER = 19001;
    private static final ByteString UNKNOWN_VALUE = ByteString.copyFromUtf8("contract-test");

    @Test
    void storedFieldSelectionDistinguishesOmittedEmptyAndNamedRequests() {
        QueryRequest omitted = QueryRequest.newBuilder().build();
        QueryRequest empty = QueryRequest.newBuilder()
                .setStoredFieldSelection(StoredFieldSelection.getDefaultInstance())
                .build();
        QueryRequest named = QueryRequest.newBuilder()
                .setStoredFieldSelection(
                        StoredFieldSelection.newBuilder().addFields("title").addFields("category"))
                .build();

        assertFalse(omitted.hasStoredFieldSelection());
        assertTrue(empty.hasStoredFieldSelection());
        assertEquals(0, empty.getStoredFieldSelection().getFieldsCount());
        assertEquals(
                List.of("title", "category"), named.getStoredFieldSelection().getFieldsList());

        SearchHit projected =
                SearchHit.newBuilder().setDocId("doc-1").setScore(1.0).build();
        assertFalse(projected.hasTitle());
        assertFalse(projected.hasContent());
    }

    @Test
    void compiledDescriptorContainsTheExpectedPublicSurface() throws Exception {
        FileDescriptorSet descriptorSet = loadDescriptorSet();
        Map<String, FileDescriptorProto> files = filesByName(descriptorSet);

        assertEquals(
                Set.of(
                        "cluster.proto",
                        "common/cursor.proto",
                        "common/facet.proto",
                        "common/filter.proto",
                        "common/fusion_strategy.proto",
                        "common/search_type.proto",
                        "common/sort.proto",
                        "index.proto",
                        "query.proto"),
                files.keySet());

        assertEnumValues(
                files.get("cluster.proto"),
                "NodeRole",
                Map.of(
                        "NODE_ROLE_UNKNOWN", 0,
                        "NODE_ROLE_INDEX", 1,
                        "NODE_ROLE_QUERY", 2,
                        "NODE_ROLE_COORDINATOR", 3));
        assertEnumValues(
                files.get("common/filter.proto"),
                "FilterOperator",
                Map.of(
                        "FILTER_OP_UNSPECIFIED", 0,
                        "EQ", 1,
                        "NE", 2,
                        "GT", 3,
                        "GTE", 4,
                        "LT", 5,
                        "LTE", 6,
                        "IN", 7,
                        "NOT_IN", 8));
        assertEnumValues(
                files.get("common/search_type.proto"),
                "SearchType",
                Map.of("SEARCH_TYPE_UNSPECIFIED", 0, "BM25", 1, "SEMANTIC", 2, "HYBRID", 3));
        assertEnumValues(
                files.get("common/sort.proto"),
                "SortOrder",
                Map.of("SORT_ORDER_UNSPECIFIED", 0, "SORT_ORDER_ASC", 1, "SORT_ORDER_DESC", 2));

        assertRpc(
                files.get("cluster.proto"),
                "ClusterService",
                "RegisterNode",
                "dsearch.cluster.RegisterNodeRequest",
                "dsearch.cluster.RegisterNodeResponse");
        assertRpc(
                files.get("cluster.proto"),
                "ClusterService",
                "Heartbeat",
                "dsearch.cluster.HeartbeatRequest",
                "dsearch.cluster.HeartbeatResponse");
        assertRpc(
                files.get("cluster.proto"),
                "ClusterService",
                "DeregisterNode",
                "dsearch.cluster.DeregisterNodeRequest",
                "dsearch.cluster.DeregisterNodeResponse");
        assertRpc(
                files.get("cluster.proto"),
                "ClusterService",
                "GetShardMap",
                "dsearch.cluster.GetShardMapRequest",
                "dsearch.cluster.GetShardMapResponse");
        assertRpc(
                files.get("cluster.proto"),
                "ClusterService",
                "getClusterInfo",
                "dsearch.cluster.GetClusterInfoRequest",
                "dsearch.cluster.GetClusterInfoResponse");
        assertRpc(
                files.get("index.proto"),
                "IndexService",
                "IndexDocument",
                "dsearch.index.IndexDocumentRequest",
                "dsearch.index.IndexDocumentResponse");
        assertRpc(
                files.get("index.proto"),
                "IndexService",
                "BulkIndexDocument",
                "dsearch.index.BulkIndexDocumentRequest",
                "dsearch.index.BulkIndexDocumentResponse");
        assertRpc(
                files.get("index.proto"),
                "IndexService",
                "DeleteDocument",
                "dsearch.index.DeleteDocumentRequest",
                "dsearch.index.DeleteDocumentResponse");
        assertRpc(
                files.get("index.proto"),
                "IndexService",
                "GetDocument",
                "dsearch.index.GetDocumentRequest",
                "dsearch.index.GetDocumentResponse");
        assertRpc(
                files.get("index.proto"),
                "IndexService",
                "GetDocumentCount",
                "dsearch.index.GetDocumentCountRequest",
                "dsearch.index.GetDocumentCountResponse");
        assertRpc(
                files.get("index.proto"),
                "IndexService",
                "SearchIndex",
                "dsearch.index.IndexSearchRequest",
                "dsearch.index.IndexSearchResponse");
        assertRpc(
                files.get("query.proto"),
                "QueryService",
                "Search",
                "dsearch.query.QueryRequest",
                "dsearch.query.QueryResponse");

        DescriptorProto nodeInfo = message(files.get("cluster.proto"), "NodeInfo");
        assertEquals(
                List.of(1),
                nodeInfo.getReservedRangeList().stream()
                        .map(range -> range.getStart())
                        .toList());
        assertEquals(
                List.of(2),
                nodeInfo.getReservedRangeList().stream()
                        .map(range -> range.getEnd())
                        .toList());
        assertFalse(nodeInfo.getFieldList().stream().anyMatch(field -> field.getNumber() == 1));
    }

    @Test
    void representativeMessagesRoundTripAndPreserveUnknownFields() throws Exception {
        List<Message> messages = List.of(
                IndexDocumentRequest.newBuilder()
                        .setPartitionId("partition-a")
                        .setDocument(Document.newBuilder()
                                .setId("doc-1")
                                .addFields(Field.newBuilder().setName("title").setValue("hello"))
                                .build())
                        .build(),
                IndexSearchRequest.newBuilder()
                        .setPartitionId("partition-a")
                        .setQuery("distributed search")
                        .setFrom(2)
                        .setSize(10)
                        .setSearchType(SearchType.HYBRID)
                        .addFilters(Filter.newBuilder()
                                .setField("tenant")
                                .setOperator(FilterOperator.EQ)
                                .addValues("acme"))
                        .setHighlight(true)
                        .build(),
                GetDocumentRequest.newBuilder()
                        .setPartitionId("partition-a")
                        .setId("doc:[* TO *]")
                        .build(),
                QueryRequest.newBuilder()
                        .setQueryString("protobuf contracts")
                        .setPartitionId("partition-a")
                        .setTopK(5)
                        .setPage(1)
                        .setSize(5)
                        .setSearchType(SearchType.SEMANTIC)
                        .setHighlight(true)
                        .build(),
                QueryResponse.newBuilder()
                        .addHits(SearchHit.newBuilder()
                                .setDocId("doc-1")
                                .setScore(0.95)
                                .setTitle("A result")
                                .putFields("tenant", "acme"))
                        .setTotalHits(1)
                        .setPage(1)
                        .setSize(10)
                        .setFanout(FanoutMetadata.newBuilder()
                                .setAttemptedNodes(3)
                                .setSucceededNodes(2)
                                .setFailedNodes(1)
                                .setTimedOutNodes(0)
                                .setStatus(FanoutStatus.FANOUT_STATUS_PARTIAL_FAILURE))
                        .build(),
                FacetResponse.newBuilder()
                        .setField("category")
                        .addBuckets(FacetBucket.newBuilder().setValue("books").setCount(4))
                        .build(),
                Filter.newBuilder()
                        .setField("price")
                        .setOperator(FilterOperator.GTE)
                        .addValues("10")
                        .addValues("20")
                        .build(),
                GetClusterInfoResponse.newBuilder()
                        .addNodes(NodeInfo.newBuilder()
                                .setNodeId("index-0")
                                .setPort(5000)
                                .setHealthPort(5100))
                        .setRoutingStrategy("round_robin")
                        .setComponentLabel("index")
                        .setReplicationFactor(1)
                        .setContractVersion(1)
                        .setTopologyEpoch("epoch-1")
                        .setTopologyVersion(7)
                        .build(),
                BulkIndexDocumentResult.newBuilder()
                        .setRequestIndex(0)
                        .setId("doc-1")
                        .setSuccess(false)
                        .setError("commit status unknown")
                        .build());

        for (Message original : messages) {
            byte[] withUnknownField = appendUnknownField(original.toByteArray());
            Message parsed = original.getParserForType().parseFrom(withUnknownField);

            assertEquals(
                    original.getAllFields(),
                    parsed.getAllFields(),
                    original.getDescriptorForType().getFullName());
            UnknownFieldSet.Field unknown = parsed.getUnknownFields().getField(UNKNOWN_FIELD_NUMBER);
            assertNotNull(unknown, original.getDescriptorForType().getFullName());
            assertEquals(List.of(UNKNOWN_VALUE), unknown.getLengthDelimitedList());
            assertArrayEquals(withUnknownField, parsed.toByteArray());
        }
    }

    @Test
    void descriptorRemainsCompatibleWithCheckedBaseline() throws Exception {
        ContractBaseline baseline = ContractBaseline.load();
        baseline.assertCompatible(loadDescriptorSet());
    }

    @Test
    void compatibilityAllowsAdditiveFieldsAndEnumValues() throws Exception {
        FileDescriptorSet original = loadDescriptorSet();
        FileDescriptorProto index = filesByName(original).get("index.proto");
        DescriptorProto request = message(index, "IndexDocumentRequest");
        DescriptorProto.Builder requestWithAdditiveField = request.toBuilder();
        requestWithAdditiveField.addField(FieldDescriptorProto.newBuilder()
                .setName("trace_id")
                .setNumber(100)
                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));

        FileDescriptorProto.Builder indexWithAdditiveField = index.toBuilder();
        indexWithAdditiveField.setMessageType(
                index.getMessageTypeList().indexOf(request), requestWithAdditiveField.build());
        FileDescriptorSet additive = replaceFile(original, indexWithAdditiveField.build());

        ContractBaseline.load().assertCompatible(additive);
    }

    @Test
    void compatibilityRejectsFieldNumberReuse() throws Exception {
        FileDescriptorSet original = loadDescriptorSet();
        FileDescriptorProto index = filesByName(original).get("index.proto");
        DescriptorProto request = message(index, "IndexDocumentRequest");
        DescriptorProto.Builder changedRequest = request.toBuilder();
        changedRequest.setField(
                0,
                request.getField(0).toBuilder().setName("renamed_partition_id").build());
        FileDescriptorProto.Builder changedIndex = index.toBuilder();
        changedIndex.setMessageType(index.getMessageTypeList().indexOf(request), changedRequest.build());

        AssertionError error = assertThrows(
                AssertionError.class,
                () -> ContractBaseline.load().assertCompatible(replaceFile(original, changedIndex.build())));
        assertTrue(error.getMessage().contains("IndexDocumentRequest"));
        assertTrue(error.getMessage().contains("partition_id"));
    }

    @Test
    void compatibilityRejectsRemovalRpcSignatureChangesAndEnumRenumbering() throws Exception {
        FileDescriptorSet original = loadDescriptorSet();
        ContractBaseline baseline = ContractBaseline.load();

        FileDescriptorProto index = filesByName(original).get("index.proto");
        DescriptorProto request = message(index, "IndexDocumentRequest");
        DescriptorProto.Builder requestWithoutDocument = request.toBuilder().removeField(1);
        FileDescriptorProto.Builder indexWithoutDocument = index.toBuilder();
        indexWithoutDocument.setMessageType(
                index.getMessageTypeList().indexOf(request), requestWithoutDocument.build());
        AssertionError removal = assertThrows(
                AssertionError.class,
                () -> baseline.assertCompatible(replaceFile(original, indexWithoutDocument.build())));
        assertTrue(removal.getMessage().contains("IndexDocumentRequest"));

        FileDescriptorProto query = filesByName(original).get("query.proto");
        ServiceDescriptorProto queryService = service(query, "QueryService");
        MethodDescriptorProto search = method(queryService, "Search");
        MethodDescriptorProto changedSearch =
                search.toBuilder().setOutputType(".dsearch.query.QueryRequest").build();
        ServiceDescriptorProto.Builder changedQueryService = queryService.toBuilder();
        changedQueryService.setMethod(queryService.getMethodList().indexOf(search), changedSearch);
        FileDescriptorProto.Builder changedQuery = query.toBuilder();
        changedQuery.setService(query.getServiceList().indexOf(queryService), changedQueryService.build());
        AssertionError signature = assertThrows(
                AssertionError.class, () -> baseline.assertCompatible(replaceFile(original, changedQuery.build())));
        assertTrue(signature.getMessage().contains("QueryService"));

        FileDescriptorProto searchTypeFile = filesByName(original).get("common/search_type.proto");
        EnumDescriptorProto searchType = enumDescriptor(searchTypeFile, "SearchType");
        EnumValueDescriptorProto bm25 = searchType.getValueList().get(1);
        EnumDescriptorProto.Builder changedSearchType = searchType.toBuilder();
        changedSearchType.setValue(1, bm25.toBuilder().setNumber(9).build());
        FileDescriptorProto.Builder changedSearchTypeFile = searchTypeFile.toBuilder();
        changedSearchTypeFile.setEnumType(
                searchTypeFile.getEnumTypeList().indexOf(searchType), changedSearchType.build());
        AssertionError enumRenumbering = assertThrows(
                AssertionError.class,
                () -> baseline.assertCompatible(replaceFile(original, changedSearchTypeFile.build())));
        assertTrue(enumRenumbering.getMessage().contains("SearchType"));
    }

    private static FileDescriptorSet loadDescriptorSet() throws IOException {
        assertTrue(Files.exists(DESCRIPTOR_SET), "Missing compiled descriptor set: " + DESCRIPTOR_SET);
        return FileDescriptorSet.parseFrom(Files.readAllBytes(DESCRIPTOR_SET));
    }

    private static FileDescriptorSet replaceFile(FileDescriptorSet descriptorSet, FileDescriptorProto replacement) {
        FileDescriptorSet.Builder builder = descriptorSet.toBuilder();
        int index = descriptorSet.getFileList().stream()
                .map(FileDescriptorProto::getName)
                .toList()
                .indexOf(replacement.getName());
        builder.setFile(index, replacement);
        return builder.build();
    }

    private static Map<String, FileDescriptorProto> filesByName(FileDescriptorSet descriptorSet) {
        return descriptorSet.getFileList().stream()
                .collect(Collectors.toMap(FileDescriptorProto::getName, Function.identity()));
    }

    private static DescriptorProto message(FileDescriptorProto file, String name) {
        return file.getMessageTypeList().stream()
                .filter(message -> message.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing message " + file.getName() + ":" + name));
    }

    private static EnumDescriptorProto enumDescriptor(FileDescriptorProto file, String name) {
        return file.getEnumTypeList().stream()
                .filter(enumDescriptor -> enumDescriptor.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing enum " + file.getName() + ":" + name));
    }

    private static ServiceDescriptorProto service(FileDescriptorProto file, String name) {
        return file.getServiceList().stream()
                .filter(service -> service.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing service " + file.getName() + ":" + name));
    }

    private static MethodDescriptorProto method(ServiceDescriptorProto service, String name) {
        return service.getMethodList().stream()
                .filter(method -> method.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing RPC " + service.getName() + ":" + name));
    }

    private static void assertEnumValues(FileDescriptorProto file, String name, Map<String, Integer> expectedValues) {
        EnumDescriptorProto descriptor = file.getEnumTypeList().stream()
                .filter(enumDescriptor -> enumDescriptor.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing enum " + file.getName() + ":" + name));
        Map<String, Integer> actualValues = descriptor.getValueList().stream()
                .collect(Collectors.toMap(EnumValueDescriptorProto::getName, EnumValueDescriptorProto::getNumber));
        assertEquals(expectedValues, actualValues);
    }

    private static void assertRpc(
            FileDescriptorProto file, String serviceName, String methodName, String inputType, String outputType) {
        ServiceDescriptorProto service = file.getServiceList().stream()
                .filter(candidate -> candidate.getName().equals(serviceName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing service " + file.getName() + ":" + serviceName));
        MethodDescriptorProto method = service.getMethodList().stream()
                .filter(candidate -> candidate.getName().equals(methodName))
                .findFirst()
                .orElseThrow(() ->
                        new AssertionError("Missing RPC " + file.getName() + ":" + serviceName + "." + methodName));
        assertEquals("." + inputType, method.getInputType());
        assertEquals("." + outputType, method.getOutputType());
        assertFalse(method.getClientStreaming());
        assertFalse(method.getServerStreaming());
    }

    private static byte[] appendUnknownField(byte[] serialized) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(serialized);
        CodedOutputStream codedOutput = CodedOutputStream.newInstance(output);
        codedOutput.writeBytes(UNKNOWN_FIELD_NUMBER, UNKNOWN_VALUE);
        codedOutput.flush();
        return output.toByteArray();
    }

    private record BaselineEntry(String kind, List<String> values, int lineNumber) {}

    private static final class ContractBaseline {
        private final List<BaselineEntry> entries;

        private ContractBaseline(List<BaselineEntry> entries) {
            this.entries = entries;
        }

        private static ContractBaseline load() throws IOException {
            List<BaselineEntry> entries = new ArrayList<>();
            try (InputStream input = ProtoWireContractTest.class.getResourceAsStream("/proto-contract-baseline.txt")) {
                assertNotNull(input, "Missing checked protobuf compatibility baseline");
                String content = new String(input.readAllBytes(), StandardCharsets.UTF_8);
                int lineNumber = 0;
                for (String line : content.lines().toList()) {
                    lineNumber++;
                    String trimmed = line.trim();
                    if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                        continue;
                    }
                    String[] columns = trimmed.split("\\|", -1);
                    entries.add(new BaselineEntry(
                            columns[0], Arrays.asList(columns).subList(1, columns.length), lineNumber));
                }
            }
            return new ContractBaseline(entries);
        }

        private void assertCompatible(FileDescriptorSet descriptorSet) {
            Map<String, FileDescriptorProto> files = filesByName(descriptorSet);
            for (BaselineEntry entry : entries) {
                try {
                    switch (entry.kind()) {
                        case "file" -> requireFile(files, entry);
                        case "message" -> requireMessage(files, entry);
                        case "field" -> requireField(files, entry);
                        case "reserved_range" -> requireReservedRange(files, entry);
                        case "reserved_name" -> requireReservedName(files, entry);
                        case "enum" -> requireEnum(files, entry);
                        case "enum_value" -> requireEnumValue(files, entry);
                        case "service" -> requireService(files, entry);
                        case "rpc" -> requireRpc(files, entry);
                        default -> throw new AssertionError("Unknown baseline kind on line " + entry.lineNumber());
                    }
                } catch (AssertionError error) {
                    throw new AssertionError(
                            "Baseline line " + entry.lineNumber() + " is incompatible: " + error.getMessage(), error);
                }
            }
        }

        private static void requireFile(Map<String, FileDescriptorProto> files, BaselineEntry entry) {
            assertNotNull(
                    files.get(entry.values().get(0)),
                    "Missing file " + entry.values().get(0));
        }

        private static void requireMessage(Map<String, FileDescriptorProto> files, BaselineEntry entry) {
            message(requiredFile(files, entry.values().get(0)), entry.values().get(1));
        }

        private static void requireField(Map<String, FileDescriptorProto> files, BaselineEntry entry) {
            FileDescriptorProto file = requiredFile(files, entry.values().get(0));
            DescriptorProto message = message(file, entry.values().get(1));
            int number = Integer.parseInt(entry.values().get(2));
            String expectedName = entry.values().get(3);
            String expectedLabel = entry.values().get(4);
            String expectedType = entry.values().get(5);
            FieldDescriptorProto byNumber = message.getFieldList().stream()
                    .filter(field -> field.getNumber() == number)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError(message.getName() + " missing field number " + number));
            assertEquals(
                    expectedName, byNumber.getName(), message.getName() + " field number " + number + " was reused");
            FieldDescriptorProto byName = message.getFieldList().stream()
                    .filter(field -> field.getName().equals(expectedName))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError(message.getName() + " missing field " + expectedName));
            assertEquals(number, byName.getNumber(), message.getName() + " field " + expectedName + " changed number");
            assertEquals(expectedLabel, labelName(byName));
            assertEquals(expectedType, typeName(byName));
        }

        private static void requireReservedRange(Map<String, FileDescriptorProto> files, BaselineEntry entry) {
            DescriptorProto message = message(
                    requiredFile(files, entry.values().get(0)), entry.values().get(1));
            int start = Integer.parseInt(entry.values().get(2));
            int end = Integer.parseInt(entry.values().get(3));
            assertTrue(
                    message.getReservedRangeList().stream()
                            .anyMatch(range -> range.getStart() == start && range.getEnd() == end),
                    "Missing reserved range " + start + ".." + end);
        }

        private static void requireReservedName(Map<String, FileDescriptorProto> files, BaselineEntry entry) {
            DescriptorProto message = message(
                    requiredFile(files, entry.values().get(0)), entry.values().get(1));
            assertTrue(message.getReservedNameList().contains(entry.values().get(2)));
        }

        private static void requireEnum(Map<String, FileDescriptorProto> files, BaselineEntry entry) {
            FileDescriptorProto file = requiredFile(files, entry.values().get(0));
            assertTrue(
                    file.getEnumTypeList().stream()
                            .anyMatch(enumDescriptor -> enumDescriptor
                                    .getName()
                                    .equals(entry.values().get(1))),
                    "Missing enum " + entry.values().get(1));
        }

        private static void requireEnumValue(Map<String, FileDescriptorProto> files, BaselineEntry entry) {
            FileDescriptorProto file = requiredFile(files, entry.values().get(0));
            String enumName = entry.values().get(1);
            String valueName = entry.values().get(2);
            int number = Integer.parseInt(entry.values().get(3));
            EnumDescriptorProto descriptor = file.getEnumTypeList().stream()
                    .filter(enumDescriptor -> enumDescriptor.getName().equals(enumName))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Missing enum " + enumName));
            EnumValueDescriptorProto byName = descriptor.getValueList().stream()
                    .filter(value -> value.getName().equals(valueName))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Missing enum value " + valueName));
            assertEquals(number, byName.getNumber(), enumName + "." + valueName + " changed number");
            assertEquals(
                    valueName,
                    descriptor.getValueList().stream()
                            .filter(value -> value.getNumber() == number)
                            .findFirst()
                            .orElseThrow(() -> new AssertionError("Missing enum number " + number))
                            .getName(),
                    enumName + " number " + number + " was reused");
        }

        private static void requireService(Map<String, FileDescriptorProto> files, BaselineEntry entry) {
            FileDescriptorProto file = requiredFile(files, entry.values().get(0));
            assertTrue(
                    file.getServiceList().stream()
                            .anyMatch(service ->
                                    service.getName().equals(entry.values().get(1))),
                    "Missing service " + entry.values().get(1));
        }

        private static void requireRpc(Map<String, FileDescriptorProto> files, BaselineEntry entry) {
            FileDescriptorProto file = requiredFile(files, entry.values().get(0));
            String serviceName = entry.values().get(1);
            String methodName = entry.values().get(2);
            ServiceDescriptorProto service = file.getServiceList().stream()
                    .filter(candidate -> candidate.getName().equals(serviceName))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Missing service " + serviceName));
            MethodDescriptorProto method = service.getMethodList().stream()
                    .filter(candidate -> candidate.getName().equals(methodName))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Missing RPC " + methodName));
            assertEquals(
                    "." + entry.values().get(3),
                    method.getInputType(),
                    serviceName + "." + methodName + " input signature changed");
            assertEquals(
                    "." + entry.values().get(4),
                    method.getOutputType(),
                    serviceName + "." + methodName + " output signature changed");
            assertEquals(Boolean.parseBoolean(entry.values().get(5)), method.getClientStreaming());
            assertEquals(Boolean.parseBoolean(entry.values().get(6)), method.getServerStreaming());
        }

        private static FileDescriptorProto requiredFile(Map<String, FileDescriptorProto> files, String name) {
            FileDescriptorProto file = files.get(name);
            assertNotNull(file, "Missing file " + name);
            return file;
        }
    }

    private static String labelName(FieldDescriptorProto field) {
        return field.getLabel().name().substring("LABEL_".length()).toLowerCase(Locale.ROOT);
    }

    private static String typeName(FieldDescriptorProto field) {
        if (field.hasTypeName()) {
            return field.getTypeName().substring(1);
        }
        return field.getType().name().substring("TYPE_".length()).toLowerCase(Locale.ROOT);
    }
}
