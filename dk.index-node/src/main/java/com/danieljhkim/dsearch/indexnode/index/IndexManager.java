package com.danieljhkim.dsearch.indexnode.index;

import com.danieljhkim.dsearch.common.config.AppConfig.FieldConfig;
import com.danieljhkim.dsearch.common.exception.SchemaMismatchException;
import com.danieljhkim.dsearch.common.exception.ShardNotFoundException;
import com.danieljhkim.dsearch.common.health.HealthHttpServer;
import com.danieljhkim.dsearch.common.model.SearchDocument;
import com.danieljhkim.dsearch.common.model.SearchResult;
import com.danieljhkim.dsearch.common.pagination.SortOptions;
import com.danieljhkim.dsearch.common.schema.IndexAlias;
import com.danieljhkim.dsearch.common.schema.IndexAliasStore;
import com.danieljhkim.dsearch.common.schema.IndexSchema;
import com.danieljhkim.dsearch.common.schema.IndexSchemaCompatibility;
import com.danieljhkim.dsearch.common.schema.ReindexJob;
import com.danieljhkim.dsearch.common.validation.PartitionIdValidator;
import com.danieljhkim.dsearch.ml.embedding.TextEmbedder;
import com.danieljhkim.dsearch.ml.embedding.TextEmbeddingService;
import com.danieljhkim.dsearch.proto.common.FacetRequest;
import com.danieljhkim.dsearch.proto.common.Filter;
import com.danieljhkim.dsearch.proto.common.SearchType;
import com.danieljhkim.dsearch.proto.index.RepresentativeQuery;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class IndexManager implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(IndexManager.class.getName());
    private static final String SHARD_PREFIX = "shard-";
    private static final String MUTATION_COMMIT_PREFIX = "dsearch.replication.";
    private static final String MUTATION_COMMIT_FORMAT = MUTATION_COMMIT_PREFIX + "format";
    private static final String MUTATION_COMMIT_COUNT = MUTATION_COMMIT_PREFIX + "count";
    private static final String MUTATION_COMMIT_ENTRY_PREFIX = MUTATION_COMMIT_PREFIX + "mutation.";
    private static final String MUTATION_COMMIT_LOGICAL_PARTITION = MUTATION_COMMIT_PREFIX + "logicalPartition";
    private static final String MUTATION_COMMIT_PRIMARY_NODE = MUTATION_COMMIT_PREFIX + "primaryNode";
    private static final String MUTATION_COMMIT_FORMAT_VERSION = "1";
    private static final Counter LUCENE_COMMIT_OUTCOMES = Counter.build()
            .name("dsearch_lucene_commit_outcomes_total")
            .help("Lucene commit attempts by bounded outcome")
            .labelNames("outcome")
            .register();
    private static final Histogram LUCENE_COMMIT_DURATION = Histogram.build()
            .name("dsearch_lucene_commit_duration_seconds")
            .help("Duration of Lucene commit attempts")
            .register();
    private static final Gauge LUCENE_LAST_COMMIT = Gauge.build()
            .name("dsearch_lucene_last_successful_commit_timestamp_seconds")
            .help("Unix timestamp of the last successful Lucene commit")
            .register();
    private static final Gauge DISK_AVAILABLE_BYTES = Gauge.build()
            .name("dsearch_lucene_disk_available_bytes")
            .help("Usable bytes on the Lucene volume")
            .register();
    public static final int DEFAULT_MAX_BUFFERED_OPS_PER_SHARD = 100;
    public static final Duration DEFAULT_MAX_FLUSH_INTERVAL = Duration.ofSeconds(5);
    public static final long DEFAULT_MINIMUM_FREE_DISK_BYTES = 104857600L;

    /**
     * How many index/delete operations to buffer per shard before forcing a commit.
     */
    private final int maxBufferedOpsPerShard;

    /**
     * Max time to let buffered ops sit before being flushed by the background
     * thread.
     */
    private final Duration maxFlushInterval;

    private final long minimumFreeDiskBytes;

    private final Path baseDir;
    private final TextEmbedder embeddingService;
    private final Closeable ownedEmbeddingService;
    private final Map<String, ShardIndex> shardIndexes = new ConcurrentHashMap<>();

    // Per-shard in-memory buffer of pending operations
    private final Map<String, ShardBuffer> shardBuffers = new ConcurrentHashMap<>();

    private final ScheduledExecutorService flushScheduler;

    // Field configurations for filtering, sorting, and highlighting
    private final Map<String, FieldConfig> fieldConfigMap;
    private final IndexSchema expectedSchema;
    private final IndexAliasStore aliasStore;
    private final Map<String, SchemaMismatchException> unservableIndexes = new ConcurrentHashMap<>();
    private final Map<String, AppliedMutation> appliedMutations = new ConcurrentHashMap<>();
    private final Map<String, ReplicaIdentity> replicaIdentities = new ConcurrentHashMap<>();
    private final Set<String> repairingShards = ConcurrentHashMap.newKeySet();
    private final Path legacyMutationStateFile;
    private volatile MutationFaultInjector mutationFaultInjector = (physicalIndex, stage) -> {};
    private volatile boolean closed;

    public IndexManager(Path baseDir) {
        this(baseDir, DEFAULT_MAX_BUFFERED_OPS_PER_SHARD, DEFAULT_MAX_FLUSH_INTERVAL, null);
    }

    public IndexManager(Path baseDir, int maxBufferedOpsPerShard, Duration maxFlushInterval) {
        this(baseDir, maxBufferedOpsPerShard, maxFlushInterval, null);
    }

    public IndexManager(
            Path baseDir, int maxBufferedOpsPerShard, Duration maxFlushInterval, List<FieldConfig> fieldConfigs) {
        this(
                baseDir,
                maxBufferedOpsPerShard,
                maxFlushInterval,
                fieldConfigs,
                null,
                true,
                DEFAULT_MINIMUM_FREE_DISK_BYTES);
    }

    public IndexManager(
            Path baseDir,
            int maxBufferedOpsPerShard,
            Duration maxFlushInterval,
            List<FieldConfig> fieldConfigs,
            long minimumFreeDiskBytes) {
        this(baseDir, maxBufferedOpsPerShard, maxFlushInterval, fieldConfigs, null, true, minimumFreeDiskBytes);
    }

    public IndexManager(
            Path baseDir,
            int maxBufferedOpsPerShard,
            Duration maxFlushInterval,
            List<FieldConfig> fieldConfigs,
            TextEmbedder embeddingService) {
        this(
                baseDir,
                maxBufferedOpsPerShard,
                maxFlushInterval,
                fieldConfigs,
                embeddingService,
                false,
                DEFAULT_MINIMUM_FREE_DISK_BYTES);
    }

    public IndexManager(
            Path baseDir,
            int maxBufferedOpsPerShard,
            Duration maxFlushInterval,
            List<FieldConfig> fieldConfigs,
            TextEmbedder embeddingService,
            long minimumFreeDiskBytes) {
        this(
                baseDir,
                maxBufferedOpsPerShard,
                maxFlushInterval,
                fieldConfigs,
                embeddingService,
                false,
                minimumFreeDiskBytes,
                null);
    }

    public IndexManager(
            Path baseDir,
            int maxBufferedOpsPerShard,
            Duration maxFlushInterval,
            List<FieldConfig> fieldConfigs,
            TextEmbedder embeddingService,
            long minimumFreeDiskBytes,
            IndexSchema expectedSchema) {
        this(
                baseDir,
                maxBufferedOpsPerShard,
                maxFlushInterval,
                fieldConfigs,
                embeddingService,
                false,
                minimumFreeDiskBytes,
                expectedSchema);
    }

    IndexManager(
            Path baseDir,
            int maxBufferedOpsPerShard,
            Duration maxFlushInterval,
            List<FieldConfig> fieldConfigs,
            TextEmbedder embeddingService,
            boolean ownsEmbeddingService,
            long minimumFreeDiskBytes) {
        this(
                baseDir,
                maxBufferedOpsPerShard,
                maxFlushInterval,
                fieldConfigs,
                embeddingService,
                ownsEmbeddingService,
                minimumFreeDiskBytes,
                null);
    }

    IndexManager(
            Path baseDir,
            int maxBufferedOpsPerShard,
            Duration maxFlushInterval,
            List<FieldConfig> fieldConfigs,
            TextEmbedder embeddingService,
            boolean ownsEmbeddingService,
            long minimumFreeDiskBytes,
            IndexSchema expectedSchema) {
        if (maxBufferedOpsPerShard < 1) {
            throw new IllegalArgumentException("maxBufferedOpsPerShard must be greater than 0");
        }
        if (maxFlushInterval == null || maxFlushInterval.compareTo(Duration.ZERO) <= 0) {
            throw new IllegalArgumentException("maxFlushInterval must be greater than zero");
        }
        if (minimumFreeDiskBytes < 0) {
            throw new IllegalArgumentException("minimumFreeDiskBytes must not be negative");
        }
        this.baseDir = baseDir;
        this.legacyMutationStateFile = baseDir.resolve("replication-mutations.properties");
        this.maxBufferedOpsPerShard = maxBufferedOpsPerShard;
        this.maxFlushInterval = maxFlushInterval;
        this.minimumFreeDiskBytes = minimumFreeDiskBytes;
        TextEmbedder resolvedEmbedder = embeddingService;
        if (resolvedEmbedder == null) {
            if (!ownsEmbeddingService) {
                throw new NullPointerException("embeddingService");
            }
            resolvedEmbedder = new TextEmbeddingService();
        }
        this.embeddingService = resolvedEmbedder;
        this.ownedEmbeddingService =
                ownsEmbeddingService && resolvedEmbedder instanceof Closeable closeable ? closeable : null;

        // Build field config map
        this.fieldConfigMap = new HashMap<>();
        if (fieldConfigs != null) {
            for (FieldConfig fc : fieldConfigs) {
                this.fieldConfigMap.put(fc.getName(), fc);
            }
        }
        this.expectedSchema =
                ShardIndex.resolveRuntimeSchema(expectedSchema, this.fieldConfigMap, this.embeddingService);
        this.aliasStore = new IndexAliasStore(this.baseDir);

        ScheduledExecutorService scheduler = null;
        try {
            this.aliasStore.load();
            boolean legacyMutationStateLoaded = loadLegacyMutationState();
            loadExistingShards();
            if (legacyMutationStateLoaded) {
                migrateLegacyMutationState();
            }

            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "index-flush-scheduler");
                t.setDaemon(true);
                return t;
            });

            // Periodic flush based on time
            long intervalMillis = Math.max(1L, maxFlushInterval.toMillis());
            scheduler.scheduleAtFixedRate(
                    this::flushBuffersOnSchedule, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
            this.flushScheduler = scheduler;
        } catch (IOException e) {
            RuntimeException wrapped =
                    new RuntimeException("Failed to initialize durable index state from " + baseDir, e);
            if (scheduler != null) {
                scheduler.shutdownNow();
            }
            closeInitializingResources(wrapped);
            throw wrapped;
        } catch (RuntimeException e) {
            if (scheduler != null) {
                scheduler.shutdownNow();
            }
            closeInitializingResources(e);
            throw e;
        }
    }

    /**
     * Reports whether this manager can safely serve Lucene traffic now. It deliberately does not
     * flush or write during a probe: opening the manager already validated Lucene state, while the
     * probe verifies that its volume remains writable and has enough room for future commits.
     */
    public HealthHttpServer.Readiness readiness() {
        if (closed) {
            return HealthHttpServer.Readiness.notReady("index_manager_closed");
        }
        if (shardBuffers.values().stream().anyMatch(ShardBuffer::isWriteFenced)) {
            return HealthHttpServer.Readiness.notReady("shard_write_fenced");
        }
        if (embeddingService instanceof TextEmbeddingService textEmbeddingService && !textEmbeddingService.isReady()) {
            return HealthHttpServer.Readiness.notReady("embedding_model_not_ready");
        }
        try {
            if (!Files.isDirectory(baseDir) || !Files.isWritable(baseDir)) {
                return HealthHttpServer.Readiness.notReady("lucene_directory_not_writable");
            }
            FileStore store = Files.getFileStore(baseDir);
            long usableSpace = store.getUsableSpace();
            DISK_AVAILABLE_BYTES.set(usableSpace);
            if (usableSpace < minimumFreeDiskBytes) {
                return HealthHttpServer.Readiness.notReady("disk_space_below_threshold");
            }
            return HealthHttpServer.Readiness.up();
        } catch (IOException e) {
            return HealthHttpServer.Readiness.notReady(
                    "lucene_storage_unavailable:" + e.getClass().getSimpleName());
        }
    }

    /**
     * On startup, scan baseDir for shard directories (shard-0, shard-1, ...)
     * and create ShardIndex instances for each one.
     */
    private void loadExistingShards() {
        try {
            Files.createDirectories(baseDir);
            List<String> shardDirectoryNames;
            try (Stream<Path> paths = Files.list(baseDir)) {
                shardDirectoryNames = paths.filter(Files::isDirectory)
                        .map(Path::getFileName)
                        .map(Path::toString)
                        .filter(name -> name.startsWith(SHARD_PREFIX))
                        .sorted()
                        .toList();
            }
            for (String dirName : shardDirectoryNames) {
                String shardId = dirName.substring(SHARD_PREFIX.length());
                openExistingShard(shardId);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load existing shard indexes from " + baseDir, e);
        }
    }

    private void openExistingShard(String shardId) {
        try {
            ShardIndex shardIndex =
                    new ShardIndex(shardId, baseDir, fieldConfigMap, embeddingService, expectedSchema, true);
            shardIndexes.put(shardId, shardIndex);
            shardBuffers.put(shardId, new ShardBuffer());
            unservableIndexes.remove(shardId);
            try {
                loadCommittedMutationState(shardIndex);
                aliasStore.ensureIdentityAlias(shardId);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load durable state for " + shardId, e);
            }
        } catch (SchemaMismatchException e) {
            LOGGER.log(Level.SEVERE, "Refusing to serve incompatible index " + shardId + ": " + e.getMessage());
            unservableIndexes.put(shardId, e);
            ShardIndex readable =
                    new ShardIndex(shardId, baseDir, fieldConfigMap, embeddingService, expectedSchema, false);
            shardIndexes.put(shardId, readable);
            shardBuffers.put(shardId, new ShardBuffer());
            try {
                loadCommittedMutationState(readable);
            } catch (IOException loadFailure) {
                throw new RuntimeException("Failed to load replicated mutation state for " + shardId, loadFailure);
            }
        }
    }

    private void closeInitializingResources(Throwable cause) {
        for (ShardIndex shardIndex : shardIndexes.values()) {
            try {
                shardIndex.close();
            } catch (Exception e) {
                cause.addSuppressed(e);
            }
        }
        shardIndexes.clear();
        shardBuffers.clear();
        if (ownedEmbeddingService != null) {
            try {
                ownedEmbeddingService.close();
            } catch (Exception e) {
                cause.addSuppressed(e);
            }
        }
    }

    private ShardIndex getOrCreateShard(String shardId) {
        ensureServable(shardId);
        ShardIndex index = shardIndexes.computeIfAbsent(shardId, id -> {
            ShardIndex si = new ShardIndex(id, baseDir, fieldConfigMap, embeddingService, expectedSchema, true);
            shardBuffers.put(id, new ShardBuffer());
            try {
                aliasStore.ensureIdentityAlias(id);
            } catch (IOException e) {
                throw new RuntimeException("Failed to persist identity alias for " + id, e);
            }
            return si;
        });
        shardBuffers.computeIfAbsent(shardId, id -> new ShardBuffer());
        return index;
    }

    public IndexSchema expectedSchema() {
        return expectedSchema;
    }

    public IndexAliasStore aliasStore() {
        return aliasStore;
    }

    Path baseDirectory() {
        return baseDir;
    }

    public String resolvePhysicalIndex(String aliasOrIndex) {
        return aliasStore.resolve(aliasOrIndex);
    }

    private void ensureServable(String physicalIndex) {
        SchemaMismatchException mismatch = unservableIndexes.get(physicalIndex);
        if (mismatch != null) {
            throw mismatch;
        }
    }

    private ShardIndex requireShard(String physicalIndex) {
        ShardIndex shardIndex = shardIndexes.get(physicalIndex);
        if (shardIndex == null) {
            throw new ShardNotFoundException(physicalIndex);
        }
        return shardIndex;
    }

    /** Returns the current committed Lucene cardinality for one physical shard. */
    public long countDocuments(String partitionId) throws IOException {
        PartitionIdValidator.validate(partitionId);
        ShardIndex shardIndex = shardIndexes.get(partitionId);
        return shardIndex == null ? 0L : shardIndex.countDocuments();
    }

    private ShardBuffer getBuffer(String shardId) {
        return shardBuffers.computeIfAbsent(shardId, id -> new ShardBuffer());
    }

    /**
     * Buffered indexing: we append the doc to an in-memory buffer and only
     * flush to Lucene (index + commit) when:
     * - we hit maxBufferedOpsPerShard, OR
     * - the background thread sees that maxFlushInterval has passed.
     */
    public void indexDocument(String partitionId, SearchDocument doc) throws IOException {
        String physicalIndex = resolvePhysicalIndex(partitionId);
        ensureNotRepairing(physicalIndex);
        ShardIndex shardIndex = getOrCreateShard(physicalIndex);
        ShardBuffer buffer = getBuffer(physicalIndex);

        buffer.lock.lock();
        try {
            buffer.requireWritable(physicalIndex);
            buffer.add(BufferedOperation.index(doc));
            if (buffer.pendingOperations.size() >= maxBufferedOpsPerShard) {
                flushShardBufferLocked(physicalIndex, shardIndex, buffer);
            }
        } finally {
            buffer.lock.unlock();
        }
    }

    /**
     * Indexes a document and does not return until Lucene has committed it.
     *
     * <p>The per-shard lock keeps this operation ordered with buffered writes. Any older
     * buffered operations are committed in the same durability boundary before this method
     * returns. If the commit fails, the exception is propagated and no durable acknowledgement
     * may be issued by the caller.
     */
    public void indexDocumentDurably(String partitionId, SearchDocument doc) throws IOException {
        String physicalIndex = resolvePhysicalIndex(partitionId);
        ensureNotRepairing(physicalIndex);
        ShardIndex shardIndex = getOrCreateShard(physicalIndex);
        ShardBuffer buffer = getBuffer(physicalIndex);

        buffer.lock.lock();
        try {
            buffer.requireWritable(physicalIndex);
            buffer.add(BufferedOperation.index(doc));
            flushShardBufferLocked(physicalIndex, shardIndex, buffer);
        } finally {
            buffer.lock.unlock();
        }
    }

    /**
     * Buffered delete: same semantics as indexDocument — delete ops are buffered
     * and only flushed/committed when thresholds/timeouts are reached.
     */
    public void deleteDocument(String partitionId, String docId) throws IOException {
        String physicalIndex = resolvePhysicalIndex(partitionId);
        ensureNotRepairing(physicalIndex);
        ShardIndex shardIndex = getOrCreateShard(physicalIndex);
        ShardBuffer buffer = getBuffer(physicalIndex);
        buffer.lock.lock();
        try {
            buffer.requireWritable(physicalIndex);
            buffer.add(BufferedOperation.delete(docId));
            if (buffer.pendingOperations.size() >= maxBufferedOpsPerShard) {
                flushShardBufferLocked(physicalIndex, shardIndex, buffer);
            }
        } finally {
            buffer.lock.unlock();
        }
    }

    /**
     * Deletes a document and does not return until Lucene has committed the deletion.
     *
     * <p>The per-shard lock keeps this operation ordered with buffered writes and deletes. Any older
     * buffered operations are committed in the same durability boundary before this method returns.
     * If the commit fails, the exception is propagated and no durable acknowledgement may be issued
     * by the caller.
     */
    public void deleteDocumentDurably(String partitionId, String docId) throws IOException {
        String physicalIndex = resolvePhysicalIndex(partitionId);
        ensureNotRepairing(physicalIndex);
        ShardIndex shardIndex = getOrCreateShard(physicalIndex);
        ShardBuffer buffer = getBuffer(physicalIndex);

        buffer.lock.lock();
        try {
            buffer.requireWritable(physicalIndex);
            buffer.add(BufferedOperation.delete(docId));
            flushShardBufferLocked(physicalIndex, shardIndex, buffer);
        } finally {
            buffer.lock.unlock();
        }
    }

    /** Applies a generation-fenced replicated upsert and durably records its idempotency identity. */
    public MutationResult applyReplicatedIndex(
            String partitionId,
            SearchDocument doc,
            String operationId,
            long operationGeneration,
            long placementGeneration)
            throws IOException {
        return applyReplicatedIndex(
                partitionId, doc, operationId, operationGeneration, placementGeneration, partitionId, "");
    }

    public MutationResult applyReplicatedIndex(
            String partitionId,
            SearchDocument doc,
            String operationId,
            long operationGeneration,
            long placementGeneration,
            String logicalPartitionId,
            String primaryNodeId)
            throws IOException {
        return applyReplicatedMutation(
                partitionId,
                doc.getId(),
                operationId,
                operationGeneration,
                placementGeneration,
                logicalPartitionId,
                primaryNodeId,
                MutationType.INDEX,
                BufferedOperation.index(doc),
                false);
    }

    public MutationResult applyReplicatedIndex(
            String partitionId,
            SearchDocument doc,
            String operationId,
            long operationGeneration,
            long placementGeneration,
            String logicalPartitionId,
            String primaryNodeId,
            boolean allocateGeneration)
            throws IOException {
        return applyReplicatedMutation(
                partitionId,
                doc.getId(),
                operationId,
                operationGeneration,
                placementGeneration,
                logicalPartitionId,
                primaryNodeId,
                MutationType.INDEX,
                BufferedOperation.index(doc),
                allocateGeneration);
    }

    /** Applies a generation-fenced replicated delete; replay of the same identity is a no-op. */
    public MutationResult applyReplicatedDelete(
            String partitionId, String docId, String operationId, long operationGeneration, long placementGeneration)
            throws IOException {
        return applyReplicatedDelete(
                partitionId, docId, operationId, operationGeneration, placementGeneration, partitionId, "");
    }

    public MutationResult applyReplicatedDelete(
            String partitionId,
            String docId,
            String operationId,
            long operationGeneration,
            long placementGeneration,
            String logicalPartitionId,
            String primaryNodeId)
            throws IOException {
        return applyReplicatedMutation(
                partitionId,
                docId,
                operationId,
                operationGeneration,
                placementGeneration,
                logicalPartitionId,
                primaryNodeId,
                MutationType.DELETE,
                BufferedOperation.delete(docId),
                false);
    }

    public MutationResult applyReplicatedDelete(
            String partitionId,
            String docId,
            String operationId,
            long operationGeneration,
            long placementGeneration,
            String logicalPartitionId,
            String primaryNodeId,
            boolean allocateGeneration)
            throws IOException {
        return applyReplicatedMutation(
                partitionId,
                docId,
                operationId,
                operationGeneration,
                placementGeneration,
                logicalPartitionId,
                primaryNodeId,
                MutationType.DELETE,
                BufferedOperation.delete(docId),
                allocateGeneration);
    }

    private MutationResult applyReplicatedMutation(
            String partitionId,
            String docId,
            String operationId,
            long operationGeneration,
            long placementGeneration,
            String logicalPartitionId,
            String primaryNodeId,
            MutationType mutationType,
            BufferedOperation mutation,
            boolean allocateGeneration)
            throws IOException {
        if (operationId == null || operationId.isBlank()) {
            throw new IllegalArgumentException("replicated mutation operation_id must not be blank");
        }
        if (operationGeneration < 0 || placementGeneration < 1) {
            throw new IllegalArgumentException(
                    "replicated mutation operation generation must be non-negative and placement generation positive");
        }
        if (operationGeneration == 0 && !allocateGeneration) {
            throw new IllegalArgumentException("only a primary may allocate an operation generation");
        }
        String physicalIndex = resolvePhysicalIndex(partitionId);
        ensureNotRepairing(physicalIndex);
        ShardIndex shardIndex = getOrCreateShard(physicalIndex);
        ShardBuffer buffer = getBuffer(physicalIndex);
        String mutationKey = physicalIndex + '\0' + docId;
        buffer.lock.lock();
        try {
            buffer.requireWritable(physicalIndex);
            AppliedMutation previous = appliedMutations.get(mutationKey);
            if (previous != null) {
                if (placementGeneration < previous.placementGeneration()) {
                    throw new StaleMutationException("placement generation " + placementGeneration
                            + " is older than committed generation " + previous.placementGeneration());
                }
                if (operationGeneration == 0 && operationId.equals(previous.operationId())) {
                    if (mutationType == previous.mutationType()) {
                        return new MutationResult(true, previous.operationGeneration());
                    }
                    throw new StaleMutationException(
                            "operation identity was already committed with a different mutation type");
                }
                if (operationGeneration == 0) {
                    operationGeneration = Math.incrementExact(previous.operationGeneration());
                }
                if (operationGeneration < previous.operationGeneration()) {
                    throw new StaleMutationException("operation generation " + operationGeneration
                            + " is older than committed generation " + previous.operationGeneration());
                }
                if (operationGeneration == previous.operationGeneration()) {
                    if (operationId.equals(previous.operationId()) && mutationType == previous.mutationType()) {
                        return new MutationResult(true, previous.operationGeneration());
                    }
                    throw new StaleMutationException(
                            "operation generation was already committed with a different identity");
                }
            }
            if (operationGeneration == 0) {
                operationGeneration = 1L;
            }

            try {
                for (BufferedOperation bufferedOperation : buffer.pendingOperations) {
                    bufferedOperation.apply(shardIndex);
                }
                mutation.apply(shardIndex);
                mutationFaultInjector.checkpoint(physicalIndex, MutationCommitStage.AFTER_MUTATION_APPLIED);

                AppliedMutation applied =
                        new AppliedMutation(operationId, operationGeneration, placementGeneration, mutationType);
                appliedMutations.put(mutationKey, applied);
                ReplicaIdentity identity = new ReplicaIdentity(logicalPartitionId, primaryNodeId);
                ReplicaIdentity previousIdentity = replicaIdentities.putIfAbsent(physicalIndex, identity);
                if (previousIdentity != null && !previousIdentity.compatibleWith(identity)) {
                    throw new StaleMutationException("replica identity changed for shard " + physicalIndex);
                }
                Map<String, String> commitUserData = mutationCommitUserData(physicalIndex, shardIndex);
                shardIndex.setLiveCommitData(commitUserData);
                mutationFaultInjector.checkpoint(physicalIndex, MutationCommitStage.AFTER_COMMIT_DATA_SET);
                commitShard(shardIndex);
                mutationFaultInjector.checkpoint(physicalIndex, MutationCommitStage.AFTER_COMMIT);

                buffer.pendingOperations.clear();
                buffer.firstPendingNanos = 0L;
                return new MutationResult(false, operationGeneration);
            } catch (IOException e) {
                buffer.poison(e);
                throw e;
            } catch (RuntimeException e) {
                buffer.poison(new IOException("Replicated mutation commit failed unexpectedly", e));
                throw e;
            }
        } finally {
            buffer.lock.unlock();
        }
    }

    private void ensureNotRepairing(String physicalIndex) {
        if (repairingShards.contains(physicalIndex)) {
            throw new RepairInProgressException("Shard " + physicalIndex + " is fenced while replica repair is active");
        }
    }

    private boolean loadLegacyMutationState() throws IOException {
        if (!Files.exists(legacyMutationStateFile)) {
            return false;
        }
        Properties properties = new Properties();
        try (var input = Files.newInputStream(legacyMutationStateFile)) {
            properties.load(input);
        }
        int count = parseNonNegativeInt(requiredMutationProperty(properties, "mutation.count"), "mutation.count");
        Set<String> expectedProperties = new HashSet<>();
        expectedProperties.add("mutation.count");
        for (int index = 0; index < count; index++) {
            String prefix = "mutation." + index + ".";
            expectedProperties.add(prefix + "key");
            expectedProperties.add(prefix + "operationId");
            expectedProperties.add(prefix + "operationGeneration");
            expectedProperties.add(prefix + "placementGeneration");
            expectedProperties.add(prefix + "type");
            String key = requiredMutationProperty(properties, prefix + "key");
            validateMutationKey(key, "legacy mutation ledger");
            mergeAppliedMutation(
                    key,
                    parseAppliedMutation(
                            requiredMutationProperty(properties, prefix + "operationId"),
                            requiredMutationProperty(properties, prefix + "operationGeneration"),
                            requiredMutationProperty(properties, prefix + "placementGeneration"),
                            requiredMutationProperty(properties, prefix + "type"),
                            "legacy mutation ledger"),
                    "legacy mutation ledger");
        }
        if (!properties.stringPropertyNames().equals(expectedProperties)) {
            throw new IOException("Legacy replicated mutation ledger contains incomplete or unknown entries");
        }
        return true;
    }

    private void loadCommittedMutationState(ShardIndex shardIndex) throws IOException {
        Map<String, String> userData = shardIndex.committedUserData();
        boolean hasReplicationState =
                userData.keySet().stream().anyMatch(key -> key.startsWith(MUTATION_COMMIT_PREFIX));
        if (!hasReplicationState) {
            return;
        }
        if (!MUTATION_COMMIT_FORMAT_VERSION.equals(userData.get(MUTATION_COMMIT_FORMAT))) {
            throw new IOException("Unsupported replicated mutation commit format for shard " + shardIndex.getShardId());
        }
        int expectedCount = parseNonNegativeInt(
                requiredCommitValue(userData, MUTATION_COMMIT_COUNT, shardIndex.getShardId()), MUTATION_COMMIT_COUNT);
        int actualCount = 0;
        for (Map.Entry<String, String> entry : userData.entrySet()) {
            if (!entry.getKey().startsWith(MUTATION_COMMIT_PREFIX)
                    || entry.getKey().equals(MUTATION_COMMIT_FORMAT)
                    || entry.getKey().equals(MUTATION_COMMIT_COUNT)
                    || entry.getKey().equals(MUTATION_COMMIT_LOGICAL_PARTITION)
                    || entry.getKey().equals(MUTATION_COMMIT_PRIMARY_NODE)) {
                continue;
            }
            if (!entry.getKey().startsWith(MUTATION_COMMIT_ENTRY_PREFIX)) {
                throw new IOException("Unknown replicated mutation commit key " + entry.getKey());
            }
            String encodedDocumentId = entry.getKey().substring(MUTATION_COMMIT_ENTRY_PREFIX.length());
            String documentId = decodeCanonical(encodedDocumentId, "document id");
            if (documentId.isBlank()) {
                throw new IOException("Replicated mutation commit contains a blank document id");
            }
            String[] values = entry.getValue().split("\\|", -1);
            if (values.length != 4) {
                throw new IOException("Malformed replicated mutation commit entry for document " + documentId);
            }
            String operationId = decodeCanonical(values[0], "operation id");
            AppliedMutation mutation = parseAppliedMutation(
                    operationId, values[1], values[2], values[3], "Lucene commit for shard " + shardIndex.getShardId());
            mergeAppliedMutation(
                    shardIndex.getShardId() + '\0' + documentId,
                    mutation,
                    "Lucene commit for shard " + shardIndex.getShardId());
            actualCount++;
        }
        if (actualCount != expectedCount) {
            throw new IOException("Replicated mutation commit count mismatch for shard " + shardIndex.getShardId()
                    + ": expected " + expectedCount + " but found " + actualCount);
        }
        String logicalPartition = userData.getOrDefault(MUTATION_COMMIT_LOGICAL_PARTITION, shardIndex.getShardId());
        String primaryNode = userData.getOrDefault(MUTATION_COMMIT_PRIMARY_NODE, "");
        replicaIdentities.put(shardIndex.getShardId(), new ReplicaIdentity(logicalPartition, primaryNode));
    }

    private void migrateLegacyMutationState() throws IOException {
        for (String mutationKey : appliedMutations.keySet()) {
            String physicalIndex = mutationKey.substring(0, mutationKey.indexOf('\0'));
            if (!shardIndexes.containsKey(physicalIndex)) {
                throw new IOException("Legacy replicated mutation ledger references missing shard " + physicalIndex);
            }
        }
        for (Map.Entry<String, ShardIndex> entry : shardIndexes.entrySet()) {
            ShardBuffer buffer = getBuffer(entry.getKey());
            buffer.lock.lock();
            try {
                entry.getValue().setLiveCommitData(mutationCommitUserData(entry.getKey(), entry.getValue()));
                commitShard(entry.getValue());
            } finally {
                buffer.lock.unlock();
            }
        }
        Files.delete(legacyMutationStateFile);
    }

    private Map<String, String> mutationCommitUserData(String physicalIndex, ShardIndex shardIndex) throws IOException {
        Map<String, String> commitUserData = new HashMap<>(shardIndex.committedUserData());
        commitUserData.keySet().removeIf(key -> key.startsWith(MUTATION_COMMIT_PREFIX));
        List<Map.Entry<String, AppliedMutation>> mutations = appliedMutations.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(physicalIndex + '\0'))
                .sorted(Map.Entry.comparingByKey())
                .toList();
        commitUserData.put(MUTATION_COMMIT_FORMAT, MUTATION_COMMIT_FORMAT_VERSION);
        commitUserData.put(MUTATION_COMMIT_COUNT, Integer.toString(mutations.size()));
        ReplicaIdentity identity = replicaIdentities.get(physicalIndex);
        if (identity != null) {
            commitUserData.put(MUTATION_COMMIT_LOGICAL_PARTITION, identity.logicalPartitionId());
            commitUserData.put(MUTATION_COMMIT_PRIMARY_NODE, identity.primaryNodeId());
        }
        for (Map.Entry<String, AppliedMutation> entry : mutations) {
            String documentId = entry.getKey().substring(physicalIndex.length() + 1);
            AppliedMutation mutation = entry.getValue();
            commitUserData.put(
                    MUTATION_COMMIT_ENTRY_PREFIX + encode(documentId),
                    encode(mutation.operationId())
                            + '|'
                            + mutation.operationGeneration()
                            + '|'
                            + mutation.placementGeneration()
                            + '|'
                            + mutation.mutationType().name());
        }
        return commitUserData;
    }

    public List<ReplicaManifestData> replicaManifests() throws IOException {
        List<ReplicaManifestData> manifests = new ArrayList<>();
        for (String shardId : shardIndexes.keySet().stream().sorted().toList()) {
            manifests.add(replicaManifest(shardId));
        }
        return List.copyOf(manifests);
    }

    public ReplicaManifestData replicaManifest(String shardId) throws IOException {
        PartitionIdValidator.validate(shardId);
        ShardIndex shard = requireShard(shardId);
        ShardBuffer buffer = getBuffer(shardId);
        buffer.lock.lock();
        try {
            flushShardBufferLocked(shardId, shard, buffer);
            ReplicaIdentity identity = replicaIdentities.getOrDefault(shardId, new ReplicaIdentity(shardId, ""));
            List<Map.Entry<String, AppliedMutation>> mutations = shardMutations(shardId);
            // Operation generations are per document. This maximum is diagnostic only and
            // cannot order two distinct shard histories for destructive replica repair.
            long committedPosition = mutations.stream()
                    .mapToLong(entry -> entry.getValue().operationGeneration())
                    .max()
                    .orElse(0L);
            long placementGeneration = mutations.stream()
                    .mapToLong(entry -> entry.getValue().placementGeneration())
                    .max()
                    .orElse(0L);
            return new ReplicaManifestData(
                    shardId,
                    identity.logicalPartitionId(),
                    identity.primaryNodeId(),
                    placementGeneration,
                    committedPosition,
                    canonicalChecksum(shard, mutations),
                    shard.countDocuments(),
                    repairingShards.contains(shardId) ? "repairing" : "ready",
                    "");
        } finally {
            buffer.lock.unlock();
        }
    }

    private List<Map.Entry<String, AppliedMutation>> shardMutations(String shardId) {
        return appliedMutations.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(shardId + '\0'))
                .sorted(Map.Entry.comparingByKey())
                .toList();
    }

    private String canonicalChecksum(ShardIndex shard, List<Map.Entry<String, AppliedMutation>> mutations) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            List<SearchDocument> documents = shard.exportDocuments().stream()
                    .sorted(java.util.Comparator.comparing(SearchDocument::getId))
                    .toList();
            for (SearchDocument document : documents) {
                updateDigest(digest, document.getId());
                document.getFields().entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEach(entry -> {
                            updateDigest(digest, entry.getKey());
                            updateDigest(digest, entry.getValue());
                        });
            }
            for (Map.Entry<String, AppliedMutation> entry : mutations) {
                updateDigest(digest, entry.getKey());
                updateDigest(digest, entry.getValue().operationId());
                updateDigest(digest, Long.toString(entry.getValue().operationGeneration()));
                updateDigest(digest, Long.toString(entry.getValue().placementGeneration()));
                updateDigest(digest, entry.getValue().mutationType().name());
            }
            return java.util.HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static void updateDigest(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(
                java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    public byte[] createReplicaSnapshot(String shardId, long maxBytes) throws IOException {
        PartitionIdValidator.validate(shardId);
        ShardIndex shard = requireShard(shardId);
        ShardBuffer buffer = getBuffer(shardId);
        buffer.lock.lock();
        try {
            flushShardBufferLocked(shardId, shard, buffer);
            java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
            try (java.util.zip.ZipOutputStream zip = new java.util.zip.ZipOutputStream(output)) {
                try (Stream<Path> paths = Files.walk(shard.indexPath())) {
                    for (Path path : paths.filter(Files::isRegularFile)
                            .filter(path ->
                                    !"write.lock".equals(path.getFileName().toString()))
                            .sorted()
                            .toList()) {
                        String relative =
                                shard.indexPath().relativize(path).toString().replace('\\', '/');
                        java.util.zip.ZipEntry zipEntry = new java.util.zip.ZipEntry(relative);
                        zipEntry.setTime(0L);
                        zip.putNextEntry(zipEntry);
                        Files.copy(path, zip);
                        zip.closeEntry();
                        if (output.size() > maxBytes) {
                            throw new IOException(
                                    "Replica snapshot exceeds configured maximum of " + maxBytes + " bytes");
                        }
                    }
                }
            }
            if (output.size() > maxBytes) {
                throw new IOException("Replica snapshot exceeds configured maximum of " + maxBytes + " bytes");
            }
            return output.toByteArray();
        } finally {
            buffer.lock.unlock();
        }
    }

    public void markReplicaRepairing(String shardId) {
        PartitionIdValidator.validate(shardId);
        repairingShards.add(shardId);
    }

    public void clearReplicaRepair(String shardId) {
        repairingShards.remove(shardId);
    }

    public void installReplicaSnapshot(String shardId, Path archive, Path workDirectory) throws IOException {
        PartitionIdValidator.validate(shardId);
        ShardBuffer buffer = getBuffer(shardId);
        buffer.lock.lock();
        Path stagedShard = workDirectory.resolve("shard-" + shardId).normalize();
        Path finalShard = baseDir.resolve("shard-" + shardId).normalize();
        Path backup = workDirectory.resolve("previous-shard").normalize();
        try {
            if (!stagedShard.startsWith(workDirectory) || !finalShard.startsWith(baseDir)) {
                throw new IOException("Replica repair path escaped its configured root");
            }
            deleteRecursively(stagedShard);
            Files.createDirectories(stagedShard);
            try (java.util.zip.ZipInputStream zip = new java.util.zip.ZipInputStream(Files.newInputStream(archive))) {
                java.util.zip.ZipEntry entry;
                while ((entry = zip.getNextEntry()) != null) {
                    Path target = stagedShard.resolve(entry.getName()).normalize();
                    if (!target.startsWith(stagedShard) || entry.isDirectory()) {
                        if (!target.startsWith(stagedShard)) {
                            throw new IOException("Replica snapshot contains an unsafe path");
                        }
                        continue;
                    }
                    Files.createDirectories(target.getParent());
                    Files.copy(zip, target, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            ShardIndex previous = shardIndexes.remove(shardId);
            if (previous != null) {
                previous.close();
            }
            removeShardMutationState(shardId);
            if (Files.exists(finalShard)) {
                moveAtomically(finalShard, backup);
            }
            try {
                moveAtomically(stagedShard, finalShard);
                openExistingShard(shardId);
                deleteRecursively(backup);
            } catch (IOException | RuntimeException failure) {
                if (Files.exists(backup)) {
                    deleteRecursively(finalShard);
                    moveAtomically(backup, finalShard);
                    openExistingShard(shardId);
                }
                throw failure;
            }
        } finally {
            buffer.lock.unlock();
        }
    }

    private void removeShardMutationState(String shardId) {
        appliedMutations.keySet().removeIf(key -> key.startsWith(shardId + '\0'));
        replicaIdentities.remove(shardId);
    }

    private static void moveAtomically(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private void mergeAppliedMutation(String key, AppliedMutation candidate, String source) throws IOException {
        AppliedMutation existing = appliedMutations.get(key);
        if (existing == null || candidate.equals(existing)) {
            appliedMutations.put(key, candidate);
            return;
        }
        if (candidate.operationGeneration() == existing.operationGeneration()
                && (!candidate.operationId().equals(existing.operationId())
                        || candidate.mutationType() != existing.mutationType())) {
            throw new IOException("Conflicting replicated mutation identity for " + key + " in " + source);
        }
        if (dominates(candidate, existing)) {
            appliedMutations.put(key, candidate);
        } else if (!dominates(existing, candidate)) {
            throw new IOException("Incomparable replicated mutation generations for " + key + " in " + source);
        }
    }

    private static boolean dominates(AppliedMutation left, AppliedMutation right) {
        return left.operationGeneration() >= right.operationGeneration()
                && left.placementGeneration() >= right.placementGeneration();
    }

    private static AppliedMutation parseAppliedMutation(
            String operationId,
            String operationGeneration,
            String placementGeneration,
            String mutationType,
            String source)
            throws IOException {
        if (operationId == null || operationId.isBlank()) {
            throw new IOException(source + " contains a blank operation id");
        }
        long parsedOperationGeneration = parsePositiveLong(operationGeneration, "operation generation", source);
        long parsedPlacementGeneration = parsePositiveLong(placementGeneration, "placement generation", source);
        try {
            return new AppliedMutation(
                    operationId,
                    parsedOperationGeneration,
                    parsedPlacementGeneration,
                    MutationType.valueOf(mutationType));
        } catch (IllegalArgumentException e) {
            throw new IOException(source + " contains unknown mutation type " + mutationType, e);
        }
    }

    private static long parsePositiveLong(String value, String field, String source) throws IOException {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 1) {
                throw new IOException(source + " contains non-positive " + field);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IOException(source + " contains invalid " + field, e);
        }
    }

    private static int parseNonNegativeInt(String value, String field) throws IOException {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw new IOException(field + " must not be negative");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IOException(field + " is not an integer", e);
        }
    }

    private static String requiredCommitValue(Map<String, String> userData, String name, String shardId)
            throws IOException {
        String value = userData.get(name);
        if (value == null || value.isBlank()) {
            throw new IOException("Replicated mutation commit for shard " + shardId + " is missing " + name);
        }
        return value;
    }

    private static void validateMutationKey(String key, String source) throws IOException {
        int separator = key.indexOf('\0');
        if (separator < 1 || separator == key.length() - 1) {
            throw new IOException(source + " contains an invalid mutation key");
        }
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodeCanonical(String value, String field) throws IOException {
        try {
            String decoded = new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
            if (!encode(decoded).equals(value)) {
                throw new IOException("Replicated mutation commit contains non-canonical " + field);
            }
            return decoded;
        } catch (IllegalArgumentException e) {
            throw new IOException("Replicated mutation commit contains invalid " + field, e);
        }
    }

    private static String requiredMutationProperty(Properties properties, String name) throws IOException {
        String value = properties.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IOException("Replicated mutation ledger is missing " + name);
        }
        return value;
    }

    /**
     * Search reads from the current committed state (backward compatible).
     */
    public SearchResult searchDocument(String partitionId, String query, int limit, int from, SearchType searchType)
            throws IOException {
        return searchDocument(partitionId, query, limit, from, searchType, null, false, null);
    }

    /**
     * Search with filters, highlighting, and facets.
     * Newly indexed docs may not be visible until a flush/commit happens.
     */
    public SearchResult searchDocument(
            String partitionId,
            String query,
            int limit,
            int from,
            SearchType searchType,
            List<Filter> filters,
            boolean highlight,
            List<FacetRequest> facetRequests)
            throws IOException {
        return searchDocument(
                partitionId, query, limit, from, searchType, filters, highlight, facetRequests, SortOptions.NONE);
    }

    /**
     * Search with filters, highlighting, facets, and an explicit ordering.
     *
     * <p>Sort eligibility is checked here against this node's field configuration. The query node
     * pre-checks against the schema it knows, but this is the authoritative check: the index node
     * is the only component that knows whether the DocValues an ordering needs actually exist.
     */
    public SearchResult searchDocument(
            String partitionId,
            String query,
            int limit,
            int from,
            SearchType searchType,
            List<Filter> filters,
            boolean highlight,
            List<FacetRequest> facetRequests,
            SortOptions sortOptions)
            throws IOException {
        return searchDocument(
                partitionId, query, limit, from, searchType, filters, highlight, facetRequests, sortOptions, null);
    }

    public SearchResult searchDocument(
            String partitionId,
            String query,
            int limit,
            int from,
            SearchType searchType,
            List<Filter> filters,
            boolean highlight,
            List<FacetRequest> facetRequests,
            SortOptions sortOptions,
            List<String> storedFields)
            throws IOException {
        String physicalIndex = resolvePhysicalIndex(partitionId);
        ensureServable(physicalIndex);
        ShardIndex shardIndex = shardIndexes.get(physicalIndex);
        if (shardIndex == null) {
            return new SearchResult(new ArrayList<>(), 0);
        }
        SortOptions effectiveSort = sortOptions == null ? SortOptions.NONE : sortOptions;
        if (effectiveSort.isSorted()) {
            effectiveSort.spec().validateAgainst(fieldConfigMap);
        }
        return switch (searchType) {
            case SearchType.SEMANTIC ->
                shardIndex.semanticSearch(
                        query, limit, from, filters, highlight, facetRequests, effectiveSort, storedFields);
            default ->
                shardIndex.search(query, limit, from, filters, highlight, facetRequests, effectiveSort, storedFields);
        };
    }

    /**
     * Reads a document by its exact stored id from the same resolved physical index used by
     * mutations. A missing local shard is a successfully confirmed absence, not a provisioning
     * request.
     */
    public SearchDocument getDocument(String partitionId, String documentId) {
        String physicalIndex = resolvePhysicalIndex(partitionId);
        ensureServable(physicalIndex);
        ShardIndex shardIndex = shardIndexes.get(physicalIndex);
        return shardIndex == null ? null : shardIndex.get(documentId);
    }

    public CreatedIndex createIndex(String indexName, String alias, IndexSchema schema) throws IOException {
        PartitionIdValidator.validate(indexName);
        String resolvedAlias = alias == null || alias.isBlank() ? indexName : alias;
        PartitionIdValidator.validate(resolvedAlias);
        IndexSchema toPersist = schema == null
                ? expectedSchema
                : ShardIndex.resolveRuntimeSchema(schema, fieldConfigMap, embeddingService);
        if (schema != null) {
            IndexSchemaCompatibility.requireCompatible(toPersist, expectedSchema);
        }
        IndexAlias existing = aliasStore.getAlias(resolvedAlias);
        if (existing != null && shardIndexes.containsKey(existing.getIndexName())) {
            throw new IllegalArgumentException(
                    "Alias '" + resolvedAlias + "' already points to index " + existing.getIndexName());
        }
        if (shardIndexes.containsKey(indexName)) {
            throw new IllegalArgumentException("Index '" + indexName + "' already exists");
        }
        getOrCreateShard(indexName);
        aliasStore.putAlias(
                resolvedAlias,
                indexName,
                existing == null ? null : existing.getPreviousIndexName(),
                existing == null ? 1 : existing.getGeneration());
        return new CreatedIndex(indexName, resolvedAlias, toPersist);
    }

    public InspectedSchema inspectSchema(String indexOrAlias) {
        PartitionIdValidator.validate(indexOrAlias);
        String physicalIndex = resolvePhysicalIndex(indexOrAlias);
        ShardIndex shardIndex = shardIndexes.get(physicalIndex);
        if (shardIndex == null) {
            throw new ShardNotFoundException(physicalIndex);
        }
        IndexAlias alias = aliasStore.getAlias(indexOrAlias);
        String aliasName = resolveAliasName(indexOrAlias, physicalIndex, alias);
        long generation = alias != null ? alias.getGeneration() : 1L;
        return new InspectedSchema(physicalIndex, aliasName, shardIndex.getSchema(), generation);
    }

    /**
     * Tokenizes sample text with the analyzer actually configured for the resolved index's
     * persisted schema. Read-only: it never creates a shard, so an unknown index/alias fails with
     * {@link ShardNotFoundException} rather than silently provisioning one.
     */
    public AnalyzedIndex analyzeText(String indexOrAlias, String text, int maxTokens) {
        PartitionIdValidator.validate(indexOrAlias);
        String physicalIndex = resolvePhysicalIndex(indexOrAlias);
        ensureServable(physicalIndex);
        ShardIndex shardIndex = shardIndexes.get(physicalIndex);
        if (shardIndex == null) {
            throw new ShardNotFoundException(physicalIndex);
        }
        IndexAlias alias = aliasStore.getAlias(indexOrAlias);
        String aliasName = resolveAliasName(indexOrAlias, physicalIndex, alias);
        ShardIndex.AnalyzedText analyzed = shardIndex.analyze(text, maxTokens);
        return new AnalyzedIndex(
                physicalIndex,
                aliasName,
                shardIndex.getSchema().analyzer().name(),
                analyzed.tokens(),
                analyzed.truncated());
    }

    private static String resolveAliasName(String indexOrAlias, String physicalIndex, IndexAlias alias) {
        return alias != null ? alias.getAlias() : indexOrAlias.equals(physicalIndex) ? physicalIndex : indexOrAlias;
    }

    public ReindexResult reindex(
            String sourceAlias, String targetIndex, IndexSchema schema, List<RepresentativeQuery> verificationQueries)
            throws IOException {
        PartitionIdValidator.validate(sourceAlias);
        String sourceIndex = resolvePhysicalIndex(sourceAlias);
        ShardIndex source = shardIndexes.get(sourceIndex);
        if (source == null) {
            throw new ShardNotFoundException(sourceIndex);
        }
        String resolvedTarget =
                targetIndex == null || targetIndex.isBlank() ? nextPhysicalIndex(sourceAlias) : targetIndex;
        PartitionIdValidator.validate(resolvedTarget);
        if (resolvedTarget.equals(sourceIndex)) {
            throw new IllegalArgumentException("Reindex target must be distinct from source index " + sourceIndex);
        }
        IndexSchema targetSchema = schema == null
                ? expectedSchema
                : ShardIndex.resolveRuntimeSchema(schema, fieldConfigMap, embeddingService);
        ReindexJob job = new ReindexJob();
        job.setJobId(UUID.randomUUID().toString());
        job.setSourceAlias(sourceAlias);
        job.setSourceIndex(sourceIndex);
        job.setTargetIndex(resolvedTarget);
        job.setStatus(ReindexJob.STATUS_COPYING);
        job.setSourceCount(source.countDocuments());
        aliasStore.saveJob(job);

        ShardIndex target = getOrCreateShard(resolvedTarget);
        try {
            for (SearchDocument document : source.exportDocuments()) {
                target.index(document);
            }
            target.commit();
            long targetCount = target.countDocuments();
            job.setTargetCount(targetCount);
            boolean verified =
                    targetCount == job.getSourceCount() && verifyQueries(source, target, verificationQueries);
            job.setVerificationPassed(verified);
            job.setStatus(verified ? ReindexJob.STATUS_VERIFIED : ReindexJob.STATUS_FAILED);
            if (!verified) {
                job.setError("reindex verification failed for counts or representative queries");
            }
            aliasStore.saveJob(job);
            return new ReindexResult(
                    verified, sourceIndex, resolvedTarget, job.getSourceCount(), targetCount, verified, job.getError());
        } catch (RuntimeException | IOException e) {
            job.setStatus(ReindexJob.STATUS_INTERRUPTED);
            job.setError(e.getMessage());
            try {
                aliasStore.saveJob(job);
            } catch (IOException persistError) {
                e.addSuppressed(persistError);
            }
            throw e;
        }
    }

    public IndexAlias swapAlias(String alias, String targetIndex) throws IOException {
        PartitionIdValidator.validate(alias);
        PartitionIdValidator.validate(targetIndex);
        ensureServable(targetIndex);
        requireShard(targetIndex);
        IndexAlias current = aliasStore.getAlias(alias);
        String currentIndex = current == null ? alias : current.getIndexName();
        if (!targetIndex.equals(currentIndex)) {
            ReindexJob verified = aliasStore.current().getReindexJobs().values().stream()
                    .filter(job -> alias.equals(job.getSourceAlias()) && targetIndex.equals(job.getTargetIndex()))
                    .findFirst()
                    .orElse(null);
            if (verified != null && !verified.isComplete()) {
                throw new IllegalArgumentException(
                        "Cannot swap alias '" + alias + "' until reindex of " + targetIndex + " is verified");
            }
        }
        return aliasStore.swap(alias, targetIndex);
    }

    public IndexAlias rollbackAlias(String alias) throws IOException {
        PartitionIdValidator.validate(alias);
        IndexAlias current = aliasStore.getAlias(alias);
        if (current == null) {
            throw new IllegalArgumentException("Unknown alias '" + alias + "'");
        }
        String previous = current.getPreviousIndexName();
        if (previous == null || previous.isBlank()) {
            throw new IllegalArgumentException("No previous alias target to roll back for '" + alias + "'");
        }
        requireShard(previous);
        return aliasStore.rollback(alias);
    }

    public String nextPhysicalIndex(String alias) {
        PartitionIdValidator.validate(alias);
        IndexAlias current = aliasStore.getAlias(alias);
        int generation = current == null ? 2 : current.getGeneration() + 1;
        String candidate = alias + "_" + generation;
        while (candidate.length() <= 64
                && (shardIndexes.containsKey(candidate) || candidate.equals(aliasStore.resolve(alias)))) {
            generation++;
            candidate = alias + "_" + generation;
        }
        if (candidate.length() > 64) {
            throw new IllegalArgumentException("Unable to allocate a physical index name for alias '" + alias + "'");
        }
        return candidate;
    }

    private boolean verifyQueries(ShardIndex source, ShardIndex target, List<RepresentativeQuery> verificationQueries) {
        if (verificationQueries == null || verificationQueries.isEmpty()) {
            return true;
        }
        for (RepresentativeQuery query : verificationQueries) {
            int size = query.getSize() > 0 ? query.getSize() : 10;
            SearchType searchType = query.getSearchType() == SearchType.SEARCH_TYPE_UNSPECIFIED
                    ? SearchType.BM25
                    : query.getSearchType();
            SearchResult sourceResult = searchOn(source, query.getQuery(), size, searchType);
            SearchResult targetResult = searchOn(target, query.getQuery(), size, searchType);
            if (sourceResult.getTotalHits() != targetResult.getTotalHits()) {
                return false;
            }
        }
        return true;
    }

    private SearchResult searchOn(ShardIndex shardIndex, String query, int size, SearchType searchType) {
        return switch (searchType) {
            case SearchType.SEMANTIC -> shardIndex.semanticSearch(query, size, 0, null, false, null);
            default -> shardIndex.search(query, size, 0, null, false, null);
        };
    }

    public record CreatedIndex(String indexName, String alias, IndexSchema schema) {}

    /**
     * @param generation alias generation currently serving the index; pagination cursors bind to
     *     it so an alias swap or rollback invalidates them rather than silently resuming against a
     *     different physical index
     */
    public record InspectedSchema(String indexName, String alias, IndexSchema schema, long generation) {}

    /**
     * @param analyzer name of the analyzer actually used by the resolved index's persisted schema
     * @param truncated true when the token stream was cut short by the caller's token limit
     */
    public record AnalyzedIndex(
            String indexName,
            String alias,
            String analyzer,
            List<ShardIndex.AnalyzedToken> tokens,
            boolean truncated) {}

    public record ReindexResult(
            boolean success,
            String sourceIndex,
            String targetIndex,
            long sourceCount,
            long targetCount,
            boolean verificationPassed,
            String error) {}

    /**
     * Force a synchronous commit of all shards:
     * - flushes all in-memory buffers
     * - calls commit() on every ShardIndex
     */
    public void commitAll() throws IOException {
        // First, flush all buffers
        for (Map.Entry<String, ShardIndex> entry : shardIndexes.entrySet()) {
            String partitionId = entry.getKey();
            ShardIndex shardIndex = entry.getValue();
            ShardBuffer buffer = getBuffer(partitionId);

            buffer.lock.lock();
            try {
                flushShardBufferLocked(partitionId, shardIndex, buffer);
                // Also ensure a final commit even if there were no buffered ops.
                commitShard(shardIndex);
            } finally {
                buffer.lock.unlock();
            }
        }
    }

    /**
     * Background scheduled task: scans all shard buffers and flushes any that
     * have pending ops that have been sitting longer than maxFlushInterval.
     */
    private void flushBuffersOnSchedule() {
        long nowNanos = System.nanoTime();
        for (Map.Entry<String, ShardIndex> entry : shardIndexes.entrySet()) {
            String partitionId = entry.getKey();
            ShardIndex shardIndex = entry.getValue();
            ShardBuffer buffer = getBuffer(partitionId);

            // Avoid blocking the scheduler if a shard is currently being flushed manually
            if (!buffer.lock.tryLock()) {
                continue;
            }
            try {
                if (buffer.pendingOperations.isEmpty()) {
                    continue;
                }
                long elapsedNanos = nowNanos - buffer.firstPendingNanos;
                if (elapsedNanos >= maxFlushInterval.toNanos()) {
                    try {
                        flushShardBufferLocked(partitionId, shardIndex, buffer);
                    } catch (IOException e) {
                        LOGGER.log(Level.SEVERE, "Failed to flush shard buffer for partitionId=" + partitionId, e);
                    }
                }
            } finally {
                buffer.lock.unlock();
            }
        }
    }

    /**
     * Flushes buffered docs and deletes for a single shard.
     * Assumes buffer.lock is already held.
     */
    private void flushShardBufferLocked(String partitionId, ShardIndex shardIndex, ShardBuffer buffer)
            throws IOException {
        buffer.requireWritable(partitionId);
        if (buffer.pendingOperations.isEmpty()) {
            return;
        }
        // Apply buffered operations
        for (BufferedOperation operation : buffer.pendingOperations) {
            operation.apply(shardIndex);
        }

        commitShard(shardIndex);
        // Clear the buffer and reset counters
        buffer.pendingOperations.clear();
        buffer.firstPendingNanos = 0L;
        long flushNanos = System.nanoTime();

        LOGGER.fine(() -> "Flushed shard buffer for partitionId=" + partitionId + " at " + flushNanos);
    }

    void commitShard(ShardIndex shardIndex) throws IOException {
        long started = System.nanoTime();
        try {
            shardIndex.commit();
            LUCENE_COMMIT_OUTCOMES.labels("success").inc();
            LUCENE_LAST_COMMIT.set(System.currentTimeMillis() / 1000.0);
        } catch (IOException e) {
            LUCENE_COMMIT_OUTCOMES.labels("failure").inc();
            throw e;
        } finally {
            LUCENE_COMMIT_DURATION.observe((System.nanoTime() - started) / 1_000_000_000.0);
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        flushScheduler.shutdown();
        try {
            if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                flushScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            flushScheduler.shutdownNow();
        }

        IOException first = null;
        try {
            // Final flush & close all shards
            for (Map.Entry<String, ShardIndex> entry : shardIndexes.entrySet()) {
                String shardId = entry.getKey();
                ShardIndex shardIndex = entry.getValue();
                ShardBuffer buffer = getBuffer(shardId);

                buffer.lock.lock();
                try {
                    if (buffer.commitFailure == null) {
                        flushShardBufferLocked(shardId, shardIndex, buffer);
                        shardIndex.close();
                    } else {
                        shardIndex.rollbackAndClose();
                    }
                } catch (IOException e) {
                    if (first == null) {
                        first = e;
                    } else {
                        first.addSuppressed(e);
                    }
                } finally {
                    buffer.lock.unlock();
                }
            }
        } finally {
            if (ownedEmbeddingService != null) {
                try {
                    ownedEmbeddingService.close();
                } catch (IOException e) {
                    if (first == null) {
                        first = e;
                    } else {
                        first.addSuppressed(e);
                    }
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }

    /**
     * Per-shard buffer of pending index/delete operations.
     * Guarded by a ReentrantLock for thread-safety.
     */
    private static final class ShardBuffer {
        final ReentrantLock lock = new ReentrantLock();
        final List<BufferedOperation> pendingOperations = new ArrayList<>();
        long firstPendingNanos = 0L;
        private volatile IOException commitFailure;

        void add(BufferedOperation operation) {
            if (pendingOperations.isEmpty()) {
                firstPendingNanos = System.nanoTime();
            }
            pendingOperations.add(operation);
        }

        void poison(IOException failure) {
            if (commitFailure == null) {
                commitFailure = failure;
            }
        }

        void requireWritable(String physicalIndex) throws IOException {
            if (commitFailure != null) {
                throw new IOException(
                        "Shard " + physicalIndex + " is write-fenced after an uncertain mutation commit",
                        commitFailure);
            }
        }

        boolean isWriteFenced() {
            return commitFailure != null;
        }
    }

    void setMutationFaultInjector(MutationFaultInjector mutationFaultInjector) {
        this.mutationFaultInjector =
                mutationFaultInjector == null ? (physicalIndex, stage) -> {} : mutationFaultInjector;
    }

    public record MutationResult(boolean duplicate, long committedGeneration) {}

    public static final class StaleMutationException extends IllegalStateException {
        public StaleMutationException(String message) {
            super(message);
        }
    }

    public static final class RepairInProgressException extends IllegalStateException {
        public RepairInProgressException(String message) {
            super(message);
        }
    }

    public record ReplicaManifestData(
            String shardId,
            String logicalPartitionId,
            String primaryNodeId,
            long placementGeneration,
            long committedPosition,
            String contentChecksum,
            long documentCount,
            String state,
            String lastError) {}

    private record ReplicaIdentity(String logicalPartitionId, String primaryNodeId) {
        private ReplicaIdentity {
            logicalPartitionId =
                    logicalPartitionId == null || logicalPartitionId.isBlank() ? "unknown" : logicalPartitionId;
            primaryNodeId = primaryNodeId == null ? "" : primaryNodeId;
        }

        boolean compatibleWith(ReplicaIdentity other) {
            return logicalPartitionId.equals(other.logicalPartitionId)
                    && (primaryNodeId.isBlank()
                            || other.primaryNodeId.isBlank()
                            || primaryNodeId.equals(other.primaryNodeId));
        }
    }

    private record AppliedMutation(
            String operationId, long operationGeneration, long placementGeneration, MutationType mutationType) {}

    @FunctionalInterface
    interface MutationFaultInjector {
        void checkpoint(String physicalIndex, MutationCommitStage stage) throws IOException;
    }

    enum MutationCommitStage {
        AFTER_MUTATION_APPLIED,
        AFTER_COMMIT_DATA_SET,
        AFTER_COMMIT
    }

    private enum MutationType {
        INDEX,
        DELETE
    }

    private record BufferedOperation(OperationType type, SearchDocument document, String docId) {
        static BufferedOperation index(SearchDocument document) {
            return new BufferedOperation(OperationType.INDEX, document, null);
        }

        static BufferedOperation delete(String docId) {
            return new BufferedOperation(OperationType.DELETE, null, docId);
        }

        void apply(ShardIndex shardIndex) throws IOException {
            switch (type) {
                case INDEX -> shardIndex.index(document);
                case DELETE -> shardIndex.delete(docId);
            }
        }
    }

    private enum OperationType {
        INDEX,
        DELETE
    }
}
