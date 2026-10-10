package com.danieljhkim.dsearch.indexnode;

import com.danieljhkim.dsearch.common.cluster.NodeMembershipAgent;
import com.danieljhkim.dsearch.common.config.AppConfig;
import com.danieljhkim.dsearch.common.config.AppConfig.FieldConfig;
import com.danieljhkim.dsearch.common.config.ConfigLoader;
import com.danieljhkim.dsearch.common.grpc.GrpcTransportSecurity;
import com.danieljhkim.dsearch.common.health.HealthHttpServer;
import com.danieljhkim.dsearch.indexnode.index.IndexManager;
import com.danieljhkim.dsearch.indexnode.index.UnreadableShardException;
import com.danieljhkim.dsearch.indexnode.server.IndexNodeServer;
import com.danieljhkim.dsearch.proto.cluster.ClusterServiceGrpc;
import com.danieljhkim.dsearch.proto.cluster.NodeRole;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class IndexNodeApplication {

    private static final Logger LOGGER = Logger.getLogger(IndexNodeApplication.class.getName());

    public static void main(String[] args) throws IOException, InterruptedException {

        int grpcPort = Integer.parseInt(System.getenv("INDEX_NODE_PORT"));
        int healthPort = Integer.parseInt(System.getenv("INDEX_NODE_HEALTH_PORT"));
        String baseDirStr = System.getenv("INDEX_NODE_BASE_DIR");
        Path baseDir = Path.of(baseDirStr);

        // Load configuration including field configs
        AppConfig appConfig = ConfigLoader.load();
        List<FieldConfig> fieldConfigs = appConfig.getFieldConfigs();

        IndexingRuntimeConfig indexingConfig = resolveIndexingConfig(appConfig, System.getenv());
        AtomicReference<IndexManager> indexManagerReference = new AtomicReference<>();
        AtomicReference<HealthHttpServer.Readiness> startupReadiness =
                new AtomicReference<>(HealthHttpServer.Readiness.notReady("index_initializing"));
        AtomicReference<String> permanentHealthReason = new AtomicReference<>();
        AtomicBoolean acceptingRequests = new AtomicBoolean();
        HttpServer healthServer = HealthHttpServer.start(
                healthPort,
                "index-node",
                () -> readiness(acceptingRequests, indexManagerReference, startupReadiness),
                permanentHealthReason::get);
        IndexManager indexManager = waitForIndexManager(
                baseDir, indexingConfig, fieldConfigs, indexManagerReference, startupReadiness, permanentHealthReason);
        if (indexManager == null) {
            throw new IllegalStateException("Index node is not ready: " + permanentHealthReason.get());
        }
        String localNodeId = System.getenv("INDEX_NODE_ID");
        if (localNodeId == null || localNodeId.isBlank()) {
            localNodeId = System.getenv("NODE_ID");
        }
        IndexNodeServer indexNodeServer = new IndexNodeServer(grpcPort, indexManager, appConfig, localNodeId);
        NodeMembershipAgent membershipAgent = createMembershipAgent(appConfig, System.getenv(), grpcPort, healthPort);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down IndexNode gRPC server...");
            acceptingRequests.set(false);
            if (membershipAgent != null) {
                membershipAgent.close();
            }
            try {
                indexNodeServer.shutdown();
                healthServer.stop(0);

            } catch (InterruptedException e) {
                LOGGER.log(Level.SEVERE, "Error during shutdown", e);
                Thread.currentThread().interrupt();
            }
            try {
                indexManager.close();
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Error closing IndexManager", e);
            }
        }));

        LOGGER.info(() -> "IndexNode gRPC server started on port " + grpcPort);
        LOGGER.info(() -> "IndexNode liveness endpoint on port " + healthPort + " at /livez and readiness at /readyz");

        indexNodeServer.startAsync();
        acceptingRequests.set(true);
        if (membershipAgent != null) {
            membershipAgent.start();
        }
        indexNodeServer.awaitTermination();
    }

    static HealthHttpServer.Readiness readiness(
            AtomicBoolean acceptingRequests,
            AtomicReference<IndexManager> indexManagerReference,
            AtomicReference<HealthHttpServer.Readiness> startupReadiness) {
        if (!acceptingRequests.get()) {
            return HealthHttpServer.Readiness.notReady("index_server_not_accepting_requests");
        }
        IndexManager manager = indexManagerReference.get();
        return manager == null ? startupReadiness.get() : manager.readiness();
    }

    static NodeMembershipAgent createMembershipAgent(
            AppConfig appConfig, Map<String, String> environment, int grpcPort, int healthPort) {
        NodeMembershipAgent.ResolvedMembership resolved =
                NodeMembershipAgent.resolve(appConfig, environment, NodeRole.NODE_ROLE_INDEX, grpcPort, healthPort);
        if (resolved == null) {
            return null;
        }
        ManagedChannel channel = GrpcTransportSecurity.from(appConfig)
                .newChannel(
                        resolved.settings().coordinatorHost(),
                        resolved.settings().coordinatorPort());
        return new NodeMembershipAgent(
                resolved.identity(), resolved.settings(), ClusterServiceGrpc.newBlockingStub(channel), channel);
    }

    static IndexingRuntimeConfig resolveIndexingConfig(AppConfig appConfig, Map<String, String> environment) {
        AppConfig.IndexingConfig config = appConfig.getIndexing();
        int maxBufferedOpsPerShard =
                config != null ? config.getMaxBufferedOpsPerShard() : IndexManager.DEFAULT_MAX_BUFFERED_OPS_PER_SHARD;
        int maxFlushIntervalSeconds = config != null
                ? config.getMaxFlushIntervalSeconds()
                : (int) IndexManager.DEFAULT_MAX_FLUSH_INTERVAL.toSeconds();
        long minimumFreeDiskBytes =
                config != null ? config.getMinimumFreeDiskBytes() : IndexManager.DEFAULT_MINIMUM_FREE_DISK_BYTES;

        maxBufferedOpsPerShard =
                readPositiveInt(environment, "INDEX_NODE_MAX_BUFFERED_OPS_PER_SHARD", maxBufferedOpsPerShard);
        maxFlushIntervalSeconds =
                readPositiveInt(environment, "INDEX_NODE_MAX_FLUSH_INTERVAL_SECONDS", maxFlushIntervalSeconds);
        minimumFreeDiskBytes =
                readNonNegativeLong(environment, "INDEX_NODE_MINIMUM_FREE_DISK_BYTES", minimumFreeDiskBytes);

        return new IndexingRuntimeConfig(
                maxBufferedOpsPerShard, Duration.ofSeconds(maxFlushIntervalSeconds), minimumFreeDiskBytes);
    }

    private static IndexManager waitForIndexManager(
            Path baseDir,
            IndexingRuntimeConfig indexingConfig,
            List<FieldConfig> fieldConfigs,
            AtomicReference<IndexManager> indexManagerReference,
            AtomicReference<HealthHttpServer.Readiness> startupReadiness,
            AtomicReference<String> permanentHealthReason)
            throws InterruptedException {
        return waitForIndexManager(
                () -> new IndexManager(
                        baseDir,
                        indexingConfig.maxBufferedOpsPerShard(),
                        indexingConfig.maxFlushInterval(),
                        fieldConfigs,
                        indexingConfig.minimumFreeDiskBytes()),
                baseDir,
                indexManagerReference,
                startupReadiness,
                permanentHealthReason,
                new InitializationWait() {});
    }

    /**
     * Opens the index, retrying transient failures. A permanent unreadable shard is logged once and
     * reported on the health port; this method then waits until the process is interrupted instead
     * of taking a membership lease.
     *
     * <p>Returns null when a permanent failure has already been reported and {@code wait} returned.
     * Production wait blocks until interruption, which propagates as {@link InterruptedException}.
     */
    static IndexManager waitForIndexManager(
            Supplier<IndexManager> opener,
            Path indexBaseDir,
            AtomicReference<IndexManager> indexManagerReference,
            AtomicReference<HealthHttpServer.Readiness> startupReadiness,
            AtomicReference<String> permanentHealthReason,
            InitializationWait wait)
            throws InterruptedException {
        while (true) {
            try {
                IndexManager manager = opener.get();
                indexManagerReference.set(manager);
                return manager;
            } catch (RuntimeException e) {
                UnreadableShardException permanent = UnreadableShardException.classify(e, "unknown", indexBaseDir);
                if (permanent != null) {
                    String reason = permanent.readinessReason();
                    startupReadiness.set(HealthHttpServer.Readiness.notReady(reason));
                    permanentHealthReason.set(reason);
                    LOGGER.log(Level.SEVERE, permanent.getMessage(), e);
                    wait.afterPermanentFailure();
                    return null;
                }
                String reason = "index_initialization_failed:" + e.getClass().getSimpleName();
                startupReadiness.set(HealthHttpServer.Readiness.notReady(reason));
                LOGGER.log(Level.WARNING, "Index node is live but not ready; retrying initialization", e);
                wait.beforeRetry();
            }
        }
    }

    /**
     * Production delays. Tests override these so a permanent failure does not park the suite and a
     * transient failure does not sleep for a second.
     */
    static class InitializationWait {
        void beforeRetry() throws InterruptedException {
            Thread.sleep(1000);
        }

        void afterPermanentFailure() throws InterruptedException {
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    private static int readPositiveInt(Map<String, String> environment, String name, int defaultValue) {
        String rawValue = environment.get(name);
        if (rawValue == null || rawValue.isBlank()) {
            return defaultValue;
        }

        int parsedValue = Integer.parseInt(rawValue);
        if (parsedValue < 1) {
            throw new IllegalArgumentException(name + " must be greater than 0");
        }
        return parsedValue;
    }

    private static long readNonNegativeLong(Map<String, String> environment, String name, long defaultValue) {
        String rawValue = environment.get(name);
        if (rawValue == null || rawValue.isBlank()) {
            return defaultValue;
        }
        long parsedValue = Long.parseLong(rawValue);
        if (parsedValue < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return parsedValue;
    }

    static record IndexingRuntimeConfig(
            int maxBufferedOpsPerShard, Duration maxFlushInterval, long minimumFreeDiskBytes) {}
}
