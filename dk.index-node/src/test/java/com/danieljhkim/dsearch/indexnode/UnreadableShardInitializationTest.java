package com.danieljhkim.dsearch.indexnode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.danieljhkim.dsearch.common.health.HealthHttpServer;
import com.danieljhkim.dsearch.indexnode.index.IndexManager;
import com.danieljhkim.dsearch.indexnode.index.ShardIndex;
import com.danieljhkim.dsearch.indexnode.index.UnreadableShardException;
import com.danieljhkim.dsearch.ml.embedding.TextEmbedder;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.lucene.codecs.Codec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class UnreadableShardInitializationTest {

    private static final TextEmbedder FAKE_EMBEDDER = ignored -> new float[] {1.0f, 0.0f, 0.0f};
    private static final List<String> LUCENE95_FILES = List.of("_0.cfe", "_0.cfs", "_0.si", "segments_1");
    private static final Duration LONG_FLUSH = Duration.ofHours(1);

    @TempDir
    Path tempDir;

    @Test
    void lucene95FixtureIsPermanentForShardIndexAndIndexManager() throws Exception {
        Path base = tempDir.resolve("lucene95");
        Path shard = base.resolve("shard-0");
        copyLucene95Fixture(shard);

        RuntimeException shardFailure =
                assertThrows(RuntimeException.class, () -> new ShardIndex("0", base, Map.of(), FAKE_EMBEDDER));
        assertLucene95Failure(shardFailure, shard);

        RuntimeException managerFailure = assertThrows(
                RuntimeException.class, () -> new IndexManager(base, 1, LONG_FLUSH, null, FAKE_EMBEDDER, 0));
        assertLucene95Failure(managerFailure, shard);
    }

    @Test
    void permanentFormatFailureIsReportedOnceOnHealthAndDoesNotRetry() throws Exception {
        Path base = tempDir.resolve("reported");
        copyLucene95Fixture(base.resolve("shard-0"));
        AtomicInteger opens = new AtomicInteger();
        AtomicInteger retries = new AtomicInteger();
        AtomicInteger permanentWaits = new AtomicInteger();
        AtomicReference<IndexManager> managerRef = new AtomicReference<>();
        AtomicReference<HealthHttpServer.Readiness> startup =
                new AtomicReference<>(HealthHttpServer.Readiness.notReady("index_initializing"));
        AtomicReference<String> healthReason = new AtomicReference<>();
        AtomicBoolean acceptingRequests = new AtomicBoolean(true);
        Logger logger = Logger.getLogger(IndexNodeApplication.class.getName());
        List<LogRecord> severe = new ArrayList<>();
        List<LogRecord> retryWarnings = new ArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getLevel() == Level.SEVERE) {
                    severe.add(record);
                }
                if (record.getLevel() == Level.WARNING
                        && record.getMessage() != null
                        && record.getMessage().contains("retrying initialization")) {
                    retryWarnings.add(record);
                }
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        logger.addHandler(handler);
        HttpServer server = HealthHttpServer.start(
                0,
                "index-node",
                () -> IndexNodeApplication.readiness(acceptingRequests, managerRef, startup),
                healthReason::get);
        try {
            IndexManager manager = IndexNodeApplication.waitForIndexManager(
                    () -> {
                        opens.incrementAndGet();
                        return new IndexManager(base, 1, LONG_FLUSH, null, FAKE_EMBEDDER, 0);
                    },
                    base,
                    managerRef,
                    startup,
                    healthReason,
                    new IndexNodeApplication.InitializationWait() {
                        @Override
                        void beforeRetry() {
                            retries.incrementAndGet();
                        }

                        @Override
                        void afterPermanentFailure() {
                            permanentWaits.incrementAndGet();
                        }
                    });

            assertNull(manager);
            assertEquals(1, opens.get());
            assertEquals(0, retries.get());
            assertEquals(1, permanentWaits.get());
            assertEquals(1, severe.size());
            assertTrue(retryWarnings.isEmpty());
            assertNotNull(severe.get(0).getThrown());
            assertFalse(severe.get(0).getMessage().contains("retrying initialization"));
            assertLucene95Failure(severe.get(0).getThrown(), base.resolve("shard-0"));

            String reason = healthReason.get();
            assertNotNull(reason);
            assertTrue(reason.startsWith("index_format_unsupported:shard-0"));
            assertEquals(reason, startup.get().reason());
            assertFalse(startup.get().ready());

            int port = server.getAddress().getPort();
            HttpResponse<String> health = get(port, "/health");
            assertEquals(200, health.statusCode());
            assertTrue(health.body().contains("\"status\":\"UP\""));
            assertTrue(health.body().contains("\"readiness\":\"DOWN\""));
            assertTrue(health.body().contains(jsonFragment(reason)));

            HttpResponse<String> readiness = get(port, "/readyz");
            assertEquals(503, readiness.statusCode());
            assertTrue(readiness.body().contains("\"status\":\"DOWN\""));
            assertTrue(readiness.body().contains(jsonFragment(reason)));

            HttpResponse<String> liveness = get(port, "/livez");
            assertEquals(200, liveness.statusCode());
            assertFalse(liveness.body().contains("index_format_unsupported"));
        } finally {
            logger.removeHandler(handler);
            server.stop(0);
        }
    }

    @Test
    void transientInitializationFailuresStillRetry() throws Exception {
        Path base = tempDir.resolve("transient");
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger retries = new AtomicInteger();
        AtomicReference<String> healthReason = new AtomicReference<>();
        AtomicReference<HealthHttpServer.Readiness> startup =
                new AtomicReference<>(HealthHttpServer.Readiness.notReady("index_initializing"));

        IndexManager manager = IndexNodeApplication.waitForIndexManager(
                () -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new RuntimeException("disk temporarily unavailable");
                    }
                    return new IndexManager(base, 1, LONG_FLUSH, null, FAKE_EMBEDDER, 0);
                },
                base,
                new AtomicReference<>(),
                startup,
                healthReason,
                new IndexNodeApplication.InitializationWait() {
                    @Override
                    void beforeRetry() {
                        retries.incrementAndGet();
                    }

                    @Override
                    void afterPermanentFailure() {
                        fail("a transient disk failure was classified as permanent");
                    }
                });

        try {
            assertNotNull(manager);
            assertEquals(3, attempts.get());
            assertEquals(2, retries.get());
            assertNull(healthReason.get());
            assertEquals(
                    "index_initialization_failed:RuntimeException",
                    startup.get().reason());
            assertTrue(manager.readiness().ready());
        } finally {
            if (manager != null) {
                manager.close();
            }
        }
    }

    @Test
    void productionInitializationWaitsAreInterruptible() {
        IndexNodeApplication.InitializationWait wait = new IndexNodeApplication.InitializationWait();
        Thread.currentThread().interrupt();
        assertThrows(InterruptedException.class, wait::beforeRetry);
        assertFalse(Thread.currentThread().isInterrupted());
        Thread.currentThread().interrupt();
        assertThrows(InterruptedException.class, wait::afterPermanentFailure);
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    void corruptOnDiskShardIsPermanent() throws Exception {
        Path base = tempDir.resolve("corrupt");
        try (ShardIndex index = new ShardIndex("0", base, Map.of(), FAKE_EMBEDDER)) {
            index.commit();
        }
        Path segments;
        try (Stream<Path> files = Files.list(base.resolve("shard-0"))) {
            segments = files.filter(path -> path.getFileName().toString().startsWith("segments_"))
                    .findFirst()
                    .orElseThrow();
        }
        byte[] bytes = Files.readAllBytes(segments);
        bytes[bytes.length / 2] ^= 0x5a;
        Files.write(segments, bytes);

        RuntimeException error = assertThrows(
                RuntimeException.class, () -> new IndexManager(base, 1, LONG_FLUSH, null, FAKE_EMBEDDER, 0));
        UnreadableShardException failure = UnreadableShardException.find(error);
        assertNotNull(failure);
        assertEquals("0", failure.shardId());
        assertTrue(failure.shardPath().endsWith("shard-0"));
        assertTrue(failure.readinessReason().contains("remedy="));
        assertTrue(failure.readinessReason().contains("docs/RECOVERY.md"));
        assertTrue(failure.kind() == UnreadableShardException.Kind.CORRUPT
                || failure.kind() == UnreadableShardException.Kind.UNSUPPORTED_FORMAT);
    }

    private static void assertLucene95Failure(Throwable error, Path shard) {
        UnreadableShardException failure = UnreadableShardException.find(error);
        assertNotNull(failure);
        assertEquals(UnreadableShardException.Kind.UNSUPPORTED_FORMAT, failure.kind());
        assertEquals("0", failure.shardId());
        assertEquals("Lucene95", failure.foundCodec());
        String supported = Codec.availableCodecs().stream()
                .sorted()
                .reduce((left, right) -> left + ", " + right)
                .map(names -> "[" + names + "]")
                .orElse("[]");
        assertEquals(supported, failure.supportedCodecs());
        assertTrue(failure.supportedCodecs().contains("Lucene104"));
        String path = shard.toAbsolutePath().normalize().toString();
        assertTrue(failure.readinessReason().startsWith("index_format_unsupported:shard-0"));
        assertTrue(failure.readinessReason().contains("path=" + path));
        assertTrue(failure.readinessReason().contains("codec=Lucene95"));
        assertTrue(failure.readinessReason().contains("supported=" + supported));
        assertTrue(failure.getMessage().contains(path));
        assertTrue(failure.getMessage().contains("Lucene95"));
        assertTrue(failure.getMessage().contains(supported));
        assertTrue(failure.readinessReason().contains("reindex through the alias workflow"));
        assertTrue(failure.readinessReason().contains("docs/RECOVERY.md"));
        assertTrue(failure.readinessReason().contains("move the local data directory aside"));
        assertTrue(failure.getMessage().contains("move the local data directory aside"));
    }

    private static void copyLucene95Fixture(Path shardDirectory) throws IOException {
        Files.createDirectories(shardDirectory);
        for (String name : LUCENE95_FILES) {
            try (InputStream in =
                    UnreadableShardInitializationTest.class.getResourceAsStream("/fixtures/lucene95-shard/" + name)) {
                if (in == null) {
                    throw new IllegalStateException("Missing Lucene95 fixture " + name);
                }
                Files.copy(in, shardDirectory.resolve(name));
            }
        }
    }

    private static HttpResponse<String> get(int port, String path) throws Exception {
        return HttpClient.newHttpClient()
                .send(
                        HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
    }

    private static String jsonFragment(String reason) {
        return reason.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
