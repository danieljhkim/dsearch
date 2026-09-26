package com.danieljhkim.dsearch.indexnode.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.danieljhkim.dsearch.common.model.SearchDocument;
import com.danieljhkim.dsearch.proto.common.SearchType;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplicaRepairStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void perDocumentGenerationsCannotOrderDifferentShardContents() throws Exception {
        try (IndexManager incomplete = manager(tempDir.resolve("incomplete"));
                IndexManager complete = manager(tempDir.resolve("complete"))) {
            apply(incomplete, "doc-A", "first", 1);
            apply(complete, "doc-A", "first", 1);
            apply(complete, "doc-B", "second", 1);

            IndexManager.ReplicaManifestData incompleteManifest = incomplete.replicaManifest("tenant_r1");
            IndexManager.ReplicaManifestData completeManifest = complete.replicaManifest("tenant_r1");
            assertEquals(1, incompleteManifest.committedPosition());
            assertEquals(1, completeManifest.committedPosition());
            assertEquals(1, incompleteManifest.documentCount());
            assertEquals(2, completeManifest.documentCount());
            assertNotEquals(incompleteManifest.contentChecksum(), completeManifest.contentChecksum());
            assertEquals(
                    1,
                    complete.searchDocument("tenant_r1", "second", 10, 0, SearchType.BM25)
                            .getTotalHits());
        }
    }

    @Test
    void interruptedTransferResumesAfterTargetRestartAndInstallsCommittedSnapshot() throws Exception {
        Path sourcePath = tempDir.resolve("source");
        Path targetPath = tempDir.resolve("target");
        ReplicaRepairStore.SourceSnapshot snapshot;
        try (IndexManager source = manager(sourcePath);
                IndexManager target = manager(targetPath)) {
            apply(source, "doc-1", "first", 1);
            apply(source, "doc-2", "second", 2);
            apply(target, "doc-1", "first", 1);
            assertNotEquals(source.replicaManifest("tenant_r1"), target.replicaManifest("tenant_r1"));

            ReplicaRepairStore sourceStore = new ReplicaRepairStore(source);
            ReplicaRepairStore targetStore = new ReplicaRepairStore(target);
            snapshot = sourceStore.openSnapshot("tenant_r1", 32 * 1024 * 1024);
            long offset = targetStore.begin(
                    "repair-resume",
                    snapshot.snapshotId(),
                    snapshot.payload().length,
                    snapshot.transferChecksum(),
                    snapshot.manifest());
            int half = snapshot.payload().length / 2;
            targetStore.write("repair-resume", offset, java.util.Arrays.copyOf(snapshot.payload(), half));
        }

        try (IndexManager restartedSource = manager(sourcePath);
                IndexManager restarted = manager(targetPath)) {
            ReplicaRepairStore.SourceSnapshot reopened =
                    new ReplicaRepairStore(restartedSource).openSnapshot("tenant_r1", 32 * 1024 * 1024);
            assertEquals(snapshot.snapshotId(), reopened.snapshotId());
            ReplicaRepairStore resumed = new ReplicaRepairStore(restarted);
            assertThrows(IndexManager.RepairInProgressException.class, () -> apply(restarted, "doc-3", "must-wait", 3));
            assertThrows(
                    IndexManager.RepairInProgressException.class,
                    () -> restarted.indexDocumentDurably(
                            "tenant_r1", new SearchDocument("legacy", Map.of("title", "must-wait"))));
            long offset = resumed.begin(
                    "repair-resume",
                    reopened.snapshotId(),
                    reopened.payload().length,
                    reopened.transferChecksum(),
                    reopened.manifest());
            byte[] remainder =
                    java.util.Arrays.copyOfRange(reopened.payload(), (int) offset, reopened.payload().length);
            resumed.write("repair-resume", offset, remainder);
            IndexManager.ReplicaManifestData repaired = resumed.finish("repair-resume");

            assertEquals(snapshot.manifest().contentChecksum(), repaired.contentChecksum());
            assertEquals(snapshot.manifest().committedPosition(), repaired.committedPosition());
            assertEquals(
                    2,
                    restarted
                            .searchDocument("tenant_r1", "first OR second", 10, 0, SearchType.BM25)
                            .getTotalHits());
        }
    }

    @Test
    void concurrentSourceWriteLeavesTargetDetectablyLaggingUntilNextIdempotentRepair() throws Exception {
        try (IndexManager source = manager(tempDir.resolve("source-concurrent"));
                IndexManager target = manager(tempDir.resolve("target-concurrent"))) {
            apply(source, "doc-1", "first", 1);
            ReplicaRepairStore sourceStore = new ReplicaRepairStore(source);
            ReplicaRepairStore targetStore = new ReplicaRepairStore(target);
            ReplicaRepairStore.SourceSnapshot first = sourceStore.openSnapshot("tenant_r1", 32 * 1024 * 1024);
            apply(source, "doc-2", "concurrent", 2);

            install(targetStore, "repair-first", first);
            assertEquals(1, target.replicaManifest("tenant_r1").committedPosition());
            assertEquals(2, source.replicaManifest("tenant_r1").committedPosition());

            ReplicaRepairStore.SourceSnapshot second = sourceStore.openSnapshot("tenant_r1", 32 * 1024 * 1024);
            install(targetStore, "repair-second", second);
            assertEquals(
                    source.replicaManifest("tenant_r1").contentChecksum(),
                    target.replicaManifest("tenant_r1").contentChecksum());
            assertEquals(
                    source.searchDocument("tenant_r1", "concurrent", 10, 0, SearchType.BM25)
                            .getTotalHits(),
                    target.searchDocument("tenant_r1", "concurrent", 10, 0, SearchType.BM25)
                            .getTotalHits());
        }
    }

    @Test
    void corruptedTransferCannotPublishTheReplica() throws Exception {
        try (IndexManager source = manager(tempDir.resolve("source-corrupt"));
                IndexManager target = manager(tempDir.resolve("target-corrupt"))) {
            apply(source, "doc-1", "verified", 1);
            ReplicaRepairStore.SourceSnapshot snapshot =
                    new ReplicaRepairStore(source).openSnapshot("tenant_r1", 32 * 1024 * 1024);
            ReplicaRepairStore targetStore = new ReplicaRepairStore(target);
            long offset = targetStore.begin(
                    "repair-corrupt",
                    snapshot.snapshotId(),
                    snapshot.payload().length,
                    snapshot.transferChecksum(),
                    snapshot.manifest());
            byte[] corrupted = snapshot.payload().clone();
            corrupted[corrupted.length / 2] ^= 1;
            targetStore.write("repair-corrupt", offset, corrupted);

            assertThrows(Exception.class, () -> targetStore.finish("repair-corrupt"));
            assertThrows(IndexManager.RepairInProgressException.class, () -> apply(target, "doc-2", "blocked", 2));
        }
    }

    private static void install(
            ReplicaRepairStore targetStore, String repairId, ReplicaRepairStore.SourceSnapshot snapshot)
            throws Exception {
        long offset = targetStore.begin(
                repairId,
                snapshot.snapshotId(),
                snapshot.payload().length,
                snapshot.transferChecksum(),
                snapshot.manifest());
        targetStore.write(repairId, offset, snapshot.payload());
        targetStore.finish(repairId);
    }

    private static void apply(IndexManager manager, String id, String content, long generation) throws Exception {
        manager.applyReplicatedIndex(
                "tenant_r1",
                new SearchDocument(id, Map.of("title", content, "content", content)),
                "op-" + generation,
                generation,
                7,
                "tenant",
                "n0");
    }

    private static IndexManager manager(Path path) {
        return new IndexManager(path, 10, Duration.ofHours(1), null, ignored -> new float[] {1.0f, 0.0f});
    }
}
