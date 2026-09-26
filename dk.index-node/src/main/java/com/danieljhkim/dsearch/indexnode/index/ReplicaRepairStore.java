package com.danieljhkim.dsearch.indexnode.index;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Durable, idempotent staging for bounded replica snapshot transfers. */
public final class ReplicaRepairStore {

    private static final Pattern SAFE_ID = Pattern.compile("^[A-Za-z0-9._-]{1,160}$");
    private static final int MAX_CHUNK_BYTES = 1024 * 1024;
    private static final String METADATA = "repair.properties";
    private static final String PAYLOAD = "snapshot.zip";

    private final IndexManager indexManager;
    private final Path repairRoot;
    private final Map<String, SourceSnapshot> sourceSnapshots = new ConcurrentHashMap<>();

    public ReplicaRepairStore(IndexManager indexManager) {
        this.indexManager = Objects.requireNonNull(indexManager, "indexManager");
        this.repairRoot = indexManager.baseDirectory().resolve(".replica-repair");
        restoreFences();
    }

    public SourceSnapshot openSnapshot(String shardId, long maxBytes) throws IOException {
        if (maxBytes < 1) {
            throw new IllegalArgumentException("max_snapshot_bytes must be positive");
        }
        IndexManager.ReplicaSnapshotData captured = indexManager.captureReplicaSnapshot(shardId, maxBytes);
        IndexManager.ReplicaManifestData manifest = captured.manifest();
        byte[] payload = captured.payload();
        String checksum = sha256(payload);
        String snapshotId = shardId + "-" + checksum;
        SourceSnapshot snapshot = new SourceSnapshot(snapshotId, payload, checksum, manifest);
        if (sourceSnapshots.size() >= 8) {
            sourceSnapshots.clear();
        }
        sourceSnapshots.put(snapshotId, snapshot);
        return snapshot;
    }

    public SnapshotChunk readSnapshot(String snapshotId, long offset, int maxBytes) {
        SourceSnapshot snapshot = sourceSnapshots.get(snapshotId);
        if (snapshot == null) {
            throw new IllegalArgumentException("Unknown or expired replica snapshot " + snapshotId);
        }
        if (offset < 0 || offset > snapshot.payload().length) {
            throw new IllegalArgumentException("snapshot offset is outside the payload");
        }
        int bounded = Math.min(MAX_CHUNK_BYTES, Math.max(1, maxBytes));
        int length = (int) Math.min(bounded, snapshot.payload().length - offset);
        byte[] data = java.util.Arrays.copyOfRange(snapshot.payload(), (int) offset, (int) offset + length);
        return new SnapshotChunk(offset, data, offset + length == snapshot.payload().length);
    }

    public synchronized long begin(
            String repairId,
            String snapshotId,
            long totalBytes,
            String transferChecksum,
            IndexManager.ReplicaManifestData manifest)
            throws IOException {
        validateId(repairId, "repair_id");
        if (totalBytes < 0 || transferChecksum == null || transferChecksum.isBlank()) {
            throw new IllegalArgumentException("repair transfer metadata is incomplete");
        }
        Path directory = repairDirectory(repairId);
        Files.createDirectories(directory);
        Path metadataPath = directory.resolve(METADATA);
        Properties requested = properties(snapshotId, totalBytes, transferChecksum, manifest, "transferring", "");
        if (Files.exists(metadataPath)) {
            Properties existing = load(metadataPath);
            if (!sameTransfer(existing, requested)) {
                deleteRecursively(directory);
                Files.createDirectories(directory);
                persist(metadataPath, requested);
            }
        } else {
            persist(metadataPath, requested);
        }
        indexManager.markReplicaRepairing(manifest.shardId());
        Path payload = directory.resolve(PAYLOAD);
        long offset = Files.exists(payload) ? Files.size(payload) : 0L;
        if (offset > totalBytes) {
            Files.delete(payload);
            offset = 0L;
        }
        return offset;
    }

    public synchronized long write(String repairId, long offset, byte[] data) throws IOException {
        if (data == null || data.length == 0 || data.length > MAX_CHUNK_BYTES) {
            throw new IllegalArgumentException("repair chunk must contain 1.." + MAX_CHUNK_BYTES + " bytes");
        }
        Path directory = repairDirectory(repairId);
        Properties metadata = load(directory.resolve(METADATA));
        Path payload = directory.resolve(PAYLOAD);
        long current = Files.exists(payload) ? Files.size(payload) : 0L;
        if (offset != current) {
            throw new OffsetMismatchException(current);
        }
        long total = Long.parseLong(metadata.getProperty("totalBytes"));
        if (current + data.length > total) {
            throw new IllegalArgumentException("repair chunk exceeds declared snapshot size");
        }
        try (FileChannel channel = FileChannel.open(
                payload, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
        return current + data.length;
    }

    public synchronized IndexManager.ReplicaManifestData finish(String repairId) throws IOException {
        Path directory = repairDirectory(repairId);
        Path metadataPath = directory.resolve(METADATA);
        Properties metadata = load(metadataPath);
        metadata.setProperty("state", "verifying");
        persist(metadataPath, metadata);
        Path payload = directory.resolve(PAYLOAD);
        long expectedSize = Long.parseLong(metadata.getProperty("totalBytes"));
        if (!Files.exists(payload) || Files.size(payload) != expectedSize) {
            throw fail(metadataPath, metadata, "snapshot size does not match the declared transfer size");
        }
        String expectedTransferChecksum = metadata.getProperty("transferChecksum");
        if (!expectedTransferChecksum.equals(sha256(payload))) {
            throw fail(metadataPath, metadata, "snapshot transfer checksum mismatch");
        }
        String shardId = metadata.getProperty("shardId");
        try {
            indexManager.installReplicaSnapshot(shardId, payload, directory.resolve("install"));
            IndexManager.ReplicaManifestData actual = indexManager.replicaManifest(shardId);
            if (!metadata.getProperty("contentChecksum").equals(actual.contentChecksum())
                    || Long.parseLong(metadata.getProperty("committedPosition")) != actual.committedPosition()
                    || Long.parseLong(metadata.getProperty("placementGeneration")) != actual.placementGeneration()) {
                throw new IOException("installed replica does not match source manifest");
            }
            clearRepairsForShard(shardId);
            indexManager.clearReplicaRepair(shardId);
            return new IndexManager.ReplicaManifestData(
                    actual.shardId(),
                    actual.logicalPartitionId(),
                    actual.primaryNodeId(),
                    actual.placementGeneration(),
                    actual.committedPosition(),
                    actual.contentChecksum(),
                    actual.documentCount(),
                    "ready",
                    "");
        } catch (IOException | RuntimeException e) {
            metadata.setProperty("state", "failed");
            metadata.setProperty(
                    "lastError", e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            persist(metadataPath, metadata);
            throw e;
        }
    }

    public synchronized void abort(String repairId, String reason) throws IOException {
        Path metadataPath = repairDirectory(repairId).resolve(METADATA);
        Properties metadata = load(metadataPath);
        metadata.setProperty("state", "failed");
        metadata.setProperty("lastError", reason == null ? "aborted" : reason);
        persist(metadataPath, metadata);
    }

    private void restoreFences() {
        try {
            Files.createDirectories(repairRoot);
            try (Stream<Path> paths = Files.list(repairRoot)) {
                for (Path path : paths.filter(Files::isDirectory).toList()) {
                    Path metadata = path.resolve(METADATA);
                    if (Files.exists(metadata)) {
                        indexManager.markReplicaRepairing(load(metadata).getProperty("shardId"));
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to restore replica repair fences", e);
        }
    }

    private void clearRepairsForShard(String shardId) throws IOException {
        try (Stream<Path> paths = Files.list(repairRoot)) {
            for (Path path : paths.filter(Files::isDirectory).toList()) {
                Path metadata = path.resolve(METADATA);
                if (Files.exists(metadata) && shardId.equals(load(metadata).getProperty("shardId"))) {
                    deleteRecursively(path);
                }
            }
        }
    }

    private Path repairDirectory(String repairId) {
        validateId(repairId, "repair_id");
        Path resolved = repairRoot.resolve(repairId).normalize();
        if (!resolved.startsWith(repairRoot)) {
            throw new IllegalArgumentException("repair_id resolves outside the staging root");
        }
        return resolved;
    }

    private static Properties properties(
            String snapshotId,
            long totalBytes,
            String transferChecksum,
            IndexManager.ReplicaManifestData manifest,
            String state,
            String lastError) {
        Properties properties = new Properties();
        properties.setProperty("snapshotId", snapshotId);
        properties.setProperty("totalBytes", Long.toString(totalBytes));
        properties.setProperty("transferChecksum", transferChecksum);
        properties.setProperty("shardId", manifest.shardId());
        properties.setProperty("logicalPartitionId", manifest.logicalPartitionId());
        properties.setProperty("primaryNodeId", manifest.primaryNodeId());
        properties.setProperty("placementGeneration", Long.toString(manifest.placementGeneration()));
        properties.setProperty("committedPosition", Long.toString(manifest.committedPosition()));
        properties.setProperty("contentChecksum", manifest.contentChecksum());
        properties.setProperty("documentCount", Long.toString(manifest.documentCount()));
        properties.setProperty("state", state);
        properties.setProperty("lastError", lastError);
        return properties;
    }

    private static boolean sameTransfer(Properties left, Properties right) {
        return left.getProperty("shardId").equals(right.getProperty("shardId"))
                && left.getProperty("totalBytes").equals(right.getProperty("totalBytes"))
                && left.getProperty("transferChecksum").equals(right.getProperty("transferChecksum"));
    }

    private static Properties load(Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new IllegalArgumentException(
                    "Unknown replica repair " + path.getParent().getFileName());
        }
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(path)) {
            properties.load(input);
        }
        return properties;
    }

    private static void persist(Path path, Properties properties) throws IOException {
        Files.createDirectories(path.getParent());
        Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
        try (OutputStream output = Files.newOutputStream(
                temporary, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            properties.store(output, "dsearch replica repair staging");
        }
        try {
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (java.nio.file.AtomicMoveNotSupportedException e) {
            Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static IOException fail(Path metadataPath, Properties metadata, String message) throws IOException {
        metadata.setProperty("state", "failed");
        metadata.setProperty("lastError", message);
        persist(metadataPath, metadata);
        return new IOException(message);
    }

    private static String sha256(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(path)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    digest.update(buffer, 0, read);
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static void validateId(String value, String field) {
        if (value == null || !SAFE_ID.matcher(value).matches()) {
            throw new IllegalArgumentException(field + " contains unsupported characters");
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

    public record SourceSnapshot(
            String snapshotId, byte[] payload, String transferChecksum, IndexManager.ReplicaManifestData manifest) {}

    public record SnapshotChunk(long offset, byte[] data, boolean complete) {}

    public static final class OffsetMismatchException extends IllegalStateException {
        private final long expectedOffset;

        OffsetMismatchException(long expectedOffset) {
            super("repair chunk offset mismatch; expected " + expectedOffset);
            this.expectedOffset = expectedOffset;
        }

        public long expectedOffset() {
            return expectedOffset;
        }
    }
}
