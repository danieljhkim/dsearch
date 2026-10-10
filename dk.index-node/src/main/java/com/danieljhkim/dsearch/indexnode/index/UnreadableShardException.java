package com.danieljhkim.dsearch.indexnode.index;

import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;

/**
 * Permanent failure to open a shard. Retrying cannot succeed: the on-disk Lucene format is
 * unsupported or the index is corrupt.
 *
 * <p>This build does not ship {@code lucene-backward-codecs}. Lucene 10 reads a 9.x codec only with
 * that jar, and adding it is a separate decision. The operator reindexes, restores a snapshot, or
 * moves the local data directory aside and restarts.
 */
public final class UnreadableShardException extends RuntimeException {

    static final String REMEDY = "reindex through the alias workflow, restore from snapshot (docs/RECOVERY.md), "
            + "or move the local data directory aside and restart";

    private static final Pattern CODEC_NAME = Pattern.compile("codec '([^']+)'");
    private static final int MAX_CAUSE_DEPTH = 16;

    public enum Kind {
        UNSUPPORTED_FORMAT,
        CORRUPT
    }

    private final String shardId;
    private final Path shardPath;
    private final Kind kind;
    private final String foundCodec;
    private final String supportedCodecs;
    private final Integer formatVersion;
    private final Integer formatMin;
    private final Integer formatMax;

    private UnreadableShardException(
            String shardId,
            Path shardPath,
            Kind kind,
            String foundCodec,
            String supportedCodecs,
            Integer formatVersion,
            Integer formatMin,
            Integer formatMax,
            Throwable cause) {
        super(
                actionableMessage(
                        shardId, shardPath, kind, foundCodec, supportedCodecs, formatVersion, formatMin, formatMax),
                Objects.requireNonNull(cause, "cause"));
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.shardPath = Objects.requireNonNull(shardPath, "shardPath");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.foundCodec = Objects.requireNonNull(foundCodec, "foundCodec");
        this.supportedCodecs = Objects.requireNonNull(supportedCodecs, "supportedCodecs");
        this.formatVersion = formatVersion;
        this.formatMin = formatMin;
        this.formatMax = formatMax;
    }

    public String shardId() {
        return shardId;
    }

    public Path shardPath() {
        return shardPath;
    }

    public Kind kind() {
        return kind;
    }

    public String foundCodec() {
        return foundCodec;
    }

    public String supportedCodecs() {
        return supportedCodecs;
    }

    /**
     * Stable not-ready token plus the path, codecs, and remedy. {@code /readyz} and, for this
     * failure, node {@code /health} publish this string.
     */
    public String readinessReason() {
        String token = kind == Kind.CORRUPT ? "index_corrupt" : "index_format_unsupported";
        StringBuilder reason = new StringBuilder();
        reason.append(token)
                .append(":shard-")
                .append(shardId)
                .append(" path=")
                .append(absoluteShardPath())
                .append(" codec=")
                .append(foundCodec)
                .append(" supported=")
                .append(supportedCodecs);
        if (formatVersion != null) {
            reason.append(" formatVersion=")
                    .append(formatVersion)
                    .append(" formatMin=")
                    .append(formatMin)
                    .append(" formatMax=")
                    .append(formatMax);
        }
        reason.append(" remedy=").append(REMEDY);
        return reason.toString();
    }

    /**
     * Returns the permanent failure when {@code error} is or wraps an {@link UnreadableShardException}.
     * Other failures, including a disk that is briefly unavailable, return null so startup can retry.
     */
    /**
     * Resolves a permanent open failure. Prefers an exception already translated at shard open, and
     * otherwise classifies a raw Lucene format or corruption error against {@code shardPath}.
     */
    public static UnreadableShardException classify(Throwable error, String shardId, Path shardPath) {
        UnreadableShardException found = find(error);
        if (found != null) {
            return found;
        }
        return translate(shardId, shardPath, error);
    }

    public static UnreadableShardException find(Throwable error) {
        Throwable current = error;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (current instanceof UnreadableShardException unreadable) {
                return unreadable;
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                return null;
            }
            current = cause;
        }
        return null;
    }

    /**
     * Classifies a Lucene open failure. Returns null when the failure is not a permanent format or
     * corruption error.
     */
    static UnreadableShardException translate(String shardId, Path shardPath, Throwable error) {
        IndexFormatTooOldException tooOld = findCause(error, IndexFormatTooOldException.class);
        if (tooOld != null) {
            return new UnreadableShardException(
                    shardId,
                    shardPath,
                    Kind.UNSUPPORTED_FORMAT,
                    codecName(tooOld),
                    supportedCodecsNow(),
                    null,
                    null,
                    null,
                    tooOld);
        }
        IndexFormatTooNewException tooNew = findCause(error, IndexFormatTooNewException.class);
        if (tooNew != null) {
            return new UnreadableShardException(
                    shardId,
                    shardPath,
                    Kind.UNSUPPORTED_FORMAT,
                    "unknown",
                    supportedCodecsNow(),
                    tooNew.getVersion(),
                    tooNew.getMinVersion(),
                    tooNew.getMaxVersion(),
                    tooNew);
        }
        CorruptIndexException corrupt = findCause(error, CorruptIndexException.class);
        if (corrupt != null) {
            return new UnreadableShardException(
                    shardId, shardPath, Kind.CORRUPT, "unknown", supportedCodecsNow(), null, null, null, corrupt);
        }
        return null;
    }

    private Path absoluteShardPath() {
        return shardPath.toAbsolutePath().normalize();
    }

    private static String actionableMessage(
            String shardId,
            Path shardPath,
            Kind kind,
            String foundCodec,
            String supportedCodecs,
            Integer formatVersion,
            Integer formatMin,
            Integer formatMax) {
        String problem = kind == Kind.CORRUPT
                ? "is corrupt and cannot be opened"
                : "uses a Lucene format this process cannot read";
        StringBuilder message = new StringBuilder();
        message.append("Shard ")
                .append(shardId)
                .append(" at ")
                .append(shardPath.toAbsolutePath().normalize())
                .append(' ')
                .append(problem)
                .append(". Found codec ")
                .append(foundCodec)
                .append("; this process supports ")
                .append(supportedCodecs);
        if (formatVersion != null) {
            message.append(". Found format version ")
                    .append(formatVersion)
                    .append(" (supported ")
                    .append(formatMin)
                    .append("..")
                    .append(formatMax)
                    .append(')');
        }
        message.append(". Remedy: ")
                .append(REMEDY)
                .append(". lucene-backward-codecs is not on the classpath; an unsupported or corrupt shard does not")
                .append(" become readable by retrying.");
        return message.toString();
    }

    private static String codecName(IndexFormatTooOldException error) {
        String reason = error.getReason();
        if (reason == null) {
            return "unknown";
        }
        Matcher matcher = CODEC_NAME.matcher(reason);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "unknown";
    }

    private static String supportedCodecsNow() {
        return Codec.availableCodecs().stream().sorted().collect(Collectors.joining(", ", "[", "]"));
    }

    private static <T extends Throwable> T findCause(Throwable error, Class<T> type) {
        Throwable current = error;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (type.isInstance(current)) {
                return type.cast(current);
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                return null;
            }
            current = cause;
        }
        return null;
    }
}
