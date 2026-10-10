package com.danieljhkim.dsearch.indexnode.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.LockObtainFailedException;
import org.junit.jupiter.api.Test;

class UnreadableShardExceptionTest {

    @Test
    void tooNewAndCorruptFailuresArePermanentWhileLockAndIoFailuresAreNot() {
        Path shardPath = Path.of("indexes", "shard-4");
        IndexFormatTooNewException tooNew = new IndexFormatTooNewException("segments_2", 99, 1, 10);
        UnreadableShardException translated = UnreadableShardException.translate("4", shardPath, tooNew);

        assertEquals(UnreadableShardException.Kind.UNSUPPORTED_FORMAT, translated.kind());
        assertEquals("unknown", translated.foundCodec());
        assertTrue(translated.readinessReason().startsWith("index_format_unsupported:shard-4"));
        assertTrue(translated.readinessReason().contains("formatVersion=99"));
        assertTrue(translated.readinessReason().contains("formatMin=1"));
        assertTrue(translated.readinessReason().contains("formatMax=10"));
        assertTrue(translated
                .readinessReason()
                .contains(shardPath.toAbsolutePath().normalize().toString()));
        assertTrue(translated.getMessage().contains("format version 99"));
        assertRemedy(translated.readinessReason());
        assertRemedy(translated.getMessage());

        CorruptIndexException corrupt = new CorruptIndexException("checksum failed", "segments_2");
        UnreadableShardException corruptFailure =
                UnreadableShardException.translate("4", shardPath, new IOException("open", corrupt));
        assertEquals(UnreadableShardException.Kind.CORRUPT, corruptFailure.kind());
        assertTrue(corruptFailure.readinessReason().startsWith("index_corrupt:shard-4"));
        assertEquals("unknown", corruptFailure.foundCodec());
        assertEquals(corruptFailure, UnreadableShardException.classify(corruptFailure, "other", shardPath));

        IndexFormatTooOldException withoutCodec = new IndexFormatTooOldException("segments_2", "older than dirt");
        assertEquals(
                "unknown",
                UnreadableShardException.translate("4", shardPath, withoutCodec).foundCodec());

        assertNull(
                UnreadableShardException.translate("4", shardPath, new LockObtainFailedException("write.lock held")));
        assertNull(UnreadableShardException.translate("4", shardPath, new IOException("disk temporarily unavailable")));
        assertNull(UnreadableShardException.classify(
                new RuntimeException("disk temporarily unavailable"), "4", shardPath));
        for (String codec : Codec.availableCodecs()) {
            assertTrue(translated.supportedCodecs().contains(codec));
            assertTrue(translated.readinessReason().contains(codec));
        }
    }

    private static void assertRemedy(String text) {
        assertTrue(text.contains("reindex through the alias workflow"));
        assertTrue(text.contains("docs/RECOVERY.md"));
        assertTrue(text.contains("move the local data directory aside"));
        assertTrue(text.contains("supported=") || text.contains("this process supports"));
    }
}
