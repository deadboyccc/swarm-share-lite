// manifest/ManifestSerializerTest.java
package io.swarmshare.manifest;

import io.swarmshare.core.domain.ChunkDescriptor;

/**
 * Unit tests for {@link ManifestSerializer}.
 *
 * <p>Tests verify JSON round-trip serialization and deserialization,
 * ensuring the wire contract is stable and manifests can be reliably
 * exchanged between seeder and peers.
 */
import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ManifestSerializer}.
 *
 * <p>
 * Coverage targets:
 * <ul>
 * <li>File round-trip — write then read produces field-identical manifest</li>
 * <li>String round-trip — toJson → fromJson preserves all fields</li>
 * <li>JSON shape — expected field names present in the serialized output</li>
 * <li>Multi-chunk — all chunk descriptors serialized and restored</li>
 * <li>Rejection — missing or unreadable files</li>
 * </ul>
 */
class ManifestSerializerTest {

    private final ManifestSerializer serializer = new ManifestSerializer();
    @TempDir
    Path tempDir;

    // ── helpers ──────────────────────────────────────────────────────────────────

    /**
     * Builds a deterministic single-chunk manifest for use across tests.
     */
    private static Manifest singleChunkManifest() {
        String fileHash = "abc123def456abc123def456abc123def456abc123def456abc123def456abcd";
        ChunkId id = new ChunkId(fileHash, 0);
        ChunkDescriptor chunk = new ChunkDescriptor(id, 0L, 1024, "cafebabe" + "0".repeat(56));
        return new Manifest(fileHash, "ubuntu.iso", 1024L, 1024, List.of(chunk));
    }

    /**
     * Builds a deterministic two-chunk manifest for multi-chunk tests.
     */
    private static Manifest twoChunkManifest() {
        String fileHash = "deadbeef" + "0".repeat(56);
        ChunkId id0 = new ChunkId(fileHash, 0);
        ChunkId id1 = new ChunkId(fileHash, 1);
        ChunkDescriptor c0 = new ChunkDescriptor(id0, 0L, 512, "1111" + "0".repeat(60));
        ChunkDescriptor c1 = new ChunkDescriptor(id1, 512L, 512, "2222" + "0".repeat(60));
        return new Manifest(fileHash, "archive.tar.gz", 1024L, 512, List.of(c0, c1));
    }

    // ── file round-trip ──────────────────────────────────────────────────────────

    @Test
    void writeAndRead_singleChunk_roundTripPreservesAllFields() {
        Path jsonFile = tempDir.resolve("manifest.json");
        Manifest original = singleChunkManifest();

        serializer.write(original, jsonFile);
        Manifest restored = serializer.read(jsonFile);

        assertThat(restored.fileHash()).isEqualTo(original.fileHash());
        assertThat(restored.fileName()).isEqualTo(original.fileName());
        assertThat(restored.totalSize()).isEqualTo(original.totalSize());
        assertThat(restored.chunkSize()).isEqualTo(original.chunkSize());
        assertThat(restored.totalChunks()).isEqualTo(original.totalChunks());
    }

    @Test
    void writeAndRead_twoChunks_allChunkDescriptorsRestored() {
        Path jsonFile = tempDir.resolve("manifest2.json");
        Manifest original = twoChunkManifest();

        serializer.write(original, jsonFile);
        Manifest restored = serializer.read(jsonFile);

        assertThat(restored.totalChunks()).isEqualTo(2);

        ChunkDescriptor r0 = restored.chunks().get(0);
        assertThat(r0.id().index()).isEqualTo(0);
        assertThat(r0.id().manifestHash()).isEqualTo(original.fileHash());
        assertThat(r0.offset()).isEqualTo(0L);
        assertThat(r0.size()).isEqualTo(512);
        assertThat(r0.sha256()).isEqualTo(original.chunks().get(0).sha256());

        ChunkDescriptor r1 = restored.chunks().get(1);
        assertThat(r1.id().index()).isEqualTo(1);
        assertThat(r1.offset()).isEqualTo(512L);
        assertThat(r1.size()).isEqualTo(512);
    }

    @Test
    void write_createsANonEmptyJsonFile() throws IOException {
        Path jsonFile = tempDir.resolve("nonempty.json");
        serializer.write(singleChunkManifest(), jsonFile);

        assertThat(jsonFile).exists();
        assertThat(Files.size(jsonFile)).isGreaterThan(0);
    }

    @Test
    void write_outputIsParseable_asValidJsonText() throws IOException {
        Path jsonFile = tempDir.resolve("valid-json.json");
        serializer.write(singleChunkManifest(), jsonFile);

        String content = Files.readString(jsonFile);
        // A minimal validity check: well-formed JSON starts with '{' and ends with '}'
        assertThat(content.trim()).startsWith("{").endsWith("}");
    }

    // ── string round-trip ────────────────────────────────────────────────────────

    @Test
    void toJsonAndFromJson_singleChunk_roundTripPreservesAllFields() {
        Manifest original = singleChunkManifest();

        String json = serializer.toJson(original);
        Manifest restored = serializer.fromJson(json);

        assertThat(restored.fileHash()).isEqualTo(original.fileHash());
        assertThat(restored.fileName()).isEqualTo(original.fileName());
        assertThat(restored.totalSize()).isEqualTo(original.totalSize());
        assertThat(restored.chunkSize()).isEqualTo(original.chunkSize());
        assertThat(restored.totalChunks()).isEqualTo(original.totalChunks());
    }

    @Test
    void toJsonAndFromJson_chunkChecksums_preserved() {
        Manifest original = twoChunkManifest();

        Manifest restored = serializer.fromJson(serializer.toJson(original));

        for (int i = 0; i < original.totalChunks(); i++) {
            assertThat(restored.chunks().get(i).sha256())
                    .as("sha256 of chunk %d", i)
                    .isEqualTo(original.chunks().get(i).sha256());
        }
    }

    @Test
    void toJsonAndFromJson_chunkIdManifestHash_matchesFileHash() {
        Manifest original = singleChunkManifest();

        Manifest restored = serializer.fromJson(serializer.toJson(original));

        restored.chunks().forEach(desc -> assertThat(desc.id().manifestHash())
                .isEqualTo(restored.fileHash()));
    }

    // ── JSON shape
    // ────────────────────────────────────────────────────────────────

    @Test
    void toJson_containsExpectedTopLevelFieldNames() {
        String json = serializer.toJson(singleChunkManifest());

        assertThat(json).contains("\"fileHash\"");
        assertThat(json).contains("\"fileName\"");
        assertThat(json).contains("\"totalSize\"");
        assertThat(json).contains("\"chunkSize\"");
        assertThat(json).contains("\"chunks\"");
    }

    @Test
    void toJson_containsExpectedChunkFieldNames() {
        String json = serializer.toJson(singleChunkManifest());

        assertThat(json).contains("\"index\"");
        assertThat(json).contains("\"offset\"");
        assertThat(json).contains("\"size\"");
        assertThat(json).contains("\"sha256\"");
    }

    @Test
    void toJson_fileHashValue_isEmbeddedInOutput() {
        Manifest manifest = singleChunkManifest();
        String json = serializer.toJson(manifest);

        assertThat(json).contains(manifest.fileHash());
    }

    // ── integration with ManifestBuilder ─────────────────────────────────────────

    @Test
    void roundTrip_builderThenSerializerThenRead_preservesAllData(@TempDir Path dir)
            throws IOException {
        // Write a real 200-byte binary file
        byte[] content = new byte[200];
        for (int i = 0; i < content.length; i++)
            content[i] = (byte) i;
        Path sourceFile = dir.resolve("source.bin");
        Files.write(sourceFile, content);

        // Build a real manifest
        Manifest built = new ManifestBuilder(64).build(sourceFile);

        // Serialize to disk
        Path jsonFile = dir.resolve("manifest.json");
        serializer.write(built, jsonFile);

        // Deserialize
        Manifest restored = serializer.read(jsonFile);

        // Verify all top-level fields
        assertThat(restored.fileHash()).isEqualTo(built.fileHash());
        assertThat(restored.fileName()).isEqualTo(built.fileName());
        assertThat(restored.totalSize()).isEqualTo(built.totalSize());
        assertThat(restored.chunkSize()).isEqualTo(built.chunkSize());
        assertThat(restored.totalChunks()).isEqualTo(built.totalChunks());

        // Verify every chunk descriptor
        for (int i = 0; i < built.totalChunks(); i++) {
            ChunkDescriptor b = built.chunks().get(i);
            ChunkDescriptor r = restored.chunks().get(i);
            assertThat(r.id().index()).isEqualTo(b.id().index());
            assertThat(r.id().manifestHash()).isEqualTo(b.id().manifestHash());
            assertThat(r.offset()).isEqualTo(b.offset());
            assertThat(r.size()).isEqualTo(b.size());
            assertThat(r.sha256()).isEqualTo(b.sha256());
        }
    }

    // ── error cases
    // ───────────────────────────────────────────────────────────────

    @Test
    void read_nonExistentFile_throwsUncheckedIOException() {
        Path missing = tempDir.resolve("does-not-exist.json");

        assertThatThrownBy(() -> serializer.read(missing))
                .isInstanceOf(java.io.UncheckedIOException.class);
    }

    @Test
    void fromJson_invalidJson_throwsUncheckedIOException() {
        assertThatThrownBy(() -> serializer.fromJson("not-valid-json{{{{"))
                .isInstanceOf(java.io.UncheckedIOException.class);
    }

    @Test
    void fromJson_emptyString_throwsUncheckedIOException() {
        assertThatThrownBy(() -> serializer.fromJson(""))
                .isInstanceOf(java.io.UncheckedIOException.class);
    }
}