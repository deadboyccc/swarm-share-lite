package io.swarmshare.core.domain;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The manifest is the swarm's shared contract; these tests pin down its
 * immutability and index-based lookup.
 */
class ManifestTest {

    private static final String HASH = "abc";

    private static ChunkDescriptor chunk(int index, long offset, int size) {
        return new ChunkDescriptor(new ChunkId(HASH, index), offset, size, "ff");
    }

    @Test
    void totalChunks_countsDescriptors() {
        Manifest manifest = new Manifest(HASH, "f.bin", 15, 10,
                List.of(chunk(0, 0, 10), chunk(1, 10, 5)));

        assertThat(manifest.totalChunks()).isEqualTo(2);
    }

    @Test
    void chunkAt_returnsDescriptorByIndex() {
        ChunkDescriptor second = chunk(1, 10, 5);
        Manifest manifest = new Manifest(HASH, "f.bin", 15, 10, List.of(chunk(0, 0, 10), second));

        assertThat(manifest.chunkAt(1)).isSameAs(second);
    }

    @Test
    void chunkAt_outOfRange_throws() {
        Manifest manifest = new Manifest(HASH, "f.bin", 10, 10, List.of(chunk(0, 0, 10)));

        assertThatThrownBy(() -> manifest.chunkAt(1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void chunks_areDefensivelyCopied() {
        List<ChunkDescriptor> source = new ArrayList<>(List.of(chunk(0, 0, 10)));
        Manifest manifest = new Manifest(HASH, "f.bin", 10, 10, source);

        source.add(chunk(1, 10, 10));

        assertThat(manifest.totalChunks()).isEqualTo(1);
    }

    @Test
    void chunks_areUnmodifiable() {
        Manifest manifest = new Manifest(HASH, "f.bin", 10, 10, List.of(chunk(0, 0, 10)));

        assertThatThrownBy(() -> manifest.chunks().add(chunk(1, 10, 10)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void emptyChunkList_isRepresentable_soValidatorsCanReportIt() {
        Manifest manifest = new Manifest(HASH, "f.bin", 0, 10, List.of());

        assertThat(manifest.totalChunks()).isZero();
    }

    @Test
    void equality_isByValue() {
        Manifest a = new Manifest(HASH, "f.bin", 10, 10, List.of(chunk(0, 0, 10)));
        Manifest b = new Manifest(HASH, "f.bin", 10, 10, List.of(chunk(0, 0, 10)));

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
    }
}
