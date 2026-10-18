package io.swarmshare.core.domain;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ManifestTest {

    @Test
    void copiesChunkListSoCallersCannotMutate() {
        List<ChunkDescriptor> chunks = new ArrayList<>();
        chunks.add(new ChunkDescriptor(new ChunkId("h".repeat(1), 0), 0L, 4, "s"));
        Manifest manifest = new Manifest("h", "f.bin", 4L, 4, chunks);

        chunks.clear();

        assertThat(manifest.totalChunks()).isEqualTo(1);
        assertThat(manifest.chunkAt(0).size()).isEqualTo(4);
        assertThatThrownBy(() -> manifest.chunks().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
