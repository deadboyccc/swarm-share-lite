package io.swarmshare.core.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChunkIdTest {

    @Test
    void rejectsBlankManifestHash() {
        assertThatThrownBy(() -> new ChunkId(" ", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("manifestHash");
    }

    @Test
    void rejectsNegativeIndex() {
        assertThatThrownBy(() -> new ChunkId("abc", -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("index");
    }

    @Test
    void equalityDependsOnHashAndIndex() {
        assertThat(new ChunkId("abc", 0)).isEqualTo(new ChunkId("abc", 0));
        assertThat(new ChunkId("abc", 0)).isNotEqualTo(new ChunkId("abc", 1));
        assertThat(new ChunkId("abc", 0)).isNotEqualTo(new ChunkId("xyz", 0));
    }
}
