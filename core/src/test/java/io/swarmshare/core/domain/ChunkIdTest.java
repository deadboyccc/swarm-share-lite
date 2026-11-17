package io.swarmshare.core.domain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link ChunkId} is the identity key for every chunk in the swarm, so its
 * validation and value semantics are load-bearing for every map and set keyed by it.
 */
class ChunkIdTest {

    @Test
    void validId_exposesComponents() {
        ChunkId id = new ChunkId("abc", 7);

        assertThat(id.manifestHash()).isEqualTo("abc");
        assertThat(id.index()).isEqualTo(7);
    }

    @Test
    void indexZero_isAllowed() {
        assertThat(new ChunkId("abc", 0).index()).isZero();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t\n"})
    void blankManifestHash_isRejected(String blank) {
        assertThatThrownBy(() -> new ChunkId(blank, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("manifestHash");
    }

    @Test
    void negativeIndex_isRejected() {
        assertThatThrownBy(() -> new ChunkId("abc", -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("index");
    }

    @Test
    void equality_isByValue() {
        assertThat(new ChunkId("abc", 1))
                .isEqualTo(new ChunkId("abc", 1))
                .hasSameHashCodeAs(new ChunkId("abc", 1));
    }

    @Test
    void sameIndexInDifferentManifests_areDifferentChunks() {
        assertThat(new ChunkId("manifest-a", 3)).isNotEqualTo(new ChunkId("manifest-b", 3));
    }
}
