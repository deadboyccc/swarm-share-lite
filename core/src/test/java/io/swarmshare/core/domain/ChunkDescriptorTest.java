package io.swarmshare.core.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChunkDescriptorTest {

    private static final ChunkId ID = new ChunkId("abc", 0);

    @Test
    void validDescriptor_exposesComponents() {
        ChunkDescriptor desc = new ChunkDescriptor(ID, 1024L, 512, "ff");

        assertThat(desc.id()).isEqualTo(ID);
        assertThat(desc.offset()).isEqualTo(1024L);
        assertThat(desc.size()).isEqualTo(512);
        assertThat(desc.sha256()).isEqualTo("ff");
    }

    @Test
    void offsetZero_isAllowed() {
        assertThat(new ChunkDescriptor(ID, 0L, 1, "ff").offset()).isZero();
    }

    @Test
    void negativeOffset_isRejected() {
        assertThatThrownBy(() -> new ChunkDescriptor(ID, -1L, 1, "ff"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("offset");
    }

    @Test
    void zeroSize_isRejected() {
        assertThatThrownBy(() -> new ChunkDescriptor(ID, 0L, 0, "ff"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size");
    }

    @Test
    void negativeSize_isRejected() {
        assertThatThrownBy(() -> new ChunkDescriptor(ID, 0L, -5, "ff"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size");
    }
}
