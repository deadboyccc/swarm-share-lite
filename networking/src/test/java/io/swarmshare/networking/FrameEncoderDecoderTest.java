package io.swarmshare.networking;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link FrameEncoder} and {@link FrameDecoder}.
 *
 * <p>
 * Tests verify correctness of binary framing: message type bytes,
 * length-prefixed strings, payload encoding/decoding, and error handling
 * for prematurely terminated streams.
 */
class FrameEncoderDecoderTest {

    private static final String MANIFEST_HASH = "deadbeef".repeat(8);

    @Test
    void chunkRequest_roundTripsThroughDecoder() throws Exception {
        var outBuf = new ByteArrayOutputStream();
        var out = new DataOutputStream(outBuf);
        FrameEncoder.writeChunkRequest(out, MANIFEST_HASH, 42);

        var in = new DataInputStream(new ByteArrayInputStream(outBuf.toByteArray()));
        assertThat(in.readByte()).isEqualTo(FrameEncoder.MSG_CHUNK_REQUEST);

        var parsed = FrameDecoder.readChunkRequest(in);
        assertThat(parsed.manifestHash()).isEqualTo(MANIFEST_HASH);
        assertThat(parsed.chunkIndex()).isEqualTo(42);
    }

    @Test
    void chunkResponse_roundTripsPayload() throws Exception {
        byte[] payload = "chunk-bytes".getBytes(StandardCharsets.UTF_8);
        var outBuf = new ByteArrayOutputStream();
        FrameEncoder.writeChunkResponse(new DataOutputStream(outBuf), FrameEncoder.STATUS_OK, payload);

        var in = new DataInputStream(new ByteArrayInputStream(outBuf.toByteArray()));
        assertThat(in.readByte()).isEqualTo(FrameEncoder.STATUS_OK);
        assertThat(in.readInt()).isEqualTo(payload.length);
        assertThat(FrameDecoder.readExactly(in, payload.length)).isEqualTo(payload);
    }

    @Test
    void pieceMapRequest_roundTripsHash() throws Exception {
        var outBuf = new ByteArrayOutputStream();
        FrameEncoder.writePieceMapRequest(new DataOutputStream(outBuf), MANIFEST_HASH);

        var in = new DataInputStream(new ByteArrayInputStream(outBuf.toByteArray()));
        assertThat(in.readByte()).isEqualTo(FrameEncoder.MSG_PIECE_MAP_REQUEST);
        assertThat(FrameDecoder.readPieceMapRequest(in)).isEqualTo(MANIFEST_HASH);
    }

    @Test
    void pieceMapResponse_roundTripsBitSet() throws Exception {
        BitSet original = new BitSet(16);
        original.set(0);
        original.set(3);
        original.set(15);
        byte[] bitSetBytes = original.toByteArray();

        var outBuf = new ByteArrayOutputStream();
        FrameEncoder.writePieceMapResponse(new DataOutputStream(outBuf), bitSetBytes);

        var in = new DataInputStream(new ByteArrayInputStream(outBuf.toByteArray()));
        assertThat(in.readByte()).isEqualTo(FrameEncoder.STATUS_OK);
        int len = in.readInt();
        BitSet restored = BitSet.valueOf(FrameDecoder.readExactly(in, len));
        assertThat(restored.get(0)).isTrue();
        assertThat(restored.get(3)).isTrue();
        assertThat(restored.get(15)).isTrue();
    }

    @Test
    void readExactly_throwsOnPrematureEof() {
        var in = new DataInputStream(new ByteArrayInputStream(new byte[] { 0x01, 0x02 }));
        assertThatThrownBy(() -> FrameDecoder.readExactly(in, 10))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Stream ended");
    }
}
