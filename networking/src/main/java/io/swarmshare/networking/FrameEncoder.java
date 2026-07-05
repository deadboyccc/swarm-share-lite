package io.swarmshare.networking;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Serializes swarm wire-protocol frames to a {@link DataOutputStream}.
 *
 * <p>
 * All multi-byte integers use big-endian byte order, matching
 * {@link java.io.DataInputStream} / {@link java.io.DataOutputStream} defaults.
 * The helpers are minimal and intentionally explicit about lengths to avoid
 * ambiguity when reading from a raw TCP stream.
 */
public final class FrameEncoder {

    public static final byte MSG_PIECE_MAP_REQUEST = 0x01;
    public static final byte MSG_PIECE_MAP_RESPONSE = 0x02;
    public static final byte MSG_CHUNK_REQUEST = 0x03;
    public static final byte MSG_CHUNK_RESPONSE = 0x04;

    public static final byte STATUS_OK = 0x00;
    public static final byte STATUS_NOT_FOUND = 0x01;
    public static final byte STATUS_ERROR = 0x02;

    private FrameEncoder() {
    }

    public static void writeChunkRequest(DataOutputStream out,
            String manifestHash, int chunkIndex)
            throws IOException {
        // Format: [MSG_CHUNK_REQUEST][hashLen:4][hashBytes][chunkIndex:4]
        byte[] hashBytes = manifestHash.getBytes(StandardCharsets.UTF_8);
        out.writeByte(MSG_CHUNK_REQUEST);
        out.writeInt(hashBytes.length);
        out.write(hashBytes);
        out.writeInt(chunkIndex);
        out.flush();
    }

    /**
     * Response layout: status (1) | payload length (4 BE) | payload (N).
     * No message-type prefix — the request/response pairing is implicit on a TCP
     * stream.
     */
    public static void writeChunkResponse(DataOutputStream out, byte status, byte[] payload)
            throws IOException {
        // Response layout: status (1) | payload length (4 BE) | payload (N).
        out.writeByte(status);
        out.writeInt(payload.length);
        if (payload.length > 0) {
            out.write(payload);
        }
        out.flush();
    }

    public static void writePieceMapRequest(DataOutputStream out, String manifestHash)
            throws IOException {
        // Format: [MSG_PIECE_MAP_REQUEST][hashLen:4][hashBytes]
        byte[] hashBytes = manifestHash.getBytes(StandardCharsets.UTF_8);
        out.writeByte(MSG_PIECE_MAP_REQUEST);
        out.writeInt(hashBytes.length);
        out.write(hashBytes);
        out.flush();
    }

    public static void writePieceMapResponse(DataOutputStream out, byte[] bitSetBytes)
            throws IOException {
        // Reply with OK status, then length-prefixed bitset bytes
        out.writeByte(STATUS_OK);
        out.writeInt(bitSetBytes.length);
        if (bitSetBytes.length > 0) {
            out.write(bitSetBytes);
        }
        out.flush();
    }
}
