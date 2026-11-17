package io.swarmshare.networking;

import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.PeerInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.BitSet;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Client-side protocol tests: a scripted single-connection peer replies with
 * well-formed, malformed, or truncated frames.
 */
@Timeout(20)
class TcpPeerConnectorTest {

    private static final ChunkId CHUNK = new ChunkId("deadbeef", 3);

    private final TcpPeerConnector connector = new TcpPeerConnector();
    private ServerSocket server;

    @AfterEach
    void tearDown() throws IOException {
        connector.close();
        if (server != null) {
            server.close();
        }
    }

    @Test
    void fetchChunk_sendsRequestFrameAndReturnsPayload() throws Exception {
        AtomicReference<FrameDecoder.ParsedChunkRequest> seen = new AtomicReference<>();
        PeerInfo peer = scriptedPeer((in, out) -> {
            assertThat(in.readByte()).isEqualTo(FrameEncoder.MSG_CHUNK_REQUEST);
            seen.set(FrameDecoder.readChunkRequest(in));
            FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_OK, new byte[]{1, 2, 3});
        });

        byte[] data = connector.fetchChunkAsync(peer, CHUNK, 3).join();

        assertThat(data).containsExactly(1, 2, 3);
        assertThat(seen.get()).isEqualTo(new FrameDecoder.ParsedChunkRequest("deadbeef", 3));
    }

    @Test
    void fetchChunk_notFoundStatus_failsWithPeerAddress() throws Exception {
        PeerInfo peer = scriptedPeer((in, out) -> {
            drainChunkRequest(in);
            FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_NOT_FOUND, new byte[0]);
        });

        assertThatThrownBy(() -> connector.fetchChunkAsync(peer, CHUNK, 10).join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("Chunk fetch failed")
                .hasMessageContaining(peer.displayAddress())
                .hasMessageContaining("status 0x01");
    }

    @Test
    void fetchChunk_lengthDifferentFromManifest_isRejected() throws Exception {
        PeerInfo peer = scriptedPeer((in, out) -> {
            drainChunkRequest(in);
            FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_OK, new byte[4]);
        });

        assertThatThrownBy(() -> connector.fetchChunkAsync(peer, CHUNK, 10).join())
                .hasMessageContaining("Expected 10 bytes, peer sent 4");
    }

    @Test
    void fetchChunk_negativeLength_isRejected() throws Exception {
        PeerInfo peer = scriptedPeer((in, out) -> {
            drainChunkRequest(in);
            out.writeByte(FrameEncoder.STATUS_OK);
            out.writeInt(-1);
            out.flush();
        });

        assertThatThrownBy(() -> connector.fetchChunkAsync(peer, CHUNK, 10).join())
                .hasMessageContaining("negative payload length");
    }

    @Test
    void fetchChunk_peerDropsConnectionMidPayload_fails() throws Exception {
        PeerInfo peer = scriptedPeer((in, out) -> {
            drainChunkRequest(in);
            out.writeByte(FrameEncoder.STATUS_OK);
            out.writeInt(10);
            out.write(new byte[4]); // then hang up
            out.flush();
        });

        assertThatThrownBy(() -> connector.fetchChunkAsync(peer, CHUNK, 10).join())
                .hasMessageContaining("Stream ended before 10 bytes read");
    }

    @Test
    void fetchChunk_connectionRefused_failsExceptionally() throws Exception {
        int closedPort;
        try (var probe = new ServerSocket(0)) {
            closedPort = probe.getLocalPort();
        }
        PeerInfo peer = new PeerInfo(UUID.randomUUID(), new InetSocketAddress("localhost", closedPort));

        assertThatThrownBy(() -> connector.fetchChunkAsync(peer, CHUNK, 10).join())
                .isInstanceOf(CompletionException.class)
                .hasMessageContaining("Chunk fetch failed");
    }

    @Test
    void fetchPieceMap_returnsBitSet() throws Exception {
        BitSet held = new BitSet();
        held.set(1);
        held.set(5);
        PeerInfo peer = scriptedPeer((in, out) -> {
            assertThat(in.readByte()).isEqualTo(FrameEncoder.MSG_PIECE_MAP_REQUEST);
            assertThat(FrameDecoder.readPieceMapRequest(in)).isEqualTo("deadbeef");
            FrameEncoder.writePieceMapResponse(out, held.toByteArray());
        });

        assertThat(connector.fetchPieceMapAsync(peer, "deadbeef").join()).isEqualTo(held);
    }

    @Test
    void fetchPieceMap_emptyBitSet_isValid() throws Exception {
        PeerInfo peer = scriptedPeer((in, out) -> {
            in.readByte();
            FrameDecoder.readPieceMapRequest(in);
            FrameEncoder.writePieceMapResponse(out, new byte[0]);
        });

        assertThat(connector.fetchPieceMapAsync(peer, "deadbeef").join().isEmpty()).isTrue();
    }

    @Test
    void fetchPieceMap_notFound_failsWithPeerAddress() throws Exception {
        PeerInfo peer = scriptedPeer((in, out) -> {
            in.readByte();
            FrameDecoder.readPieceMapRequest(in);
            FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_NOT_FOUND, new byte[0]);
        });

        assertThatThrownBy(() -> connector.fetchPieceMapAsync(peer, "deadbeef").join())
                .hasMessageContaining("PieceMap fetch failed for peer " + peer.displayAddress());
    }

    @Test
    void fetchPieceMap_hostileLength_isRejectedBeforeAllocating() throws Exception {
        PeerInfo peer = scriptedPeer((in, out) -> {
            in.readByte();
            FrameDecoder.readPieceMapRequest(in);
            out.writeByte(FrameEncoder.STATUS_OK);
            out.writeInt(Integer.MAX_VALUE);
            out.flush();
        });

        assertThatThrownBy(() -> connector.fetchPieceMapAsync(peer, "deadbeef").join())
                .hasMessageContaining("Invalid piece-map length");
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    @FunctionalInterface
    private interface Script {
        void serve(DataInputStream in, DataOutputStream out) throws IOException;
    }

    /** Starts a peer that accepts exactly one connection and runs {@code script} on it. */
    private PeerInfo scriptedPeer(Script script) throws IOException {
        server = new ServerSocket(0);
        Thread.startVirtualThread(() -> {
            try (Socket socket = server.accept()) {
                script.serve(new DataInputStream(socket.getInputStream()),
                        new DataOutputStream(socket.getOutputStream()));
            } catch (IOException | AssertionError ignored) {
                // the client-side assertion reports the failure
            }
        });
        return new PeerInfo(UUID.randomUUID(), new InetSocketAddress("localhost", server.getLocalPort()));
    }

    private static void drainChunkRequest(DataInputStream in) throws IOException {
        in.readByte();
        FrameDecoder.readChunkRequest(in);
    }
}
