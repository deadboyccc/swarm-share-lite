package io.swarmshare.networking;

import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.PeerInfo;
import io.swarmshare.core.port.PeerConnector;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * {@link PeerConnector} implementation over plain TCP binary framing.
 *
 * <p>
 * Opens a fresh connection per request — simple and stateless. Connection
 * pooling
 * is a future optimisation. Each fetch method opens a socket, exchanges a
 * single
 * request/response pair, and then closes the socket. Timeouts are applied to
 * both
 * connect and read operations to avoid indefinite blocking.
 */
public final class TcpPeerConnector implements PeerConnector {

    private static final int TIMEOUT_MS = 10_000;
    private static final Executor VIRTUAL_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    @Override
    public CompletableFuture<byte[]> fetchChunkAsync(PeerInfo peer, ChunkId id, int size) {
        return CompletableFuture.supplyAsync(() -> {
            try (Socket socket = new Socket()) {
                socket.connect(peer.address(), TIMEOUT_MS);
                socket.setSoTimeout(TIMEOUT_MS);
                // Set up Data streams for simple length-prefixed framing
                var out = new DataOutputStream(socket.getOutputStream());
                var in = new DataInputStream(socket.getInputStream());

                // Send chunk request and read response header
                FrameEncoder.writeChunkRequest(out, id.manifestHash(), id.index());

                byte status = in.readByte();
                int dataLen = in.readInt();

                if (status != FrameEncoder.STATUS_OK) {
                    throw new IOException("Peer returned status 0x%02x for chunk %d"
                            .formatted(status, id.index()));
                }
                if (dataLen != size) {
                    throw new IOException("Expected %d bytes, peer sent %d".formatted(size, dataLen));
                }

                // Read exactly the number of bytes advertised by the peer
                return FrameDecoder.readExactly(in, dataLen);
            } catch (IOException e) {
                throw new RuntimeException("Chunk fetch failed: " + id, e);
            }
        }, VIRTUAL_EXECUTOR);
    }

    @Override
    public CompletableFuture<BitSet> fetchPieceMapAsync(PeerInfo peer, String manifestHash) {
        return CompletableFuture.supplyAsync(() -> {
            try (Socket socket = new Socket()) {
                socket.connect(peer.address(), TIMEOUT_MS);
                socket.setSoTimeout(TIMEOUT_MS);
                var out = new DataOutputStream(socket.getOutputStream());
                var in = new DataInputStream(socket.getInputStream());

                // Send request for piece map and parse the length-prefixed response
                FrameEncoder.writePieceMapRequest(out, manifestHash);

                byte status = in.readByte();
                int dataLen = in.readInt();

                if (status != FrameEncoder.STATUS_OK) {
                    throw new IOException("PieceMap request failed with status 0x%02x".formatted(status));
                }

                byte[] bitSetBytes = FrameDecoder.readExactly(in, dataLen);
                return BitSet.valueOf(bitSetBytes);
            } catch (IOException e) {
                throw new RuntimeException("PieceMap fetch failed for peer: " + peer.id(), e);
            }
        }, VIRTUAL_EXECUTOR);
    }
}
