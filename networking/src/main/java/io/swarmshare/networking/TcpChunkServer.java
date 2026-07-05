package io.swarmshare.networking;

import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;
import io.swarmshare.core.port.StorageProvider;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.BitSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TCP listener that serves chunk and piece-map requests to remote peers.
 *
 * <p>Each accepted connection is handled on a dedicated virtual thread — no thread-pool
 * sizing required for blocking socket I/O under Project Loom.
 */
public final class TcpChunkServer implements AutoCloseable {

    private static final Logger LOG = System.getLogger(TcpChunkServer.class.getName());

    private final int port;
    private final StorageProvider storage;
    private final Manifest manifest;
    private final BitSet heldChunks;

    private volatile ServerSocket serverSocket;
    private volatile ExecutorService executor;

    public TcpChunkServer(int port, StorageProvider storage, Manifest manifest, BitSet heldChunks) {
        this.port = port;
        this.storage = storage;
        this.manifest = manifest;
        this.heldChunks = heldChunks;
    }

    /**
     * Binds to {@code port} and accepts connections until interrupted or {@link #close()} is called.
     */
    public void start() throws IOException {
        executor = Executors.newVirtualThreadPerTaskExecutor();
        serverSocket = new ServerSocket(port);
        LOG.log(Level.INFO, "Listening on port {0}", serverSocket.getLocalPort());

        try {
            while (!Thread.currentThread().isInterrupted() && !serverSocket.isClosed()) {
                Socket client = serverSocket.accept();
                executor.submit(() -> handleClient(client));
            }
        } catch (IOException e) {
            if (!serverSocket.isClosed()) {
                throw e;
            }
        }
    }

    /**
     * Returns the bound local port after {@link #start()} has opened the socket.
     */
    public int localPort() {
        ServerSocket ss = serverSocket;
        if (ss == null || !ss.isBound()) {
            throw new IllegalStateException("Server socket is not bound yet");
        }
        return ss.getLocalPort();
    }

    private void handleClient(Socket client) {
        try (client;
             var in = new DataInputStream(client.getInputStream());
             var out = new DataOutputStream(client.getOutputStream())) {

            byte msgType = in.readByte();
            switch (msgType) {
                case FrameEncoder.MSG_PIECE_MAP_REQUEST -> handlePieceMapRequest(in, out);
                case FrameEncoder.MSG_CHUNK_REQUEST -> handleChunkRequest(in, out);
                default -> FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_ERROR, new byte[0]);
            }
        } catch (IOException e) {
            LOG.log(Level.DEBUG, "Client disconnected: {0}", e.getMessage());
        }
    }

    private void handleChunkRequest(DataInputStream in, DataOutputStream out) throws IOException {
        var req = FrameDecoder.readChunkRequest(in);

        if (!manifest.fileHash().equals(req.manifestHash())) {
            FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_NOT_FOUND, new byte[0]);
            return;
        }

        if (req.chunkIndex() < 0 || req.chunkIndex() >= manifest.totalChunks()) {
            FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_NOT_FOUND, new byte[0]);
            return;
        }

        var desc = manifest.chunkAt(req.chunkIndex());
        var bytes = storage.readChunk(
                new ChunkId(req.manifestHash(), req.chunkIndex()),
                desc.offset(),
                desc.size());

        if (bytes.isPresent()) {
            FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_OK, bytes.get());
        } else {
            FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_NOT_FOUND, new byte[0]);
        }
    }

    private void handlePieceMapRequest(DataInputStream in, DataOutputStream out) throws IOException {
        String requestedHash = FrameDecoder.readPieceMapRequest(in);
        if (!manifest.fileHash().equals(requestedHash)) {
            FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_NOT_FOUND, new byte[0]);
            return;
        }
        synchronized (heldChunks) {
            FrameEncoder.writePieceMapResponse(out, heldChunks.toByteArray());
        }
    }

    @Override
    public void close() {
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Failed to close server socket: {0}", e.getMessage());
            }
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
