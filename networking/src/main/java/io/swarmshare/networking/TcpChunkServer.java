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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * TCP listener that serves chunk and piece-map requests to remote peers.
 *
 * <p>
 * Each accepted connection is handled on a dedicated virtual thread — no
 * thread-pool
 * sizing required for blocking socket I/O under Project Loom. The server is
 * intentionally
 * simple: it reads a single request from the peer, writes a response, then
 * closes the
 * socket. This keeps the protocol stateless and the implementation
 * straightforward.
 */
public final class TcpChunkServer implements AutoCloseable {

    private static final Logger LOG = System.getLogger(TcpChunkServer.class.getName());

    /**
     * TCP port this server binds to on {@link #start()}.
     */
    private final int port;
    /**
     * Backing storage used to read chunk bytes for outgoing chunk responses.
     */
    private final StorageProvider storage;
    /**
     * Manifest describing the file being seeded (hash, chunk layout, total count).
     */
    private final Manifest manifest;
    /**
     * Bitmap of chunk indices this node currently holds, used to answer piece-map requests.
     */
    private final BitSet heldChunks;

    // Assigned once start() runs; volatile so close()/localPort() on other
    // threads see the up-to-date reference without extra synchronization.
    private volatile ServerSocket serverSocket;
    private volatile ExecutorService executor;
    /**
     * Set by {@link #close()} so a bind that finishes after close can discard the
     * socket instead of leaking a listener the CLI already treated as failed.
     */
    private volatile boolean closed;

    public TcpChunkServer(int port, StorageProvider storage, Manifest manifest, BitSet heldChunks) {
        this.port = port;
        this.storage = storage;
        this.manifest = manifest;
        this.heldChunks = heldChunks;
    }

    /**
     * Binds to {@code port} and accepts connections until interrupted or
     * {@link #close()} is called.
     */
    public void start() throws IOException {
        if (closed) {
            throw new IOException("Server already closed");
        }
        ExecutorService vtExecutor = Executors.newVirtualThreadPerTaskExecutor();
        ServerSocket socket;
        try {
            socket = new ServerSocket(port);
        } catch (IOException e) {
            vtExecutor.shutdownNow();
            throw e;
        }
        if (closed) {
            socket.close();
            vtExecutor.shutdownNow();
            throw new IOException("Server closed before bind completed");
        }
        executor = vtExecutor;
        serverSocket = socket;
        LOG.log(Level.INFO, "Listening on port {0}", serverSocket.getLocalPort());

        try {
            // Accept loop: runs on the calling thread; each accepted client is
            // dispatched to its own virtual thread so a slow/stalled client can't
            // block subsequent accepts.
            while (!Thread.currentThread().isInterrupted() && !serverSocket.isClosed()) {
                Socket client = serverSocket.accept();
                try {
                    // Each client handled on its own virtual thread to avoid blocking carrier
                    // threads
                    executor.submit(() -> handleClient(client));
                } catch (RejectedExecutionException e) {
                    client.close();
                }
            }
        } catch (IOException e) {
            // ServerSocket.accept() throws IOException when close() is called
            // concurrently from another thread; treat that as a normal shutdown
            // rather than an error, and only propagate genuine I/O failures.
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

    /**
     * Handles a single accepted connection end-to-end: reads one request,
     * dispatches it by message type, writes one response, then lets the
     * try-with-resources block close the socket and streams.
     */
    private void handleClient(Socket client) {
        // Try-with-resources ensures socket and streams are closed when the method
        // exits.
        try (client;
             var in = new DataInputStream(client.getInputStream());
             var out = new DataOutputStream(client.getOutputStream())) {

            // First byte indicates message type; delegate to specific handlers.
            byte msgType = in.readByte();
            switch (msgType) {
                case FrameEncoder.MSG_PIECE_MAP_REQUEST -> handlePieceMapRequest(in, out);
                case FrameEncoder.MSG_CHUNK_REQUEST -> handleChunkRequest(in, out);
                // Unknown message type: reply with an error status rather than
                // silently dropping the connection, so the peer isn't left hanging.
                default -> FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_ERROR, new byte[0]);
            }
        } catch (IOException e) {
            // Client may disconnect abruptly; log at DEBUG and move on.
            LOG.log(Level.DEBUG, "Client disconnected: {0}", e.getMessage());
        }
    }

    /**
     * Reads a CHUNK_REQUEST body, validates it against this server's manifest,
     * and writes back either the chunk bytes (STATUS_OK) or STATUS_NOT_FOUND.
     */
    private void handleChunkRequest(DataInputStream in, DataOutputStream out) throws IOException {
        var req = FrameDecoder.readChunkRequest(in);
        // Validate manifest hash first — quick rejection for wrong swarm
        if (!manifest.fileHash().equals(req.manifestHash())) {
            FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_NOT_FOUND, new byte[0]);
            return;
        }

        // Validate requested index bounds
        if (req.chunkIndex() < 0 || req.chunkIndex() >= manifest.totalChunks()) {
            FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_NOT_FOUND, new byte[0]);
            return;
        }

        synchronized (heldChunks) {
            if (!heldChunks.get(req.chunkIndex())) {
                FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_NOT_FOUND, new byte[0]);
                return;
            }
        }

        // Attempt to read the chunk from storage and reply with the bytes if present
        var desc = manifest.chunkAt(req.chunkIndex());
        var bytes = storage.readChunk(
                new ChunkId(req.manifestHash(), req.chunkIndex()),
                desc.offset(),
                desc.size());

        // storage.readChunk returns empty if the chunk isn't actually on disk yet
        // (e.g. not downloaded/verified), even though it passed index validation above.
        if (bytes.isPresent()) {
            FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_OK, bytes.get());
        } else {
            FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_NOT_FOUND, new byte[0]);
        }
    }

    /**
     * Reads a PIECE_MAP_REQUEST body and replies with this node's current
     * {@link #heldChunks} bitmap, serialized as bytes.
     */
    private void handlePieceMapRequest(DataInputStream in, DataOutputStream out) throws IOException {
        String requestedHash = FrameDecoder.readPieceMapRequest(in);
        // Return NOT_FOUND if this server's manifest does not match the request
        if (!manifest.fileHash().equals(requestedHash)) {
            FrameEncoder.writeChunkResponse(out, FrameEncoder.STATUS_NOT_FOUND, new byte[0]);
            return;
        }

        byte[] snapshot;
        synchronized (heldChunks) {
            snapshot = heldChunks.toByteArray();
        }
        FrameEncoder.writePieceMapResponse(out, snapshot);
    }

    /**
     * Stops accepting new connections and shuts down the virtual-thread executor.
     * Safe to call multiple times or before {@link #start()} has run.
     */
    @Override
    public void close() {
        closed = true;
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Failed to close server socket: {0}", e.getMessage());
            }
        }
        if (executor != null) {
            // shutdownNow() interrupts in-flight handleClient tasks; each one is
            // wrapped in try-with-resources so interruption still closes sockets cleanly.
            // Wait so try-with-resources can close StorageProvider afterwards without
            // racing FileChannel reads still in handleClient.
            executor.shutdownNow();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}