package io.swarmshare.networking;

import io.swarmshare.core.domain.PeerInfo;
import io.swarmshare.manifest.ManifestBuilder;
import io.swarmshare.storage.FileChannelStorage;
import io.swarmshare.transfer.TransferManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end transfer over loopback TCP: seeder serves, leecher downloads.
 */
class TwoNodeIntegrationTest {

    private TcpChunkServer server;
    private Thread serverThread;

    private static void waitForServerBound(TcpChunkServer server, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                server.localPort();
                return;
            } catch (IllegalStateException ignored) {
                Thread.sleep(50);
            }
        }
        throw new AssertionError("Server did not bind within " + timeoutMs + " ms");
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
        if (serverThread != null) {
            serverThread.interrupt();
        }
    }

    @Test
    void seederAndLeecher_transferFile(@TempDir Path tmpDir) throws Exception {
        Path srcFile = tmpDir.resolve("source.bin");
        Path outFile = tmpDir.resolve("output.bin");

        byte[] data = new byte[512_000];
        new Random(42).nextBytes(data);
        Files.write(srcFile, data);

        var manifest = new ManifestBuilder(128_000).build(srcFile);

        try (var seederStorage = new SeederFileStorage(srcFile)) {
            BitSet held = new BitSet(manifest.totalChunks());
            held.set(0, manifest.totalChunks());

            server = new TcpChunkServer(0, seederStorage, manifest, held);
            serverThread = Thread.startVirtualThread(() -> {
                try {
                    server.start();
                } catch (IOException ignored) {
                    // closed during tearDown
                }
            });

            waitForServerBound(server, 5_000);
            PeerInfo seederPeer = new PeerInfo(
                    UUID.randomUUID(), new InetSocketAddress("localhost", server.localPort()));

            var outStorage = new FileChannelStorage(outFile);
            var connector = new TcpPeerConnector();
            var manager = new TransferManager(manifest, List.of(seederPeer), outStorage, connector);
            manager.start();
            outStorage.close();

            assertThat(Files.readAllBytes(outFile)).isEqualTo(data);
        }
    }
}
