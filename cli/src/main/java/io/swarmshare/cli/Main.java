package io.swarmshare.cli;

import io.swarmshare.manifest.ManifestBuilder;
import io.swarmshare.core.domain.Manifest;
import io.swarmshare.networking.TcpChunkServer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.file.Path;
import java.util.BitSet;

/**
 * Minimal CLI entrypoint for development and manual testing.
 *
 * Commands:
 * seed <file> <port> - serve the given file on TCP port
 * build-manifest <file> - compute and print manifest summary
 */
public final class Main {

  private static final Logger LOG = System.getLogger(Main.class.getName());

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      printUsage();
      return;
    }

    switch (args[0]) {
      case "seed" -> runSeed(args);
      case "build-manifest" -> runBuildManifest(args);
      default -> printUsage();
    }
  }

  private static void runBuildManifest(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: build-manifest <file> [chunkSize]");
      return;
    }
    Path file = Path.of(args[1]);
    int chunkSize = (args.length >= 3) ? Integer.parseInt(args[2]) : ManifestBuilder.DEFAULT_CHUNK_SIZE;
    Manifest manifest = new ManifestBuilder(chunkSize).build(file);
    System.out.println("fileHash=" + manifest.fileHash());
    System.out.println("fileName=" + manifest.fileName());
    System.out.println("totalSize=" + manifest.totalSize());
    System.out.println("chunkSize=" + manifest.chunkSize());
    System.out.println("totalChunks=" + manifest.totalChunks());
  }

  private static void runSeed(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: seed <file> <port>");
      return;
    }
    Path file = Path.of(args[1]);
    int port = Integer.parseInt(args[2]);

    LOG.log(Level.INFO, "Building manifest for seeder...");
    Manifest manifest = new ManifestBuilder().build(file);

    SeederFileStorage storage = new SeederFileStorage(file);
    try {
      BitSet held = new BitSet(manifest.totalChunks());
      held.set(0, manifest.totalChunks());

      TcpChunkServer server = new TcpChunkServer(port, storage, manifest, held);

      Thread serverThread = Thread.startVirtualThread(() -> {
        try {
          server.start();
        } catch (java.io.IOException ignored) {
          // server closed
        }
      });

      // wait until server binds
      long deadline = System.currentTimeMillis() + 5_000;
      while (System.currentTimeMillis() < deadline) {
        try {
          int boundPort = server.localPort();
          LOG.log(Level.INFO, "Seeder running. fileHash={0} port={1}", manifest.fileHash(), boundPort);
          break;
        } catch (IllegalStateException ignored) {
          Thread.sleep(50);
        }
      }

      // block until server thread exits (server closed/interrupted)
      serverThread.join();
    } finally {
      storage.close();
    }
  }

  private static void printUsage() {
    System.out.println("Usage: <command> [args]\n");
    System.out.println("Commands:");
    System.out.println("  seed <file> <port>       Serve file to peers over TCP");
    System.out.println("  build-manifest <file> [chunkSize]   Compute and print manifest summary");
  }
}
