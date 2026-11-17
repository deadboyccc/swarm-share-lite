package io.swarmshare.cli;

import io.swarmshare.core.domain.Manifest;
import io.swarmshare.core.domain.PeerInfo;
import io.swarmshare.manifest.ManifestBuilder;
import io.swarmshare.manifest.ManifestSerializer;
import io.swarmshare.manifest.ManifestValidator;
import io.swarmshare.networking.TcpChunkServer;
import io.swarmshare.networking.TcpPeerConnector;
import io.swarmshare.storage.FileChannelStorage;
import io.swarmshare.transfer.TransferManager;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * CLI entrypoint for development and manual testing, built on picocli.
 *
 * <p>
 * Commands:
 * <ul>
 * <li>{@code seed <file> <port>} — serve the given file to peers over TCP,
 * writing a manifest JSON file alongside it</li>
 * <li>{@code build-manifest <file> [chunkSize]} — compute, print, and persist
 * a manifest without serving the file</li>
 * <li>{@code download <manifestFile> <output> <peerHost:peerPort>...} — fetch
 * a manifest's chunks from one or more peers and write them to disk</li>
 * </ul>
 */
@Command(name = "swarm-share", description = "P2P chunk-based file distribution CLI", subcommands = {
        Main.SeedCommand.class,
        Main.BuildManifestCommand.class,
        Main.DownloadCommand.class
})
public final class Main implements Runnable {

    /**
     * One line per record ({@code [LEVEL] message}) instead of JUL's default
     * two-line, timestamped layout, which buries the message in CLI output.
     */
    private static final String LOG_FORMAT = "[%4$s] %5$s%6$s%n";

    @Mixin
    private HelpOption help;

    public static void main(String[] args) {
        if (System.getProperty("java.util.logging.SimpleFormatter.format") == null) {
            System.setProperty("java.util.logging.SimpleFormatter.format", LOG_FORMAT);
        }
        System.exit(commandLine().execute(args));
    }

    /**
     * Builds the configured command line. Failures inside a command are reported as a
     * single {@code "<command> failed: <reason>"} line on stderr with exit code 1 —
     * the reason, not a stack trace, is what a CLI user can act on.
     */
    static CommandLine commandLine() {
        CommandLine cli = new CommandLine(new Main());
        cli.setExecutionExceptionHandler((ex, commandLine, parseResult) -> {
            commandLine.getErr().println(commandLine.getCommandName() + " failed: " + describe(ex));
            return 1;
        });
        return cli;
    }

    /** Joins the messages of an exception chain, skipping wrappers that only repeat their cause. */
    static String describe(Throwable failure) {
        StringBuilder text = new StringBuilder();
        for (Throwable t = failure; t != null; t = t.getCause()) {
            String message = t.getMessage();
            if (message == null || message.isBlank()) {
                message = t.getClass().getSimpleName();
            }
            if (!text.toString().contains(message)) {
                if (!text.isEmpty()) {
                    text.append(": ");
                }
                text.append(message);
            }
        }
        return text.toString();
    }

    @Override
    public void run() {
        // No subcommand given — print usage instead of doing nothing silently.
        CommandLine.usage(this, System.out);
    }

    /**
     * Derives the conventional manifest path for a source file:
     * {@code <file>.manifest.json}
     * next to the file itself. Used by both {@code seed} and {@code build-manifest}
     * so a
     * {@code download} run always knows where to look.
     */
    private static Path manifestPathFor(Path sourceFile) {
        return sourceFile.resolveSibling(sourceFile.getFileName() + ".manifest.json");
    }

    private static BitSet heldChunksFor(Manifest manifest) {
        BitSet held = new BitSet(manifest.totalChunks());
        held.set(0, manifest.totalChunks());
        return held;
    }

    /**
     * {@code seed <file> <port>} — builds a manifest for {@code file}, persists it
     * to
     * {@code <file>.manifest.json}, and serves every chunk to peers over a TCP
     * {@link TcpChunkServer} listening on {@code port}.
     */
    @Command(name = "seed", description = "Serve file to peers over TCP")
    static final class SeedCommand implements Callable<Integer> {

        private static final Logger LOG = System.getLogger(SeedCommand.class.getName());

        @Mixin
        private HelpOption help;

        @Spec
        private CommandSpec spec;

        @Parameters(index = "0", description = "File to seed")
        private Path file;

        @Parameters(index = "1", description = "TCP port to listen on")
        private int port;

        @Override
        public Integer call() throws Exception {
            LOG.log(Level.INFO, "Building manifest for seeder...");
            Manifest manifest = new ManifestBuilder().build(file);

            // Persist the manifest so leechers have something to download() against —
            // without this, "download" has no way to learn the chunk layout or file hash.
            Path manifestPath = manifestPathFor(file);
            new ManifestSerializer().write(manifest, manifestPath);
            LOG.log(Level.INFO, "Manifest written to {0}", manifestPath);

            // Use the storage module's FileChannelStorage directly against the source
            // file. Seeding only ever reads chunks back out to serve peers — there's no
            // write path, so no preallocation or resume logic is needed here.
            try (var storage = new FileChannelStorage(file);
                    var server = new TcpChunkServer(port, storage, manifest, heldChunksFor(manifest))) {
                // Bind on this thread so "address already in use" fails the command
                // immediately with the real cause.
                int boundPort = server.bind();
                LOG.log(Level.INFO, "Seeder running. fileHash={0}", manifest.fileHash());
                spec.commandLine().getOut().println("Listening on port " + boundPort);
                spec.commandLine().getOut().flush();
                server.serve(); // blocks until the process is stopped
            }

            return 0;
        }
    }

    /**
     * {@code build-manifest <file> [chunkSize]} — computes a manifest for
     * {@code file},
     * prints a summary, and persists it to {@code <file>.manifest.json} (or a
     * custom
     * path via {@code --output}) so it can be shared with peers without running a
     * seeder.
     */
    @Command(name = "build-manifest", description = "Compute, print, and persist a manifest")
    static final class BuildManifestCommand implements Callable<Integer> {

        @Mixin
        private HelpOption help;

        @Spec
        private CommandSpec spec;

        @Parameters(index = "0", description = "File to build a manifest for")
        private Path file;

        @Parameters(index = "1", arity = "0..1", description = "Chunk size in bytes (default: ${DEFAULT-VALUE})", defaultValue = ""
                + ManifestBuilder.DEFAULT_CHUNK_SIZE)
        private int chunkSize;

        @Option(names = { "-o",
                "--output" }, description = "Where to write the manifest JSON (default: <file>.manifest.json)")
        private Path outputOverride;

        @Override
        public Integer call() {
            Manifest manifest = new ManifestBuilder(chunkSize).build(file);

            var out = spec.commandLine().getOut();
            out.println("fileHash=" + manifest.fileHash());
            out.println("fileName=" + manifest.fileName());
            out.println("totalSize=" + manifest.totalSize());
            out.println("chunkSize=" + manifest.chunkSize());
            out.println("totalChunks=" + manifest.totalChunks());

            Path manifestPath = (outputOverride != null) ? outputOverride : manifestPathFor(file);
            new ManifestSerializer().write(manifest, manifestPath);
            out.println("manifestPath=" + manifestPath);

            return 0;
        }
    }

    /**
     * {@code download <manifestFile> <output> <peerHost:peerPort>...} — fetches a
     * manifest's chunks from one or more peers and writes them to {@code output}.
     */
    @Command(name = "download", description = "Download a file from peers using its manifest")
    static final class DownloadCommand implements Callable<Integer> {

        private static final Logger LOG = System.getLogger(DownloadCommand.class.getName());

        @Mixin
        private HelpOption help;

        @Parameters(index = "0", description = "Path to the manifest JSON file")
        private Path manifestFile;

        @Parameters(index = "1", description = "Where to write the downloaded file")
        private Path output;

        @Parameters(index = "2..*", arity = "1..*", description = "Peers to download from, as host:port")
        private List<String> peerAddresses;

        @Override
        public Integer call() throws Exception {
            Manifest manifest = new ManifestSerializer().read(manifestFile);
            switch (ManifestValidator.validate(manifest)) {
                case ManifestValidator.ValidationResult.Valid ignored -> {
                }
                case ManifestValidator.ValidationResult.Invalid invalid ->
                        throw new IllegalArgumentException("Invalid manifest:\n" + invalid.summary());
            }

            List<PeerInfo> peers = peerAddresses.stream()
                    .map(DownloadCommand::parsePeer)
                    .toList();

            try (var storage = new FileChannelStorage(output);
                 var connector = new TcpPeerConnector()) {
                var manager = new TransferManager(manifest, peers, storage, connector);

                LOG.log(Level.INFO, "Downloading {0} ({1} chunks) from {2} peer(s)...",
                        manifest.fileName(), manifest.totalChunks(), peers.size());

                manager.start(); // blocks until complete or throws IllegalStateException

                LOG.log(Level.INFO, "Download complete: {0}", output);
            }

            return 0;
        }

        static PeerInfo parsePeer(String hostPort) {
            int colon = hostPort.lastIndexOf(':');
            if (colon <= 0 || colon == hostPort.length() - 1) {
                throw new IllegalArgumentException("Expected host:port, got: " + hostPort);
            }
            String host = hostPort.substring(0, colon);
            if (host.startsWith("[") && host.endsWith("]") && host.length() > 2) {
                host = host.substring(1, host.length() - 1);
            }
            int port;
            try {
                port = Integer.parseInt(hostPort.substring(colon + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Expected host:port, got: " + hostPort, e);
            }
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Port out of range: " + port);
            }
            var address = new InetSocketAddress(host, port);
            return new PeerInfo(UUID.randomUUID(), address);
        }
    }

    /**
     * Adds {@code -h/--help} to a command. A mixin rather than
     * {@code mixinStandardHelpOptions} because the CLI has no version to report.
     */
    static final class HelpOption {
        @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
        boolean helpRequested;
    }
}
