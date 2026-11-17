package io.swarmshare.cli;

import io.swarmshare.core.domain.Manifest;
import io.swarmshare.manifest.ManifestBuilder;
import io.swarmshare.manifest.ManifestSerializer;
import io.swarmshare.networking.TcpChunkServer;
import io.swarmshare.storage.FileChannelStorage;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.concurrent.Callable;

/**
 * {@code seed} — builds the manifest for a file, writes it where peers can get it,
 * and serves every chunk over TCP until the process is stopped.
 *
 * <p>
 * Accepts the options from ARCHITECTURE.md §13 ({@code --file}, {@code --port},
 * {@code --manifest-out}) and, for compatibility with earlier releases, the
 * positional form {@code seed <file> [<port>]}.
 */
@Command(name = "seed", description = "Announce and serve a file to the swarm")
final class SeedCommand implements Callable<Integer> {

    static final int DEFAULT_PORT = 7070;

    @Mixin
    private HelpOption help;

    @Spec
    private CommandSpec spec;

    @Option(names = "--file", paramLabel = "<file>", description = "Path to file to seed")
    private Path fileOption;

    @Option(names = "--port", paramLabel = "<port>",
            description = "TCP port to listen on (default: " + DEFAULT_PORT + "; 0 picks a free port)")
    private Integer portOption;

    @Option(names = "--manifest-out", paramLabel = "<path>",
            description = "Where to write the manifest JSON (default: <file>.manifest.json)")
    private Path manifestOut;

    @Parameters(index = "0", arity = "0..1", paramLabel = "<file>", description = "Positional form of --file")
    private Path fileArgument;

    @Parameters(index = "1", arity = "0..1", paramLabel = "<port>", description = "Positional form of --port")
    private Integer portArgument;

    @Override
    public Integer call() throws Exception {
        Path file = either("file", fileOption, fileArgument);
        if (file == null) {
            throw new ParameterException(spec.commandLine(), "Missing required option: '--file=<file>'");
        }
        Integer requestedPort = either("port", portOption, portArgument);
        int port = requestedPort == null ? DEFAULT_PORT : requestedPort;

        Manifest manifest = new ManifestBuilder().build(file);
        Path manifestPath = manifestOut != null ? manifestOut : ManifestFiles.defaultPathFor(file);
        // Persist the manifest: it is the swarm's contract, and leechers need it
        // to learn the chunk layout and hashes before any transfer.
        new ManifestSerializer().write(manifest, manifestPath);

        PrintWriter out = spec.commandLine().getOut();
        out.println("Manifest hash: " + manifest.fileHash());
        out.println("Total chunks:  " + manifest.totalChunks());
        out.println("Manifest file: " + manifestPath);

        // The seeder holds every chunk from the start. The source file is only read,
        // so storage is never preallocated (that would open it for writing).
        BitSet heldChunks = new BitSet(manifest.totalChunks());
        heldChunks.set(0, manifest.totalChunks());
        try (var storage = new FileChannelStorage(file);
             var server = new TcpChunkServer(port, storage, manifest, heldChunks)) {
            // Bind on this thread so "address already in use" fails the command
            // immediately with the real cause.
            int boundPort = server.bind();
            out.println("Listening on port " + boundPort);
            out.flush();
            server.serve(); // blocks until the process is stopped
        }
        return 0;
    }

    /** The value given as an option or positionally; giving two different values is a usage error. */
    private <T> T either(String name, T option, T positional) {
        if (option != null && positional != null && !option.equals(positional)) {
            throw new ParameterException(spec.commandLine(),
                    "Conflicting values for %s: --%s=%s and positional %s".formatted(name, name, option, positional));
        }
        return option != null ? option : positional;
    }
}
