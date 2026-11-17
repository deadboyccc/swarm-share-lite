package io.swarmshare.cli;

import io.swarmshare.core.domain.Manifest;
import io.swarmshare.core.domain.PeerInfo;
import io.swarmshare.manifest.ManifestSerializer;
import io.swarmshare.manifest.ManifestValidator;
import io.swarmshare.networking.TcpPeerConnector;
import io.swarmshare.storage.FileChannelStorage;
import io.swarmshare.transfer.TransferManager;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.io.PrintWriter;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * {@code leech} — downloads the file described by a manifest from one or more peers
 * (ARCHITECTURE.md §13), resuming from whatever valid chunks the output file
 * already holds.
 */
@Command(name = "leech", description = "Download a file from the swarm")
final class LeechCommand implements Callable<Integer> {

    private static final Logger LOG = System.getLogger(LeechCommand.class.getName());

    @Mixin
    private HelpOption help;

    @Spec
    private CommandSpec spec;

    @Option(names = "--manifest", required = true, paramLabel = "<path>", description = "Path to manifest JSON")
    private Path manifestPath;

    @Option(names = "--peer", required = true, arity = "1..*", paramLabel = "<host:port>",
            description = "Address of a peer serving the file; repeat or list several, in order of preference")
    private List<String> peerAddresses;

    @Option(names = "--output", required = true, paramLabel = "<path>", description = "Output file path")
    private Path outputPath;

    @Override
    public Integer call() {
        download(manifestPath, outputPath, peerAddresses, spec.commandLine().getOut());
        return 0;
    }

    /**
     * The download flow shared by {@code leech} and {@code download}: validate the
     * manifest before touching the output file, then run the transfer to completion.
     *
     * @throws IllegalArgumentException if the manifest or a peer address is invalid
     * @throws IllegalStateException    if the transfer could not complete or verify
     */
    static void download(Path manifestFile, Path output, List<String> peerAddresses, PrintWriter out) {
        Manifest manifest = new ManifestSerializer().read(manifestFile);
        switch (ManifestValidator.validate(manifest)) {
            case ManifestValidator.ValidationResult.Valid ignored -> {
            }
            case ManifestValidator.ValidationResult.Invalid invalid ->
                    throw new IllegalArgumentException("Invalid manifest:\n" + invalid.summary());
        }
        List<PeerInfo> peers = peerAddresses.stream().map(PeerAddresses::parse).toList();

        try (var storage = new FileChannelStorage(output);
             var connector = new TcpPeerConnector()) {
            LOG.log(Level.INFO, "Downloading {0} ({1} chunks) from {2} peer(s)...",
                    manifest.fileName(), manifest.totalChunks(), peers.size());
            new TransferManager(manifest, peers, storage, connector).start();
        }
        out.println("Download complete: " + output);
        out.flush();
    }
}
