package io.swarmshare.cli;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * {@code download <manifest> <output> <host:port>...} — the positional form of
 * {@link LeechCommand leech}, kept so commands written for earlier releases work.
 */
@Command(name = "download", description = "Same as leech, with positional arguments")
final class DownloadCommand implements Callable<Integer> {

    @Mixin
    private HelpOption help;

    @Spec
    private CommandSpec spec;

    @Parameters(index = "0", paramLabel = "<manifest>", description = "Path to the manifest JSON file")
    private Path manifestFile;

    @Parameters(index = "1", paramLabel = "<output>", description = "Where to write the downloaded file")
    private Path output;

    @Parameters(index = "2..*", arity = "1..*", paramLabel = "<host:port>",
            description = "Peers to download from, in order of preference")
    private List<String> peerAddresses;

    @Override
    public Integer call() {
        LeechCommand.download(manifestFile, output, peerAddresses, spec.commandLine().getOut());
        return 0;
    }
}
