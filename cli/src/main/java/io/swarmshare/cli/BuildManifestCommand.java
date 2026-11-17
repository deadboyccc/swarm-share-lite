package io.swarmshare.cli;

import io.swarmshare.core.domain.Manifest;
import io.swarmshare.manifest.ManifestBuilder;
import io.swarmshare.manifest.ManifestSerializer;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.concurrent.Callable;

/**
 * {@code build-manifest <file> [chunkSize]} — computes a manifest, prints a summary,
 * and writes it to {@code <file>.manifest.json} (or {@code --output}) so it can be
 * shared with peers without running a seeder.
 */
@Command(name = "build-manifest", description = "Compute, print, and persist a manifest")
final class BuildManifestCommand implements Callable<Integer> {

    @Mixin
    private HelpOption help;

    @Spec
    private CommandSpec spec;

    @Parameters(index = "0", paramLabel = "<file>", description = "File to build a manifest for")
    private Path file;

    @Parameters(index = "1", arity = "0..1", paramLabel = "<chunkSize>",
            description = "Chunk size in bytes (default: ${DEFAULT-VALUE})",
            defaultValue = "" + ManifestBuilder.DEFAULT_CHUNK_SIZE)
    private int chunkSize;

    @Option(names = {"-o", "--output"}, paramLabel = "<path>",
            description = "Where to write the manifest JSON (default: <file>.manifest.json)")
    private Path outputOverride;

    @Override
    public Integer call() {
        Manifest manifest = new ManifestBuilder(chunkSize).build(file);
        Path manifestPath = outputOverride != null ? outputOverride : ManifestFiles.defaultPathFor(file);
        new ManifestSerializer().write(manifest, manifestPath);

        PrintWriter out = spec.commandLine().getOut();
        out.println("fileHash=" + manifest.fileHash());
        out.println("fileName=" + manifest.fileName());
        out.println("totalSize=" + manifest.totalSize());
        out.println("chunkSize=" + manifest.chunkSize());
        out.println("totalChunks=" + manifest.totalChunks());
        out.println("manifestPath=" + manifestPath);
        return 0;
    }
}
