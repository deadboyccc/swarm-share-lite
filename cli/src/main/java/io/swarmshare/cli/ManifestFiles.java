package io.swarmshare.cli;

import java.nio.file.Path;

/**
 * Where manifests live when the user does not say: {@code <file>.manifest.json}
 * next to the file, so {@code seed}, {@code build-manifest} and the peers that
 * receive the manifest all agree on one name.
 */
final class ManifestFiles {

    private ManifestFiles() {
    }

    static Path defaultPathFor(Path sourceFile) {
        return sourceFile.resolveSibling(sourceFile.getFileName() + ".manifest.json");
    }
}
