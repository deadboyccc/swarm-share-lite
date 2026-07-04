// manifest/ManifestSerializer.java
package io.swarmshare.manifest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.swarmshare.core.domain.ChunkDescriptor;
import io.swarmshare.core.domain.ChunkId;
import io.swarmshare.core.domain.Manifest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Serializes and deserializes {@link Manifest} to and from JSON using Jackson.
 *
 * <h3>Why not Java records directly?</h3>
 * Jackson supports records natively from Jackson 2.12+, but {@link ChunkDescriptor}
 * and {@link ChunkId} nest in ways that require explicit mapping: Jackson needs to
 * know how to reconstruct the nested record graph from flat JSON fields. We solve this
 * with a set of private DTO records annotated with {@code @JsonProperty} and
 * {@code @JsonCreator}, which keep the domain model annotation-free and decouple
 * JSON field names from record component names.
 *
 * <h3>Manifest as a wire contract</h3>
 * The manifest JSON is written once by the seeder and distributed to all peers before
 * any chunk transfer begins. Its JSON shape is therefore a public wire contract — field
 * names should not be renamed without a versioning strategy.
 *
 * <h3>Thread safety</h3>
 * {@link ObjectMapper} is thread-safe after configuration. The single {@code MAPPER}
 * instance is shared across all calls.
 */
public final class ManifestSerializer {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    // ── public API ────────────────────────────────────────────────────────────────

    private static ManifestDto toDto(Manifest manifest) {
        List<ChunkDto> chunkDtos = manifest.chunks().stream()
                .map(desc -> new ChunkDto(
                        desc.id().index(),
                        desc.offset(),
                        desc.size(),
                        desc.sha256()))
                .toList();
        return new ManifestDto(
                manifest.fileHash(),
                manifest.fileName(),
                manifest.totalSize(),
                manifest.chunkSize(),
                chunkDtos);
    }

    private static Manifest fromDto(ManifestDto dto) {
        List<ChunkDescriptor> descriptors = dto.chunks().stream()
                .map(cd -> new ChunkDescriptor(
                        new ChunkId(dto.fileHash(), cd.index()),
                        cd.offset(),
                        cd.size(),
                        cd.sha256()))
                .toList();
        return new Manifest(
                dto.fileHash(),
                dto.fileName(),
                dto.totalSize(),
                dto.chunkSize(),
                descriptors);
    }

    /**
     * Serializes {@code manifest} to a JSON file at {@code outputPath}.
     *
     * <p>The file is created or overwritten. The output is pretty-printed for
     * readability — manifests are tiny relative to the files they describe.
     *
     * @param manifest   the manifest to write
     * @param outputPath destination path; parent directories must exist
     * @throws UncheckedIOException if the file cannot be written
     */
    public void write(Manifest manifest, Path outputPath) {
        try {
            MAPPER.writeValue(outputPath.toFile(), toDto(manifest));
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to write manifest to %s".formatted(outputPath), e);
        }
    }

    /**
     * Deserializes a {@link Manifest} from the JSON file at {@code manifestPath}.
     *
     * @param manifestPath path to the manifest JSON file
     * @return the deserialized manifest
     * @throws UncheckedIOException if the file cannot be read or parsed
     */
    public Manifest read(Path manifestPath) {
        try {
            ManifestDto dto = MAPPER.readValue(manifestPath.toFile(), ManifestDto.class);
            return fromDto(dto);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read manifest from %s".formatted(manifestPath), e);
        }
    }

    // ── DTO layer ─────────────────────────────────────────────────────────────────
    // Private DTOs decouple the domain model from the JSON wire format.
    // Domain records stay annotation-free; only these DTOs carry Jackson annotations.

    /**
     * Serializes {@code manifest} to a JSON string (useful for wire transmission).
     *
     * @param manifest the manifest to serialize
     * @return a pretty-printed JSON string
     * @throws UncheckedIOException if serialization fails
     */
    public String toJson(Manifest manifest) {
        try {
            return MAPPER.writeValueAsString(toDto(manifest));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize manifest to JSON", e);
        }
    }

    /**
     * Deserializes a {@link Manifest} from a JSON string.
     *
     * @param json the JSON string to parse
     * @return the deserialized manifest
     * @throws UncheckedIOException if the string cannot be parsed
     */
    public Manifest fromJson(String json) {
        try {
            ManifestDto dto = MAPPER.readValue(json, ManifestDto.class);
            return fromDto(dto);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to deserialize manifest from JSON", e);
        }
    }

    // ── private DTO records ───────────────────────────────────────────────────────

    private record ManifestDto(
            @JsonProperty("fileHash") String fileHash,
            @JsonProperty("fileName") String fileName,
            @JsonProperty("totalSize") long totalSize,
            @JsonProperty("chunkSize") int chunkSize,
            @JsonProperty("chunks") List<ChunkDto> chunks
    ) {
        @JsonCreator
        ManifestDto {
        }  // Jackson needs an explicit creator for records with @JsonProperty
    }

    private record ChunkDto(
            @JsonProperty("index") int index,
            @JsonProperty("offset") long offset,
            @JsonProperty("size") int size,
            @JsonProperty("sha256") String sha256
    ) {
        @JsonCreator
        ChunkDto {
        }
    }
}