package io.github.hursungyun.kafbat.ui.serde.sources;

import com.google.protobuf.DescriptorProtos;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Optional;

/** Descriptor source that loads from a local file */
public class LocalFileDescriptorSource implements DescriptorSource {

    private final String filePath;

    public LocalFileDescriptorSource(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            throw new IllegalArgumentException("filePath cannot be null or empty");
        }
        this.filePath = filePath.trim();
    }

    @Override
    public DescriptorProtos.FileDescriptorSet loadDescriptorSet() throws IOException {
        try (FileInputStream fis = new FileInputStream(filePath)) {
            return DescriptorProtos.FileDescriptorSet.parseFrom(fis);
        }
    }

    @Override
    public Optional<Instant> getLastModified() {
        try {
            Path path = Paths.get(filePath);
            return Optional.of(Files.getLastModifiedTime(path).toInstant());
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public String getDescription() {
        return "Local file: " + filePath;
    }

    @Override
    public boolean supportsRefresh() {
        return true;
    }
}
