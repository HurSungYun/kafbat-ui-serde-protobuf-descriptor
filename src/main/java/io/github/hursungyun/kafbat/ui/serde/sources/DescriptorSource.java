package io.github.hursungyun.kafbat.ui.serde.sources;

import com.google.protobuf.DescriptorProtos;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

/** Interface for loading protobuf descriptor sets from various sources */
public interface DescriptorSource {

    /**
     * Load the descriptor set from this source
     *
     * @return The loaded descriptor set
     * @throws IOException if loading fails
     */
    DescriptorProtos.FileDescriptorSet loadDescriptorSet() throws IOException;

    /**
     * Get the last modified time of the descriptor source if available
     *
     * @return Last modified time, empty if not supported
     */
    Optional<Instant> getLastModified();

    /**
     * Get a human-readable description of this source
     *
     * @return Source description
     */
    String getDescription();

    /**
     * Check if this source supports refresh/reload operations
     *
     * @return true if refresh is supported
     */
    boolean supportsRefresh();
}
