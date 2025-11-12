package io.github.hursungyun.kafbat.ui.serde;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import io.github.hursungyun.kafbat.ui.serde.auth.MinioClientFactory;
import io.github.hursungyun.kafbat.ui.serde.auth.S3Configuration;
import io.github.hursungyun.kafbat.ui.serde.serialization.ProtobufDeserializer;
import io.github.hursungyun.kafbat.ui.serde.serialization.ProtobufSerializer;
import io.github.hursungyun.kafbat.ui.serde.sources.DescriptorSource;
import io.github.hursungyun.kafbat.ui.serde.sources.DescriptorSourceFactory;
import io.github.hursungyun.kafbat.ui.serde.sources.S3DescriptorSource;
import io.github.hursungyun.kafbat.ui.serde.sources.S3TopicMappingSource;
import io.kafbat.ui.serde.api.PropertyResolver;
import io.kafbat.ui.serde.api.SchemaDescription;
import io.kafbat.ui.serde.api.Serde;
import io.minio.MinioClient;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtobufDescriptorSetSerde implements Serde {

    private static final Logger logger = LoggerFactory.getLogger(ProtobufDescriptorSetSerde.class);

    // Thread-safe fields accessed by both main thread and refresh thread
    private volatile Map<String, Descriptors.FileDescriptor> fileDescriptorMap;
    private volatile Map<String, Descriptors.Descriptor> topicToMessageDescriptorMap =
            new HashMap<>();
    private volatile Descriptors.Descriptor defaultMessageDescriptor;

    // Configuration fields (set once during configure())
    private DescriptorSource descriptorSource;
    private S3TopicMappingSource topicMappingSource;
    private PropertyResolver serdeProperties;
    private ProtobufSerializer protobufSerializer;
    private ProtobufDeserializer protobufDeserializer;
    private boolean strictFieldValidation;
    private DescriptorRefreshScheduler refreshScheduler;

    @Override
    public void configure(
            PropertyResolver serdeProperties,
            PropertyResolver clusterProperties,
            PropertyResolver appProperties) {
        this.serdeProperties = serdeProperties;

        try {
            initializeDescriptorSources(serdeProperties);
            initializeDescriptors();
            initializeSerializers();
            startBackgroundRefresh();
        } catch (IOException | Descriptors.DescriptorValidationException e) {
            String sourceDescription =
                    descriptorSource != null ? descriptorSource.getDescription() : "unknown source";
            throw new RuntimeException(
                    "Failed to load protobuf descriptor set from: " + sourceDescription, e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure ProtobufDescriptorSetSerde", e);
        }
    }

    private void initializeDescriptorSources(PropertyResolver serdeProperties) {
        this.descriptorSource = DescriptorSourceFactory.create(serdeProperties);
        this.topicMappingSource = createTopicMappingSource(serdeProperties);
    }

    private void initializeDescriptors()
            throws IOException, Descriptors.DescriptorValidationException {
        loadDescriptorSet();
        configureTopicMappings(serdeProperties);
    }

    private void initializeSerializers() {
        // Check if strict field validation is enabled (default: true)
        Optional<Boolean> strictModeOpt =
                serdeProperties.getProperty("serialization.strict.field.validation", Boolean.class);
        this.strictFieldValidation = strictModeOpt.orElse(true);

        this.protobufSerializer = new ProtobufSerializer(strictFieldValidation);
        this.protobufDeserializer = new ProtobufDeserializer();
    }

    private void loadDescriptorSet() throws IOException, Descriptors.DescriptorValidationException {
        DescriptorProtos.FileDescriptorSet descriptorSet = descriptorSource.loadDescriptorSet();

        fileDescriptorMap = new HashMap<>();
        Map<String, Descriptors.FileDescriptor> tempDescriptors = new HashMap<>();

        // First pass: create all FileDescriptors without dependencies
        for (DescriptorProtos.FileDescriptorProto fileDescriptorProto :
                descriptorSet.getFileList()) {
            if (fileDescriptorProto.getDependencyCount() == 0) {
                Descriptors.FileDescriptor fileDescriptor =
                        Descriptors.FileDescriptor.buildFrom(
                                fileDescriptorProto, new Descriptors.FileDescriptor[0]);
                tempDescriptors.put(fileDescriptorProto.getName(), fileDescriptor);
            }
        }

        // Second pass: create FileDescriptors with dependencies
        buildFileDescriptorsWithDependencies(descriptorSet, tempDescriptors);

        this.fileDescriptorMap = tempDescriptors;
    }

    private S3TopicMappingSource createTopicMappingSource(PropertyResolver properties) {
        // Check for S3 topic mapping configuration
        Optional<String> s3Bucket =
                properties.getProperty("topic.mapping.value.s3.bucket", String.class);
        Optional<String> s3ObjectKey =
                properties.getProperty("topic.mapping.value.s3.object.key", String.class);

        if (s3Bucket.isPresent() && s3ObjectKey.isPresent()) {
            // Use existing S3 configuration from descriptor source
            if (descriptorSource instanceof S3DescriptorSource) {
                // Reuse the MinIO client from descriptor source - this requires exposing the client
                // For now, we'll create a new client with the same configuration
                return createS3TopicMappingSourceFromProperties(
                        properties, s3Bucket.get(), s3ObjectKey.get());
            } else {
                // Check if we have S3 credentials in properties
                Optional<String> s3Endpoint = properties.getProperty("s3.endpoint", String.class);
                if (s3Endpoint.isPresent()) {
                    return createS3TopicMappingSourceFromProperties(
                            properties, s3Bucket.get(), s3ObjectKey.get());
                }
            }
        }

        return null;
    }

    private S3TopicMappingSource createS3TopicMappingSourceFromProperties(
            PropertyResolver properties, String bucket, String objectKey) {
        // Create S3 configuration from properties (reusing same S3 config as descriptors)
        S3Configuration config = S3Configuration.fromProperties(properties, "descriptor.value.s3");

        // Create MinIO client using the factory
        MinioClient minioClient = MinioClientFactory.create(config);

        return new S3TopicMappingSource(
                minioClient, bucket, objectKey, config.getRefreshInterval());
    }

    private void configureTopicMappings(PropertyResolver serdeProperties) {
        Optional<String> defaultMessageName =
                serdeProperties.getProperty("message.value.default.type", String.class);
        Map<String, String> combinedTopicMappings = loadCombinedTopicMappings(serdeProperties);
        Map<String, Descriptors.Descriptor> allDescriptors = buildDescriptorMap();

        // Build new state without mutating existing state (thread-safe)
        Descriptors.Descriptor newDefaultDescriptor =
                buildDefaultMessageDescriptor(defaultMessageName, allDescriptors);
        Map<String, Descriptors.Descriptor> newTopicMappings =
                buildTopicSpecificMappings(combinedTopicMappings, allDescriptors);

        // Atomic assignment - volatile writes ensure visibility to other threads
        this.defaultMessageDescriptor = newDefaultDescriptor;
        this.topicToMessageDescriptorMap = newTopicMappings;
    }

    private Map<String, String> loadCombinedTopicMappings(PropertyResolver serdeProperties) {
        Map<String, String> combinedTopicMappings = new HashMap<>();

        // Load from S3 if available
        if (topicMappingSource != null) {
            try {
                Map<String, String> s3TopicMappings = topicMappingSource.loadTopicMappings();
                combinedTopicMappings.putAll(s3TopicMappings);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to load topic mappings from S3: "
                                + topicMappingSource.getDescription(),
                        e);
            }
        }

        // Get topic-specific message mappings from local configuration (overrides S3)
        Optional<Map<String, String>> localTopicMessageMappings =
                serdeProperties.getMapProperty(
                        "topic.mapping.value.local", String.class, String.class);
        if (localTopicMessageMappings.isPresent()) {
            combinedTopicMappings.putAll(localTopicMessageMappings.get());
        }

        return combinedTopicMappings;
    }

    private Map<String, Descriptors.Descriptor> buildDescriptorMap() {
        Map<String, Descriptors.Descriptor> allDescriptors = new HashMap<>();
        for (Descriptors.FileDescriptor fileDescriptor : fileDescriptorMap.values()) {
            for (Descriptors.Descriptor messageDescriptor : fileDescriptor.getMessageTypes()) {
                allDescriptors.put(messageDescriptor.getFullName(), messageDescriptor);
            }
        }
        return allDescriptors;
    }

    /**
     * Build default message descriptor without mutating state (thread-safe).
     *
     * @return The default descriptor, or null if not configured
     */
    private Descriptors.Descriptor buildDefaultMessageDescriptor(
            Optional<String> defaultMessageName,
            Map<String, Descriptors.Descriptor> allDescriptors) {
        if (defaultMessageName.isPresent()) {
            return findDescriptorByName(allDescriptors, defaultMessageName.get());
        }
        return null;
    }

    /**
     * Build topic-specific mappings without mutating state (thread-safe).
     *
     * @return New map of topic to descriptor mappings
     */
    private Map<String, Descriptors.Descriptor> buildTopicSpecificMappings(
            Map<String, String> combinedTopicMappings,
            Map<String, Descriptors.Descriptor> allDescriptors) {
        Map<String, Descriptors.Descriptor> newMappings = new HashMap<>();

        if (!combinedTopicMappings.isEmpty()) {
            for (Map.Entry<String, String> entry : combinedTopicMappings.entrySet()) {
                String topic = entry.getKey();
                String messageName = entry.getValue();
                Descriptors.Descriptor descriptor =
                        findDescriptorByName(allDescriptors, messageName);
                if (descriptor != null) {
                    newMappings.put(topic, descriptor);
                }
            }
        }

        return newMappings;
    }

    private Optional<Descriptors.Descriptor> descriptorFor(String topic, Target target) {
        // For now, only handle VALUE target (keys would need separate mapping)
        if (target == Target.KEY) {
            return Optional.empty();
        }

        // First try topic-specific mapping, then fall back to default
        return Optional.ofNullable(topicToMessageDescriptorMap.get(topic))
                .or(() -> Optional.ofNullable(defaultMessageDescriptor));
    }

    @Override
    public Optional<String> getDescription() {
        String sourceDescription =
                descriptorSource != null ? descriptorSource.getDescription() : "unknown source";
        return Optional.of(
                "Protobuf Descriptor Set Serde - deserializes protobuf messages from "
                        + sourceDescription);
    }

    @Override
    public Optional<SchemaDescription> getSchema(String topic, Target target) {
        return Optional.empty();
    }

    @Override
    public boolean canDeserialize(String topic, Target target) {
        return descriptorFor(topic, target).isPresent();
    }

    @Override
    public boolean canSerialize(String topic, Target target) {
        // Support serialization for topics with configured message types
        return descriptorFor(topic, target).isPresent();
    }

    @Override
    public Serializer serializer(String topic, Target target) {
        return inputString -> {
            // Get the descriptor for this topic
            Optional<Descriptors.Descriptor> descriptorOpt = descriptorFor(topic, target);
            if (descriptorOpt.isEmpty()) {
                throw new IllegalStateException(
                        "No message type configured for topic: " + topic + ", target: " + target);
            }

            try {
                return protobufSerializer.serialize(descriptorOpt.get(), inputString);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to serialize JSON to protobuf message for topic "
                                + topic
                                + " with configured message type "
                                + descriptorOpt.get().getFullName(),
                        e);
            }
        };
    }

    @Override
    public Deserializer deserializer(String topic, Target target) {
        return (recordHeaders, bytes) -> {
            // Try to deserialize with specific descriptor for topic first
            Optional<Descriptors.Descriptor> specificDescriptor = descriptorFor(topic, target);
            if (specificDescriptor.isPresent()) {
                // If a specific descriptor is configured, use only that one
                try {
                    return protobufDeserializer.deserialize(specificDescriptor.get(), bytes);
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to deserialize protobuf message for topic "
                                    + topic
                                    + " with configured message type "
                                    + specificDescriptor.get().getFullName(),
                            e);
                }
            }

            // No specific descriptor configured - cannot deserialize
            throw new IllegalStateException(
                    "No message type configured for topic: " + topic + ", target: " + target);
        };
    }

    /**
     * Start background refresh scheduler if the descriptor source or topic mapping source supports
     * refresh. The refresh will run at the interval specified in the descriptor source
     * configuration.
     */
    private void startBackgroundRefresh() {
        // Check if either descriptor source or topic mapping source supports refresh
        boolean descriptorSupportsRefresh =
                descriptorSource != null && descriptorSource.supportsRefresh();
        boolean topicMappingSupportsRefresh = topicMappingSource != null;

        if (!descriptorSupportsRefresh && !topicMappingSupportsRefresh) {
            logger.debug("Background refresh not started: no sources support refresh");
            return;
        }

        // Get refresh interval from S3 configuration
        Optional<Long> intervalSecondsOpt =
                serdeProperties.getProperty(
                        "descriptor.value.s3.refresh.interval.seconds", Long.class);
        long intervalSeconds = intervalSecondsOpt.orElse(60L); // Default to 60 seconds

        if (intervalSeconds <= 0) {
            logger.warn(
                    "Invalid refresh interval: {} seconds. Background refresh will not be started.",
                    intervalSeconds);
            return;
        }

        // Build source description for logging
        StringBuilder sources = new StringBuilder();
        if (descriptorSource != null) {
            sources.append(descriptorSource.getDescription());
        }
        if (topicMappingSource != null) {
            if (sources.length() > 0) {
                sources.append(", ");
            }
            sources.append(topicMappingSource.getDescription());
        }

        // Create and start the refresh scheduler
        refreshScheduler =
                new DescriptorRefreshScheduler(
                        this::refreshDescriptors, intervalSeconds, sources.toString());
        refreshScheduler.start();
    }

    /**
     * Refresh the descriptor set and topic mappings from source if they support refresh Uses
     * graceful error handling - refresh failures are logged but don't break existing functionality
     *
     * @return true if refresh was attempted, false if not supported
     */
    public boolean refreshDescriptors() {
        boolean refreshed = false;

        // Refresh descriptors (graceful failure handling)
        if (descriptorSource != null && descriptorSource.supportsRefresh()) {
            try {
                logger.debug(
                        "Attempting to refresh descriptor set from: {}",
                        descriptorSource.getDescription());

                // Force invalidation if S3 source
                if (descriptorSource instanceof S3DescriptorSource) {
                    ((S3DescriptorSource) descriptorSource).invalidateCache();
                }

                // Reload descriptors - this will use graceful error handling in S3DescriptorSource
                loadDescriptorSet();
                refreshed = true;
                logger.info(
                        "Successfully refreshed descriptor set from: {}",
                        descriptorSource.getDescription());
            } catch (Exception e) {
                // Log warning but don't break existing functionality
                logger.warn(
                        "Failed to refresh descriptor set from: {}. Continuing with existing"
                                + " descriptors. Error: {}",
                        descriptorSource.getDescription(),
                        e.getMessage());
                // Don't throw exception - keep existing descriptors working
            }
        }

        // Refresh topic mappings (graceful failure handling)
        if (topicMappingSource != null) {
            try {
                logger.debug(
                        "Attempting to refresh topic mappings from: {}",
                        topicMappingSource.getDescription());

                // Force invalidation of S3 topic mapping cache
                topicMappingSource.invalidateCache();

                // Reconfigure topic mappings (will reload from S3)
                configureTopicMappings(serdeProperties);
                refreshed = true;
                logger.info(
                        "Successfully refreshed topic mappings from: {}",
                        topicMappingSource.getDescription());
            } catch (Exception e) {
                // Log warning but don't break existing functionality
                logger.warn(
                        "Failed to refresh topic mappings from: {}. Continuing with existing"
                                + " mappings. Error: {}",
                        topicMappingSource.getDescription(),
                        e.getMessage());
                // Don't throw exception - keep existing mappings working
            }
        }

        return refreshed;
    }

    /**
     * Get information about the descriptor source and topic mapping source
     *
     * @return Source descriptions and last modified times if available
     */
    public Map<String, Object> getSourceInfo() {
        Map<String, Object> info = new HashMap<>();
        if (descriptorSource != null) {
            info.put("descriptorSource", descriptorSource.getDescription());
            info.put("descriptorSupportsRefresh", descriptorSource.supportsRefresh());
            descriptorSource
                    .getLastModified()
                    .ifPresent(
                            lastModified ->
                                    info.put("descriptorLastModified", lastModified.toString()));
        }
        if (topicMappingSource != null) {
            info.put("topicMappingSource", topicMappingSource.getDescription());
            info.put("topicMappingSupportsRefresh", topicMappingSource.supportsRefresh());
            topicMappingSource
                    .getLastModified()
                    .ifPresent(
                            lastModified ->
                                    info.put("topicMappingLastModified", lastModified.toString()));
        }
        return info;
    }

    /** Find a protobuf descriptor by name, trying full name first, then simple name */
    private Descriptors.Descriptor findDescriptorByName(
            Map<String, Descriptors.Descriptor> allDescriptors, String messageName) {
        // Try full name first
        Descriptors.Descriptor descriptor = allDescriptors.get(messageName);
        if (descriptor != null) {
            return descriptor;
        }

        // Fall back to simple name lookup
        for (Descriptors.Descriptor desc : allDescriptors.values()) {
            if (desc.getName().equals(messageName)) {
                return desc;
            }
        }

        return null;
    }

    private void buildFileDescriptorsWithDependencies(
            DescriptorProtos.FileDescriptorSet descriptorSet,
            Map<String, Descriptors.FileDescriptor> tempDescriptors)
            throws Descriptors.DescriptorValidationException {
        boolean progress = true;
        while (progress && tempDescriptors.size() < descriptorSet.getFileCount()) {
            progress = false;
            for (DescriptorProtos.FileDescriptorProto fileDescriptorProto :
                    descriptorSet.getFileList()) {
                if (tempDescriptors.containsKey(fileDescriptorProto.getName())) {
                    continue;
                }

                Descriptors.FileDescriptor[] dependencies =
                        resolveDependencies(fileDescriptorProto, tempDescriptors);
                if (dependencies != null) {
                    Descriptors.FileDescriptor fileDescriptor =
                            Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, dependencies);
                    tempDescriptors.put(fileDescriptorProto.getName(), fileDescriptor);
                    progress = true;
                }
            }
        }
    }

    private Descriptors.FileDescriptor[] resolveDependencies(
            DescriptorProtos.FileDescriptorProto fileDescriptorProto,
            Map<String, Descriptors.FileDescriptor> tempDescriptors) {
        Descriptors.FileDescriptor[] dependencies =
                new Descriptors.FileDescriptor[fileDescriptorProto.getDependencyCount()];

        for (int i = 0; i < fileDescriptorProto.getDependencyCount(); i++) {
            String depName = fileDescriptorProto.getDependency(i);
            Descriptors.FileDescriptor dependency = tempDescriptors.get(depName);
            if (dependency == null) {
                return null; // Not all dependencies resolved yet
            }
            dependencies[i] = dependency;
        }

        return dependencies;
    }
}
