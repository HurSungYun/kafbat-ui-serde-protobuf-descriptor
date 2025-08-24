package io.github.hursungyun.kafbat.ui.serde;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.kafbat.ui.serde.api.DeserializeResult;
import io.kafbat.ui.serde.api.PropertyResolver;
import io.kafbat.ui.serde.api.SchemaDescription;
import io.kafbat.ui.serde.api.Serde;
import io.github.hursungyun.kafbat.ui.serde.auth.MinioClientFactory;
import io.github.hursungyun.kafbat.ui.serde.auth.S3Configuration;
import io.github.hursungyun.kafbat.ui.serde.sources.DescriptorSource;
import io.github.hursungyun.kafbat.ui.serde.sources.DescriptorSourceFactory;
import io.github.hursungyun.kafbat.ui.serde.sources.S3DescriptorSource;
import io.github.hursungyun.kafbat.ui.serde.sources.S3TopicMappingSource;
import io.minio.MinioClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ProtobufDescriptorSetSerde implements Serde {

    private Map<String, Descriptors.FileDescriptor> fileDescriptorMap;
    private Map<String, Descriptors.Descriptor> topicToMessageDescriptorMap = new HashMap<>();
    private Descriptors.Descriptor defaultMessageDescriptor;
    private DescriptorSource descriptorSource;
    private S3TopicMappingSource topicMappingSource;
    private PropertyResolver serdeProperties;
    private JsonFormat.Printer jsonPrinter;
    private JsonFormat.Parser jsonParser;

    @Override
    public void configure(PropertyResolver serdeProperties,
                          PropertyResolver clusterProperties,
                          PropertyResolver appProperties) {
        this.serdeProperties = serdeProperties;
        
        try {
            initializeDescriptorSources(serdeProperties);
            initializeDescriptors();
            initializeJsonFormatters();
        } catch (IOException | Descriptors.DescriptorValidationException e) {
            String sourceDescription = descriptorSource != null ? descriptorSource.getDescription() : "unknown source";
            throw new RuntimeException("Failed to load protobuf descriptor set from: " + sourceDescription, e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure ProtobufDescriptorSetSerde", e);
        }
    }

    private void initializeDescriptorSources(PropertyResolver serdeProperties) {
        this.descriptorSource = DescriptorSourceFactory.create(serdeProperties);
        this.topicMappingSource = createTopicMappingSource(serdeProperties);
    }

    private void initializeDescriptors() throws IOException, Descriptors.DescriptorValidationException {
        loadDescriptorSet();
        configureTopicMappings(serdeProperties);
    }

    private void initializeJsonFormatters() {
        this.jsonPrinter = JsonFormat.printer().includingDefaultValueFields();
        this.jsonParser = JsonFormat.parser();
    }

    private void loadDescriptorSet() throws IOException, Descriptors.DescriptorValidationException {
        DescriptorProtos.FileDescriptorSet descriptorSet = descriptorSource.loadDescriptorSet();

        fileDescriptorMap = new HashMap<>();
        Map<String, Descriptors.FileDescriptor> tempDescriptors = new HashMap<>();

        // First pass: create all FileDescriptors without dependencies
        for (DescriptorProtos.FileDescriptorProto fileDescriptorProto : descriptorSet.getFileList()) {
            if (fileDescriptorProto.getDependencyCount() == 0) {
                Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(
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
        Optional<String> s3Bucket = properties.getProperty("topic.mapping.s3.bucket", String.class);
        Optional<String> s3ObjectKey = properties.getProperty("topic.mapping.s3.object.key", String.class);

        if (s3Bucket.isPresent() && s3ObjectKey.isPresent()) {
            // Use existing S3 configuration from descriptor source
            if (descriptorSource instanceof S3DescriptorSource) {
                // Reuse the MinIO client from descriptor source - this requires exposing the client
                // For now, we'll create a new client with the same configuration
                return createS3TopicMappingSourceFromProperties(properties, s3Bucket.get(), s3ObjectKey.get());
            } else {
                // Check if we have S3 credentials in properties
                Optional<String> s3Endpoint = properties.getProperty("descriptor.s3.endpoint", String.class);
                if (s3Endpoint.isPresent()) {
                    return createS3TopicMappingSourceFromProperties(properties, s3Bucket.get(), s3ObjectKey.get());
                }
            }
        }

        return null;
    }

    private S3TopicMappingSource createS3TopicMappingSourceFromProperties(PropertyResolver properties, String bucket, String objectKey) {
        // Create S3 configuration from properties (reusing same S3 config as descriptors)
        S3Configuration config = S3Configuration.fromProperties(properties);

        // Create MinIO client using the factory
        MinioClient minioClient = MinioClientFactory.create(config);

        return new S3TopicMappingSource(minioClient, bucket, objectKey, config.getRefreshInterval());
    }

    private void configureTopicMappings(PropertyResolver serdeProperties) {
        Optional<String> defaultMessageName = serdeProperties.getProperty("message.default.type", String.class);
        Map<String, String> combinedTopicMappings = loadCombinedTopicMappings(serdeProperties);
        Map<String, Descriptors.Descriptor> allDescriptors = buildDescriptorMap();
        
        configureDefaultMessageDescriptor(defaultMessageName, allDescriptors);
        configureTopicSpecificMappings(combinedTopicMappings, allDescriptors);
    }

    private Map<String, String> loadCombinedTopicMappings(PropertyResolver serdeProperties) {
        Map<String, String> combinedTopicMappings = new HashMap<>();

        // Load from S3 if available
        if (topicMappingSource != null) {
            try {
                Map<String, String> s3TopicMappings = topicMappingSource.loadTopicMappings();
                combinedTopicMappings.putAll(s3TopicMappings);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load topic mappings from S3: " + topicMappingSource.getDescription(), e);
            }
        }

        // Get topic-specific message mappings from local configuration (overrides S3)
        Optional<Map<String, String>> localTopicMessageMappings =
                serdeProperties.getMapProperty("topic.mapping.local", String.class, String.class);
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

    private void configureDefaultMessageDescriptor(Optional<String> defaultMessageName, 
                                                  Map<String, Descriptors.Descriptor> allDescriptors) {
        if (defaultMessageName.isPresent()) {
            this.defaultMessageDescriptor = findDescriptorByName(allDescriptors, defaultMessageName.get());
        } else {
            this.defaultMessageDescriptor = null;
        }
    }

    private void configureTopicSpecificMappings(Map<String, String> combinedTopicMappings, 
                                               Map<String, Descriptors.Descriptor> allDescriptors) {
        if (!combinedTopicMappings.isEmpty()) {
            for (Map.Entry<String, String> entry : combinedTopicMappings.entrySet()) {
                String topic = entry.getKey();
                String messageName = entry.getValue();
                Descriptors.Descriptor descriptor = findDescriptorByName(allDescriptors, messageName);
                if (descriptor != null) {
                    topicToMessageDescriptorMap.put(topic, descriptor);
                }
            }
        }
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
        String sourceDescription = descriptorSource != null ? descriptorSource.getDescription() : "unknown source";
        return Optional.of("Protobuf Descriptor Set Serde - deserializes protobuf messages from " + sourceDescription);
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
                throw new IllegalStateException("No message type configured for topic: " + topic + ", target: " + target);
            }
            
            try {
                return serializeWithDescriptor(descriptorOpt.get(), inputString);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize JSON to protobuf message for topic " + topic 
                        + " with configured message type " + descriptorOpt.get().getFullName(), e);
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
                    return deserializeWithDescriptor(specificDescriptor.get(), bytes);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to deserialize protobuf message for topic " + topic
                            + " with configured message type " + specificDescriptor.get().getFullName(), e);
                }
            }

            // No specific descriptor configured - cannot deserialize
            throw new IllegalStateException("No message type configured for topic: " + topic + ", target: " + target);
        };
    }

    private DeserializeResult deserializeWithDescriptor(Descriptors.Descriptor messageDescriptor, byte[] bytes) throws Exception {
        DynamicMessage message = DynamicMessage.parseFrom(messageDescriptor, new ByteArrayInputStream(bytes));
        byte[] jsonFromProto = ProtobufSchemaUtils.toJson(message);
        String jsonString = new String(jsonFromProto);

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("messageType", messageDescriptor.getFullName());
        metadata.put("file", messageDescriptor.getFile().getName());

        return new DeserializeResult(
                jsonString,
                DeserializeResult.Type.JSON,
                metadata
        );
    }

    private byte[] serializeWithDescriptor(Descriptors.Descriptor messageDescriptor, String jsonInput) throws Exception {
        // Parse JSON input into DynamicMessage
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(messageDescriptor);
        jsonParser.merge(jsonInput, messageBuilder);
        DynamicMessage message = messageBuilder.build();
        
        // Validate required fields (proto2 only)
        validateRequiredFields(message, messageDescriptor);
        
        // Convert to byte array
        return message.toByteArray();
    }
    
    private void validateRequiredFields(DynamicMessage message, Descriptors.Descriptor messageDescriptor) {
        for (Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            if (field.isRequired() && !message.hasField(field)) {
                throw new IllegalArgumentException(
                    "Required field '" + field.getName() + "' is missing in message type '" + messageDescriptor.getFullName() + "'");
            }
        }
    }

    /**
     * Refresh the descriptor set and topic mappings from source if they support refresh
     * @return true if refresh was attempted, false if not supported
     */
    public boolean refreshDescriptors() {
        boolean refreshed = false;

        // Refresh descriptors
        if (descriptorSource != null && descriptorSource.supportsRefresh()) {
            try {
                // Force invalidation if S3 source
                if (descriptorSource instanceof S3DescriptorSource) {
                    ((S3DescriptorSource) descriptorSource).invalidateCache();
                }

                // Reload descriptors
                loadDescriptorSet();
                refreshed = true;
            } catch (Exception e) {
                throw new RuntimeException("Failed to refresh descriptor set from: " + descriptorSource.getDescription(), e);
            }
        }

        // Refresh topic mappings
        if (topicMappingSource != null) {
            try {
                // Force invalidation of S3 topic mapping cache
                topicMappingSource.invalidateCache();

                // Reconfigure topic mappings (will reload from S3)
                configureTopicMappings(serdeProperties);
                refreshed = true;
            } catch (Exception e) {
                throw new RuntimeException("Failed to refresh topic mappings from: " + topicMappingSource.getDescription(), e);
            }
        }

        return refreshed;
    }

    /**
     * Get information about the descriptor source and topic mapping source
     * @return Source descriptions and last modified times if available
     */
    public Map<String, Object> getSourceInfo() {
        Map<String, Object> info = new HashMap<>();
        if (descriptorSource != null) {
            info.put("descriptorSource", descriptorSource.getDescription());
            info.put("descriptorSupportsRefresh", descriptorSource.supportsRefresh());
            descriptorSource.getLastModified().ifPresent(lastModified ->
                info.put("descriptorLastModified", lastModified.toString()));
        }
        if (topicMappingSource != null) {
            info.put("topicMappingSource", topicMappingSource.getDescription());
            info.put("topicMappingSupportsRefresh", topicMappingSource.supportsRefresh());
            topicMappingSource.getLastModified().ifPresent(lastModified ->
                info.put("topicMappingLastModified", lastModified.toString()));
        }
        return info;
    }

    /**
     * Find a protobuf descriptor by name, trying full name first, then simple name
     */
    private Descriptors.Descriptor findDescriptorByName(Map<String, Descriptors.Descriptor> allDescriptors, String messageName) {
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

    private void buildFileDescriptorsWithDependencies(DescriptorProtos.FileDescriptorSet descriptorSet, 
                                                      Map<String, Descriptors.FileDescriptor> tempDescriptors) 
                                                      throws Descriptors.DescriptorValidationException {
        boolean progress = true;
        while (progress && tempDescriptors.size() < descriptorSet.getFileCount()) {
            progress = false;
            for (DescriptorProtos.FileDescriptorProto fileDescriptorProto : descriptorSet.getFileList()) {
                if (tempDescriptors.containsKey(fileDescriptorProto.getName())) {
                    continue;
                }

                Descriptors.FileDescriptor[] dependencies = resolveDependencies(fileDescriptorProto, tempDescriptors);
                if (dependencies != null) {
                    Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(
                            fileDescriptorProto, dependencies);
                    tempDescriptors.put(fileDescriptorProto.getName(), fileDescriptor);
                    progress = true;
                }
            }
        }
    }

    private Descriptors.FileDescriptor[] resolveDependencies(DescriptorProtos.FileDescriptorProto fileDescriptorProto, 
                                                            Map<String, Descriptors.FileDescriptor> tempDescriptors) {
        Descriptors.FileDescriptor[] dependencies = new Descriptors.FileDescriptor[fileDescriptorProto.getDependencyCount()];
        
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