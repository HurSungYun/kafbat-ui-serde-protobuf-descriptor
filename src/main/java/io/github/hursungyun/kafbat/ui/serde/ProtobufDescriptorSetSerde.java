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
        try {
            this.serdeProperties = serdeProperties;
            this.descriptorSource = DescriptorSourceFactory.create(serdeProperties);
            this.topicMappingSource = createTopicMappingSource(serdeProperties);
            loadDescriptorSet();
            configureTopicMappings(serdeProperties);
            this.jsonPrinter = JsonFormat.printer().includingDefaultValueFields();
            this.jsonParser = JsonFormat.parser();
        } catch (Exception e) {
            String sourceDescription = descriptorSource != null ? descriptorSource.getDescription() : "unknown source";
            throw new RuntimeException("Failed to load protobuf descriptor set from: " + sourceDescription, e);
        }
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
        boolean progress = true;
        while (progress && tempDescriptors.size() < descriptorSet.getFileCount()) {
            progress = false;
            for (DescriptorProtos.FileDescriptorProto fileDescriptorProto : descriptorSet.getFileList()) {
                if (tempDescriptors.containsKey(fileDescriptorProto.getName())) {
                    continue;
                }
                
                // Check if all dependencies are resolved
                boolean allDepsResolved = true;
                Descriptors.FileDescriptor[] dependencies = new Descriptors.FileDescriptor[fileDescriptorProto.getDependencyCount()];
                for (int i = 0; i < fileDescriptorProto.getDependencyCount(); i++) {
                    String depName = fileDescriptorProto.getDependency(i);
                    if (!tempDescriptors.containsKey(depName)) {
                        allDepsResolved = false;
                        break;
                    }
                    dependencies[i] = tempDescriptors.get(depName);
                }
                
                if (allDepsResolved) {
                    Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(
                            fileDescriptorProto, dependencies);
                    tempDescriptors.put(fileDescriptorProto.getName(), fileDescriptor);
                    progress = true;
                }
            }
        }
        
        this.fileDescriptorMap = tempDescriptors;
    }

    private S3TopicMappingSource createTopicMappingSource(PropertyResolver properties) {
        // Check for S3 topic mapping configuration
        Optional<String> s3Bucket = properties.getProperty("protobuf.topic.message.map.s3.bucket", String.class);
        Optional<String> s3ObjectKey = properties.getProperty("protobuf.topic.message.map.s3.object.key", String.class);
        
        if (s3Bucket.isPresent() && s3ObjectKey.isPresent()) {
            // Use existing S3 configuration from descriptor source
            if (descriptorSource instanceof S3DescriptorSource) {
                // Reuse the MinIO client from descriptor source - this requires exposing the client
                // For now, we'll create a new client with the same configuration
                return createS3TopicMappingSourceFromProperties(properties, s3Bucket.get(), s3ObjectKey.get());
            } else {
                // Check if we have S3 credentials in properties
                Optional<String> s3Endpoint = properties.getProperty("protobuf.s3.endpoint", String.class);
                if (s3Endpoint.isPresent()) {
                    return createS3TopicMappingSourceFromProperties(properties, s3Bucket.get(), s3ObjectKey.get());
                }
            }
        }
        
        return null;
    }

    private S3TopicMappingSource createS3TopicMappingSourceFromProperties(PropertyResolver properties, String bucket, String objectKey) {
        String endpoint = properties.getProperty("protobuf.s3.endpoint", String.class)
                .orElseThrow(() -> new IllegalArgumentException("protobuf.s3.endpoint is required for S3 topic mapping source"));
        
        // Get S3 credentials (optional for IAM role-based authentication)
        Optional<String> accessKey = properties.getProperty("protobuf.s3.access.key", String.class);
        Optional<String> secretKey = properties.getProperty("protobuf.s3.secret.key", String.class);
        
        // Optional configuration
        String region = properties.getProperty("protobuf.s3.region", String.class).orElse(null);
        boolean secure = properties.getProperty("protobuf.s3.secure", Boolean.class).orElse(true);
        String stsEndpoint = properties.getProperty("protobuf.s3.sts.endpoint", String.class)
                .orElse("https://sts.amazonaws.com");
        Duration refreshInterval = properties.getProperty("protobuf.s3.refresh.interval.seconds", Long.class)
                .map(Duration::ofSeconds)
                .orElse(Duration.ofHours(1));
        
        // Build MinIO client
        MinioClient.Builder clientBuilder = MinioClient.builder()
                .endpoint(endpoint);
        
        // Configure credentials using centralized logic
        DescriptorSourceFactory.configureMinioCredentials(clientBuilder, accessKey, secretKey, stsEndpoint);
        
        if (region != null) {
            clientBuilder.region(region);
        }
        
        // Configure SSL
        if (!secure) {
            if (endpoint.startsWith("https://")) {
                endpoint = endpoint.replace("https://", "http://");
                clientBuilder.endpoint(endpoint);
            }
        }
        
        MinioClient minioClient = clientBuilder.build();
        
        return new S3TopicMappingSource(minioClient, bucket, objectKey, refreshInterval);
    }

    private void configureTopicMappings(PropertyResolver serdeProperties) {
        // Get default message type for all topics
        Optional<String> defaultMessageName = serdeProperties.getProperty("protobuf.message.name", String.class);
        
        // Load topic mappings from S3 first, then override with local properties
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
                serdeProperties.getMapProperty("protobuf.topic.message.map", String.class, String.class);
        if (localTopicMessageMappings.isPresent()) {
            combinedTopicMappings.putAll(localTopicMessageMappings.get());
        }
        
        // Build descriptor map for all message types
        Map<String, Descriptors.Descriptor> allDescriptors = new HashMap<>();
        for (Descriptors.FileDescriptor fileDescriptor : fileDescriptorMap.values()) {
            for (Descriptors.Descriptor messageDescriptor : fileDescriptor.getMessageTypes()) {
                allDescriptors.put(messageDescriptor.getFullName(), messageDescriptor);
            }
        }
        
        // Set default message descriptor
        if (defaultMessageName.isPresent()) {
            this.defaultMessageDescriptor = allDescriptors.get(defaultMessageName.get());
            if (this.defaultMessageDescriptor == null) {
                // Try to find by simple name if full name not found
                for (Descriptors.Descriptor desc : allDescriptors.values()) {
                    if (desc.getName().equals(defaultMessageName.get())) {
                        this.defaultMessageDescriptor = desc;
                        break;
                    }
                }
            }
        } else {
            this.defaultMessageDescriptor = null;
        }
        
        // Set topic-specific mappings (from combined S3 + local mappings)
        if (!combinedTopicMappings.isEmpty()) {
            for (Map.Entry<String, String> entry : combinedTopicMappings.entrySet()) {
                String topic = entry.getKey();
                String messageName = entry.getValue();
                Descriptors.Descriptor descriptor = allDescriptors.get(messageName);
                if (descriptor == null) {
                    // Try to find by simple name if full name not found
                    for (Descriptors.Descriptor desc : allDescriptors.values()) {
                        if (desc.getName().equals(messageName)) {
                            descriptor = desc;
                            break;
                        }
                    }
                }
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
        // This serde only supports deserialization
        return false;
    }

    @Override
    public Serializer serializer(String topic, Target target) {
        return inputString -> {
            throw new UnsupportedOperationException("Serialization not implemented for ProtobufDescriptorSetSerde");
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
}