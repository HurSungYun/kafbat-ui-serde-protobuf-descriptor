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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ProtobufDescriptorSetSerde implements Serde {

    private Map<String, Descriptors.FileDescriptor> fileDescriptorMap;
    private Map<String, Descriptors.Descriptor> topicToMessageDescriptorMap = new HashMap<>();
    private Descriptors.Descriptor defaultMessageDescriptor;
    private String protobufDescriptorSetFile;
    private JsonFormat.Printer jsonPrinter;
    private JsonFormat.Parser jsonParser;

    @Override
    public void configure(PropertyResolver serdeProperties,
                          PropertyResolver clusterProperties,
                          PropertyResolver appProperties) {
        this.protobufDescriptorSetFile = serdeProperties.getProperty("protobuf.descriptor.set.file", String.class)
                .orElseThrow(() -> new IllegalArgumentException("protobuf.descriptor.set.file property is required"));

        try {
            loadDescriptorSet();
            configureTopicMappings(serdeProperties);
            this.jsonPrinter = JsonFormat.printer().includingDefaultValueFields();
            this.jsonParser = JsonFormat.parser();
        } catch (Exception e) {
            throw new RuntimeException("Failed to load protobuf descriptor set from: " + protobufDescriptorSetFile, e);
        }
    }

    private void loadDescriptorSet() throws IOException, Descriptors.DescriptorValidationException {
        try (FileInputStream fis = new FileInputStream(protobufDescriptorSetFile)) {
            DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(fis);
            
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
    }

    private void configureTopicMappings(PropertyResolver serdeProperties) {
        // Get default message type for all topics
        Optional<String> defaultMessageName = serdeProperties.getProperty("protobuf.message.name", String.class);
        
        // Get topic-specific message mappings using simple key:value format
        Optional<Map<String, String>> topicMessageMappings = 
                serdeProperties.getMapProperty("protobuf.topic.message.map", String.class, String.class);
        
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
            // If no default specified, use the first message type found
            this.defaultMessageDescriptor = allDescriptors.values().stream().findFirst().orElse(null);
        }
        
        // Set topic-specific mappings
        if (topicMessageMappings.isPresent()) {
            for (Map.Entry<String, String> entry : topicMessageMappings.get().entrySet()) {
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
        return Optional.of("Protobuf Descriptor Set Serde - deserializes protobuf messages using descriptor set file");
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
        Descriptors.Descriptor messageDescriptor = descriptorFor(topic, target).orElseThrow(
                () -> new IllegalStateException("No descriptor found for topic: " + topic + ", target: " + target));
        
        return (recordHeaders, bytes) -> {
            try {
                DynamicMessage message = DynamicMessage.parseFrom(messageDescriptor, new ByteArrayInputStream(bytes));
                byte[] jsonFromProto = ProtobufSchemaUtils.toJson(message);
                String jsonString = new String(jsonFromProto);
                
                Map<String, Object> metadata = new HashMap<>();
                metadata.put("messageType", messageDescriptor.getFullName());
                
                return new DeserializeResult(
                        jsonString,
                        DeserializeResult.Type.JSON,
                        metadata
                );
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize protobuf message for topic " + topic, e);
            }
        };
    }
}