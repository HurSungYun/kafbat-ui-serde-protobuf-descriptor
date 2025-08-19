package io.github.hursungyun.kafbat.ui.serde;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.kafbat.ui.serde.api.DeserializeResult;
import io.kafbat.ui.serde.api.PropertyResolver;
import io.kafbat.ui.serde.api.SchemaDescription;
import io.kafbat.ui.serde.api.Serde;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ProtobufDescriptorSetSerde implements Serde {

    private Map<String, Descriptors.FileDescriptor> fileDescriptorMap;
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
        return true;
    }

    @Override
    public boolean canSerialize(String topic, Target target) {
        return true;
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
            try {
                // Try to deserialize with each message type until one succeeds
                for (Descriptors.FileDescriptor fileDescriptor : fileDescriptorMap.values()) {
                    for (Descriptors.Descriptor messageDescriptor : fileDescriptor.getMessageTypes()) {
                        try {
                            DynamicMessage message = DynamicMessage.parseFrom(messageDescriptor, bytes);
                            String jsonString = jsonPrinter.print(message);
                            
                            Map<String, Object> metadata = new HashMap<>();
                            metadata.put("messageType", messageDescriptor.getFullName());
                            metadata.put("file", fileDescriptor.getName());
                            
                            return new DeserializeResult(
                                    jsonString,
                                    DeserializeResult.Type.JSON,
                                    metadata
                            );
                        } catch (Exception e) {
                            // Continue trying other message types
                        }
                    }
                }
                
                // If no message type worked, return raw bytes as hex
                StringBuilder hexString = new StringBuilder();
                for (byte b : bytes) {
                    hexString.append(String.format("%02x", b));
                }
                
                return new DeserializeResult(
                        hexString.toString(),
                        DeserializeResult.Type.STRING,
                        Collections.singletonMap("error", "Could not deserialize with any known message type")
                );
                
            } catch (Exception e) {
                throw new RuntimeException("Deserialization error", e);
            }
        };
    }
}