package io.github.hursungyun.kafbat.ui.serde.serialization;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.kafbat.ui.serde.api.DeserializeResult;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles deserialization from protobuf messages to JSON
 */
public class ProtobufDeserializer {
    
    /**
     * Deserialize protobuf byte array to JSON using the specified descriptor
     * Uses ProtobufSchemaUtils.toJson for protobuf → JSON conversion
     */
    public DeserializeResult deserialize(Descriptors.Descriptor messageDescriptor, byte[] bytes) throws Exception {
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
}
