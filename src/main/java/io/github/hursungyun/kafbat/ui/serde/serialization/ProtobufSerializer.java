package io.github.hursungyun.kafbat.ui.serde.serialization;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;

/**
 * Handles serialization from JSON to protobuf messages using JsonFormat.Parser
 */
public class ProtobufSerializer {
    
    private final JsonFormat.Parser jsonParser;
    private final ProtobufMessageValidator validator;
    
    public ProtobufSerializer() {
        this.jsonParser = JsonFormat.parser();
        this.validator = new ProtobufMessageValidator();
    }
    
    /**
     * Serialize JSON input to protobuf byte array using the specified descriptor
     * Uses JsonFormat.Parser for JSON → protobuf conversion
     */
    public byte[] serialize(Descriptors.Descriptor messageDescriptor, String jsonInput) throws Exception {
        // Parse JSON input into DynamicMessage using JsonFormat.Parser
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(messageDescriptor);
        jsonParser.merge(jsonInput, messageBuilder);
        DynamicMessage message = messageBuilder.build();
        
        // Validate required fields (proto2 only)
        validator.validateRequiredFields(message, messageDescriptor);
        
        // For proto3: validate that JSON contains expected keys (even if values are null)
        if (messageDescriptor.getFile().getSyntax() == Descriptors.FileDescriptor.Syntax.PROTO3) {
            validator.validateJsonKeysForProto3(jsonInput, messageDescriptor);
        }
        
        // Convert to byte array
        return message.toByteArray();
    }
}
