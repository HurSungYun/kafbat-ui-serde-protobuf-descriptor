package io.github.hursungyun.kafbat.ui.serde.serialization;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;

/** Handles serialization from JSON to protobuf messages using JsonFormat.Parser */
public class ProtobufSerializer {

    private final JsonFormat.Parser jsonParser;
    private final ProtobufMessageValidator validator;
    private final boolean strictFieldValidation;

    public ProtobufSerializer() {
        this(true); // Default to strict mode
    }

    public ProtobufSerializer(boolean strictFieldValidation) {
        this.strictFieldValidation = strictFieldValidation;
        this.jsonParser =
                strictFieldValidation
                        ? JsonFormat.parser()
                        : JsonFormat.parser().ignoringUnknownFields();
        this.validator = new ProtobufMessageValidator();
    }

    /**
     * Serialize JSON input to protobuf byte array using the specified descriptor Uses
     * JsonFormat.Parser for JSON → protobuf conversion
     */
    public byte[] serialize(Descriptors.Descriptor messageDescriptor, String jsonInput)
            throws Exception {
        // Strict validation if enabled
        if (strictFieldValidation) {
            validator.validateAllFieldsPresent(jsonInput, messageDescriptor);
        }

        // Parse JSON input into DynamicMessage using JsonFormat.Parser
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(messageDescriptor);
        jsonParser.merge(jsonInput, messageBuilder);
        DynamicMessage message = messageBuilder.build();

        // Always validate required fields (proto2 only)
        validator.validateRequiredFields(message, messageDescriptor);

        // Convert to byte array
        return message.toByteArray();
    }
}
