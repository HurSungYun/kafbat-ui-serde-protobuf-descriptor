package io.github.hursungyun.kafbat.ui.serde.serialization;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Validator for protobuf messages and JSON input
 */
public class ProtobufMessageValidator {
    
    private final ObjectMapper objectMapper;
    
    public ProtobufMessageValidator() {
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * Validate required fields for proto2 messages
     */
    public void validateRequiredFields(DynamicMessage message, Descriptors.Descriptor messageDescriptor) {
        for (Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            if (field.isRequired() && !message.hasField(field)) {
                throw new IllegalArgumentException(
                    "Required field '" + field.getName() + "' is missing in message type '" + messageDescriptor.getFullName() + "'");
            }
        }
    }
    
    /**
     * For proto3: validate that JSON contains expected keys (even if values are null)
     * This helps ensure that the client is aware of all the fields they should be setting
     */
    public void validateJsonKeysForProto3(String jsonInput, Descriptors.Descriptor messageDescriptor) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(jsonInput);
        
        // Check for explicitly provided keys with null values
        for (Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            String jsonFieldName = getJsonFieldName(field);
            
            // In proto3, if a key is explicitly present in JSON (even with null value),
            // we want to track this for validation purposes
            if (jsonNode.has(jsonFieldName)) {
                JsonNode fieldValue = jsonNode.get(jsonFieldName);
                // This is informational - we're just checking that the key exists
                // The actual validation of null values will be handled by the JsonFormat.Parser
                
                // For enum fields, validate the enum value if it's not null
                if (!fieldValue.isNull() && field.getType() == Descriptors.FieldDescriptor.Type.ENUM) {
                    validateEnumValue(field, fieldValue);
                }
            }
        }
    }
    
    /**
     * Validate that all required JSON keys are present in the input
     * Throws detailed error message listing missing keys
     */
    public void validateRequiredKeysPresent(String jsonInput, Descriptors.Descriptor messageDescriptor, List<String> requiredFields) throws Exception {
        if (requiredFields == null || requiredFields.isEmpty()) {
            return;
        }
        
        JsonNode jsonNode = objectMapper.readTree(jsonInput);
        List<String> missingKeys = new ArrayList<>();
        
        for (String requiredFieldName : requiredFields) {
            // Find the field descriptor by name
            Descriptors.FieldDescriptor field = findFieldByName(messageDescriptor, requiredFieldName);
            if (field != null) {
                String jsonFieldName = getJsonFieldName(field);
                if (!jsonNode.has(jsonFieldName)) {
                    missingKeys.add(jsonFieldName + " (" + field.getName() + ")");
                }
            } else {
                // Field name not found in descriptor - add to missing list
                missingKeys.add(requiredFieldName + " (field not found in schema)");
            }
        }
        
        if (!missingKeys.isEmpty()) {
            throw new IllegalArgumentException(
                "Missing required keys in JSON for message type '" + messageDescriptor.getFullName() + "': " +
                String.join(", ", missingKeys) + 
                ". Expected keys: " + requiredFields.stream().collect(Collectors.joining(", "))
            );
        }
    }
    
    /**
     * Validate that specific fields are present in JSON (useful for validation rules)
     */
    public void validateFieldsPresent(String jsonInput, Descriptors.Descriptor messageDescriptor, String... fieldNames) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(jsonInput);
        List<String> missingKeys = new ArrayList<>();
        
        for (String fieldName : fieldNames) {
            Descriptors.FieldDescriptor field = findFieldByName(messageDescriptor, fieldName);
            if (field != null) {
                String jsonFieldName = getJsonFieldName(field);
                if (!jsonNode.has(jsonFieldName)) {
                    missingKeys.add(jsonFieldName + " (" + field.getName() + ")");
                }
            } else {
                missingKeys.add(fieldName + " (field not found in schema)");
            }
        }
        
        if (!missingKeys.isEmpty()) {
            throw new IllegalArgumentException(
                "Missing required keys in JSON for message type '" + messageDescriptor.getFullName() + "': " +
                String.join(", ", missingKeys)
            );
        }
    }
    
    /**
     * Find field descriptor by name (tries both original name and camelCase)
     */
    private Descriptors.FieldDescriptor findFieldByName(Descriptors.Descriptor messageDescriptor, String fieldName) {
        // Try exact match first
        for (Descriptors.FieldDescriptor field : messageDescriptor.getFields()) {
            if (field.getName().equals(fieldName) || getJsonFieldName(field).equals(fieldName)) {
                return field;
            }
        }
        return null;
    }
    
    /**
     * Get the JSON field name for a protobuf field (camelCase only)
     */
    private String getJsonFieldName(Descriptors.FieldDescriptor field) {
        // Use the standard JSON name (which is camelCase by default in protobuf)
        String jsonName = field.getJsonName();
        if (jsonName != null && !jsonName.isEmpty()) {
            return jsonName;
        }
        
        // Fallback to field name as-is (assuming it's already camelCase)
        return field.getName();
    }
    
    /**
     * Validate enum value in JSON
     */
    private void validateEnumValue(Descriptors.FieldDescriptor field, JsonNode fieldValue) {
        if (fieldValue.isTextual()) {
            String enumValueName = fieldValue.asText();
            Descriptors.EnumValueDescriptor enumValue = field.getEnumType().findValueByName(enumValueName);
            if (enumValue == null) {
                throw new IllegalArgumentException(
                    "Invalid enum value '" + enumValueName + "' for field '" + field.getName() + 
                    "' in enum type '" + field.getEnumType().getFullName() + "'");
            }
        } else if (fieldValue.isNumber()) {
            int enumValueNumber = fieldValue.asInt();
            Descriptors.EnumValueDescriptor enumValue = field.getEnumType().findValueByNumber(enumValueNumber);
            if (enumValue == null) {
                throw new IllegalArgumentException(
                    "Invalid enum number '" + enumValueNumber + "' for field '" + field.getName() + 
                    "' in enum type '" + field.getEnumType().getFullName() + "'");
            }
        }
    }
}
