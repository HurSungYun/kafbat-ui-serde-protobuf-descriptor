package io.github.hursungyun.kafbat.ui.serde;

import io.kafbat.ui.serde.api.PropertyResolver;
import io.kafbat.ui.serde.api.Serde;
import io.github.hursungyun.kafbat.ui.serde.test.UserProtos;
import io.github.hursungyun.kafbat.ui.serde.test.OrderProtos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/**
 * Tests for protobuf message serialization (JSON to protobuf bytes)
 */
class SerializationTest {

    @TempDir
    Path tempDir;

    private ProtobufDescriptorSetSerde serde;
    private PropertyResolver serdeProperties;
    private PropertyResolver clusterProperties;
    private PropertyResolver appProperties;

    @BeforeEach
    void setUp() {
        serde = new ProtobufDescriptorSetSerde();
        serdeProperties = Mockito.mock(PropertyResolver.class);
        clusterProperties = Mockito.mock(PropertyResolver.class);
        appProperties = Mockito.mock(PropertyResolver.class);
    }

    @Test
    void shouldSerializeSimpleUserMessage() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.User");

        // JSON input for User message
        String jsonInput = """
                {
                    "id": 123,
                    "name": "Test User",
                    "email": "test@example.com",
                    "tags": ["tag1", "tag2"]
                }
                """;

        // Serialize JSON to protobuf bytes
        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE)
                .serialize(jsonInput);

        // Verify by deserializing back
        UserProtos.User parsedUser = UserProtos.User.parseFrom(protobufBytes);
        assertThat(parsedUser.getId()).isEqualTo(123);
        assertThat(parsedUser.getName()).isEqualTo("Test User");
        assertThat(parsedUser.getEmail()).isEqualTo("test@example.com");
        assertThat(parsedUser.getTagsList()).containsExactly("tag1", "tag2");
    }

    @Test
    void shouldSerializeOrderMessageWithNestedUser() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Order");

        // JSON input for Order message with nested User
        String jsonInput = """
                {
                    "id": 456,
                    "totalAmount": 99.99,
                    "user": {
                        "id": 123,
                        "name": "Order User",
                        "email": "order@example.com"
                    }
                }
                """;

        // Serialize JSON to protobuf bytes
        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE)
                .serialize(jsonInput);

        // Verify by deserializing back
        OrderProtos.Order parsedOrder = OrderProtos.Order.parseFrom(protobufBytes);
        assertThat(parsedOrder.getId()).isEqualTo(456);
        assertThat(parsedOrder.getTotalAmount()).isEqualTo(99.99);
        assertThat(parsedOrder.getUser().getId()).isEqualTo(123);
        assertThat(parsedOrder.getUser().getName()).isEqualTo("Order User");
        assertThat(parsedOrder.getUser().getEmail()).isEqualTo("order@example.com");
    }

    @Test
    void shouldThrowExceptionForUnknownFields() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.User");

        // JSON with unknown fields should fail
        String jsonWithUnknownFields = """
                {
                    "id": 42,
                    "name": "Valid User",
                    "unknownField": "should fail"
                }
                """;
        
        // Should throw exception for unknown fields
        assertThatThrownBy(() -> serde.serializer("test-topic", Serde.Target.VALUE)
                .serialize(jsonWithUnknownFields))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to serialize JSON to protobuf message");
    }

    @Test
    void shouldThrowExceptionForInvalidJson() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.User");

        // Invalid JSON input
        String invalidJson = "{ invalid json }";
        
        assertThatThrownBy(() -> serde.serializer("test-topic", Serde.Target.VALUE)
                .serialize(invalidJson))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to serialize JSON to protobuf message");
    }

    @Test
    void shouldUseTopicSpecificMessageTypes() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        
        // Configure with topic-specific mappings
        Map<String, String> topicMappings = new HashMap<>();
        topicMappings.put("user-events", "test.User");
        topicMappings.put("order-events", "test.Order");
        
        configureSerdeWithTopicMappings(descriptorFile, topicMappings);

        // Test User serialization for user-events topic
        String userJson = """
                {
                    "id": 789,
                    "name": "Topic User",
                    "email": "topic@example.com"
                }
                """;
        
        byte[] userBytes = serde.serializer("user-events", Serde.Target.VALUE)
                .serialize(userJson);
        UserProtos.User parsedUser = UserProtos.User.parseFrom(userBytes);
        assertThat(parsedUser.getName()).isEqualTo("Topic User");

        // Test Order serialization for order-events topic
        String orderJson = """
                {
                    "id": 999,
                    "totalAmount": 149.99
                }
                """;
        
        byte[] orderBytes = serde.serializer("order-events", Serde.Target.VALUE)
                .serialize(orderJson);
        OrderProtos.Order parsedOrder = OrderProtos.Order.parseFrom(orderBytes);
        assertThat(parsedOrder.getId()).isEqualTo(999);
        assertThat(parsedOrder.getTotalAmount()).isEqualTo(149.99);
    }

    @Test
    void shouldSupportSerializationAfterConfiguration() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.User");

        // Should support serialization after configuration
        assertThat(serde.canSerialize("test-topic", Serde.Target.VALUE)).isTrue();
        assertThat(serde.canSerialize("test-topic", Serde.Target.KEY)).isFalse(); // Keys still not supported
    }

    @Test
    void shouldThrowExceptionWhenNoMessageTypeConfigured() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        
        // Configure without message type
        when(serdeProperties.getProperty("descriptor.value.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        mockS3PropertiesEmpty();
        when(serdeProperties.getProperty("message.value.default.type", String.class))
                .thenReturn(Optional.empty());
        when(serdeProperties.getMapProperty("topic.mapping.value.local", String.class, String.class))
                .thenReturn(Optional.empty());

        serde.configure(serdeProperties, clusterProperties, appProperties);

        // Should not support serialization for unmapped topic
        assertThat(serde.canSerialize("unmapped-topic", Serde.Target.VALUE)).isFalse();

        // Should throw exception when attempting to serialize
        assertThatThrownBy(() -> serde.serializer("unmapped-topic", Serde.Target.VALUE)
                .serialize("{}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No message type configured for topic: unmapped-topic");
    }

    @Test
    void shouldHandleJsonWithNullValues() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.User");

        // JSON with explicit null values for some fields
        String jsonWithNulls = """
                {
                    "id": 123,
                    "name": "Test User",
                    "email": null,
                    "tags": null,
                    "type": null
                }
                """;

        // Should successfully serialize even with null values
        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE)
                .serialize(jsonWithNulls);

        // Verify by deserializing back - null values should be default values
        UserProtos.User parsedUser = UserProtos.User.parseFrom(protobufBytes);
        assertThat(parsedUser.getId()).isEqualTo(123);
        assertThat(parsedUser.getName()).isEqualTo("Test User");
        assertThat(parsedUser.getEmail()).isEmpty(); // Empty string for null string
        assertThat(parsedUser.getTagsList()).isEmpty(); // Empty list for null repeated field
        assertThat(parsedUser.getType()).isEqualTo(UserProtos.UserType.UNKNOWN); // Default enum value
    }

    @Test
    void shouldValidateEnumValues() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.User");

        // Test valid enum value by name
        String jsonWithValidEnum = """
                {
                    "id": 123,
                    "name": "Test User",
                    "type": "ADMIN"
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE)
                .serialize(jsonWithValidEnum);

        UserProtos.User parsedUser = UserProtos.User.parseFrom(protobufBytes);
        assertThat(parsedUser.getType()).isEqualTo(UserProtos.UserType.ADMIN);
    }

    @Test
    void shouldValidateEnumValuesByNumber() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.User");

        // Test valid enum value by number
        String jsonWithValidEnumNumber = """
                {
                    "id": 123,
                    "name": "Test User",
                    "type": 2
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE)
                .serialize(jsonWithValidEnumNumber);

        UserProtos.User parsedUser = UserProtos.User.parseFrom(protobufBytes);
        assertThat(parsedUser.getType()).isEqualTo(UserProtos.UserType.REGULAR);
    }

    @Test
    void shouldThrowExceptionForInvalidEnumValue() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.User");

        // JSON with invalid enum value
        String jsonWithInvalidEnum = """
                {
                    "id": 123,
                    "name": "Test User",
                    "type": "INVALID_TYPE"
                }
                """;

        // Should throw exception for invalid enum value
        assertThatThrownBy(() -> serde.serializer("test-topic", Serde.Target.VALUE)
                .serialize(jsonWithInvalidEnum))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to serialize JSON to protobuf message");
    }

    @Test
    void shouldThrowExceptionForInvalidEnumNumber() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.User");

        // JSON with invalid enum number
        String jsonWithInvalidEnumNumber = """
                {
                    "id": 123,
                    "name": "Test User",
                    "type": 999
                }
                """;

        // Should throw exception for invalid enum number
        assertThatThrownBy(() -> serde.serializer("test-topic", Serde.Target.VALUE)
                .serialize(jsonWithInvalidEnumNumber))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to serialize JSON to protobuf message");
    }

    @Test
    void shouldHandleOrderWithEnumStatus() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Order");

        // JSON with Order containing enum status
        String jsonWithOrderStatus = """
                {
                    "id": 789,
                    "totalAmount": 199.99,
                    "status": "CONFIRMED"
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE)
                .serialize(jsonWithOrderStatus);

        OrderProtos.Order parsedOrder = OrderProtos.Order.parseFrom(protobufBytes);
        assertThat(parsedOrder.getId()).isEqualTo(789);
        assertThat(parsedOrder.getTotalAmount()).isEqualTo(199.99);
        assertThat(parsedOrder.getStatus()).isEqualTo(OrderProtos.OrderStatus.CONFIRMED);
    }

    @Test
    void shouldProvideDetailedErrorForMissingKeys() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        
        // Create a validator to test missing key validation
        var validator = new io.github.hursungyun.kafbat.ui.serde.serialization.ProtobufMessageValidator();
        
        // Load descriptor manually for testing
        try (var is = getClass().getResourceAsStream("/test_descriptors.desc")) {
            var descriptorSet = com.google.protobuf.DescriptorProtos.FileDescriptorSet.parseFrom(is);
            
            // Build user descriptor
            var userFileDescriptor = com.google.protobuf.Descriptors.FileDescriptor.buildFrom(
                descriptorSet.getFileList().stream()
                    .filter(f -> f.getName().equals("user.proto"))
                    .findFirst().get(), 
                new com.google.protobuf.Descriptors.FileDescriptor[0]);
            
            var userDescriptor = userFileDescriptor.findMessageTypeByName("User");
            
            // JSON missing required keys
            String jsonMissingKeys = """
                    {
                        "name": "Test User"
                    }
                    """;
            
            // Test validation with specific required fields
            assertThatThrownBy(() -> validator.validateFieldsPresent(jsonMissingKeys, userDescriptor, "id", "email"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Missing required keys in JSON for message type 'test.User'")
                    .hasMessageContaining("id")
                    .hasMessageContaining("email");
        }
    }

    @Test
    void shouldProvideDetailedErrorForNonExistentFields() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        
        // Create a validator to test missing key validation
        var validator = new io.github.hursungyun.kafbat.ui.serde.serialization.ProtobufMessageValidator();
        
        // Load descriptor manually for testing
        try (var is = getClass().getResourceAsStream("/test_descriptors.desc")) {
            var descriptorSet = com.google.protobuf.DescriptorProtos.FileDescriptorSet.parseFrom(is);
            
            // Build user descriptor
            var userFileDescriptor = com.google.protobuf.Descriptors.FileDescriptor.buildFrom(
                descriptorSet.getFileList().stream()
                    .filter(f -> f.getName().equals("user.proto"))
                    .findFirst().get(), 
                new com.google.protobuf.Descriptors.FileDescriptor[0]);
            
            var userDescriptor = userFileDescriptor.findMessageTypeByName("User");
            
            // JSON with basic fields
            String json = """
                    {
                        "id": 123,
                        "name": "Test User"
                    }
                    """;
            
            // Test validation with non-existent fields
            assertThatThrownBy(() -> validator.validateFieldsPresent(json, userDescriptor, "nonExistentField", "anotherMissingField"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Missing required keys in JSON for message type 'test.User'")
                    .hasMessageContaining("nonExistentField (field not found in schema)")
                    .hasMessageContaining("anotherMissingField (field not found in schema)");
        }
    }

    private void configureSerde(Path descriptorFile, String defaultMessageType) {
        when(serdeProperties.getProperty("descriptor.value.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        mockS3PropertiesEmpty();
        when(serdeProperties.getProperty("message.value.default.type", String.class))
                .thenReturn(Optional.of(defaultMessageType));
        when(serdeProperties.getMapProperty("topic.mapping.value.local", String.class, String.class))
                .thenReturn(Optional.empty());

        serde.configure(serdeProperties, clusterProperties, appProperties);
    }

    private void configureSerdeWithTopicMappings(Path descriptorFile, Map<String, String> topicMappings) {
        when(serdeProperties.getProperty("descriptor.value.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        mockS3PropertiesEmpty();
        when(serdeProperties.getProperty("message.value.default.type", String.class))
                .thenReturn(Optional.empty());
        when(serdeProperties.getMapProperty("topic.mapping.value.local", String.class, String.class))
                .thenReturn(Optional.of(topicMappings));

        serde.configure(serdeProperties, clusterProperties, appProperties);
    }

    private void mockS3PropertiesEmpty() {
        when(serdeProperties.getProperty("s3.endpoint", String.class))
                .thenReturn(Optional.empty());
        when(serdeProperties.getProperty("descriptor.value.s3.bucket", String.class))
                .thenReturn(Optional.empty());
        when(serdeProperties.getProperty("descriptor.value.s3.object.key", String.class))
                .thenReturn(Optional.empty());
    }

    private Path copyDescriptorSetToTemp() throws IOException {
        try (InputStream is = getClass().getResourceAsStream("/test_descriptors.desc")) {
            assertThat(is).isNotNull();
            Path descriptorFile = tempDir.resolve("test_descriptors.desc");
            Files.copy(is, descriptorFile);
            return descriptorFile;
        }
    }
}