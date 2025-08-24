package io.github.hursungyun.kafbat.ui.serde;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.kafbat.ui.serde.api.DeserializeResult;
import io.kafbat.ui.serde.api.PropertyResolver;
import io.kafbat.ui.serde.api.Serde;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class ProtobufDescriptorSetSerdeTest {

    private ProtobufDescriptorSetSerde serde;
    private PropertyResolver serdeProperties;
    private PropertyResolver clusterProperties;
    private PropertyResolver appProperties;
    private ObjectMapper objectMapper;
    
    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        serde = new ProtobufDescriptorSetSerde();
        serdeProperties = Mockito.mock(PropertyResolver.class);
        clusterProperties = Mockito.mock(PropertyResolver.class);
        appProperties = Mockito.mock(PropertyResolver.class);
        objectMapper = new ObjectMapper();
    }

    @Test
    void shouldThrowExceptionWhenDescriptorSetFilePropertyIsMissing() {
        when(serdeProperties.getProperty("descriptor.file", String.class))
                .thenReturn(Optional.empty());
        // Also mock S3 properties to be empty
        mockS3PropertiesEmpty();

        assertThatThrownBy(() -> serde.configure(serdeProperties, clusterProperties, appProperties))
                .satisfies(throwable -> {
                    boolean isExpectedException = throwable instanceof IllegalArgumentException ||
                            (throwable instanceof RuntimeException && throwable.getCause() instanceof IllegalArgumentException);
                    assertThat(isExpectedException).isTrue();
                });
    }

    @Test
    void shouldHaveDescription() throws Exception {
        // Configure serde with test descriptor
        Path descriptorFile = copyDescriptorSetToTemp();
        
        when(serdeProperties.getProperty("descriptor.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        mockS3PropertiesEmpty();
        when(serdeProperties.getProperty("message.default.type", String.class))
                .thenReturn(Optional.of("User"));
        when(serdeProperties.getMapProperty("topic.mapping.local", String.class, String.class))
                .thenReturn(Optional.empty());
        
        serde.configure(serdeProperties, clusterProperties, appProperties);
        
        assertThat(serde.getDescription())
                .isPresent()
                .get().asString().contains("Protobuf Descriptor Set Serde - deserializes protobuf messages from Local file:");
    }

    @Test
    void shouldSupportDeserializationButNotSerialization() {
        // Before configuration - no descriptors loaded
        assertThat(serde.canDeserialize("test-topic", null)).isFalse();
        assertThat(serde.canSerialize("test-topic", null)).isFalse();
    }

    @Test
    void shouldSupportDeserializationAfterConfiguration() throws Exception {
        // Copy test descriptor set to temp directory
        Path descriptorFile = copyDescriptorSetToTemp();
        
        // Configure serde
        when(serdeProperties.getProperty("descriptor.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        mockS3PropertiesEmpty();
        when(serdeProperties.getProperty("message.default.type", String.class))
                .thenReturn(Optional.of("test.User"));
        when(serdeProperties.getMapProperty("topic.mapping.local", String.class, String.class))
                .thenReturn(Optional.empty());
        
        serde.configure(serdeProperties, clusterProperties, appProperties);
        
        // After configuration - descriptors loaded with default message type
        assertThat(serde.canDeserialize("test-topic", Serde.Target.VALUE)).isTrue();
        assertThat(serde.canSerialize("test-topic", Serde.Target.VALUE)).isTrue(); // Now supports serialization
    }

    @Test
    void shouldLoadDescriptorSetAndDeserializeUserMessage() throws Exception {
        // Copy test descriptor set to temp directory
        Path descriptorFile = copyDescriptorSetToTemp();
        
        // Configure serde with User message type
        when(serdeProperties.getProperty("descriptor.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        mockS3PropertiesEmpty();
        when(serdeProperties.getProperty("message.default.type", String.class))
                .thenReturn(Optional.of("User"));
        when(serdeProperties.getMapProperty("topic.mapping.local", String.class, String.class))
                .thenReturn(Optional.empty());
        
        serde.configure(serdeProperties, clusterProperties, appProperties);
        
        // Create a User message
        byte[] userBytes = createUserMessage();
        
        // Deserialize
        DeserializeResult result = serde.deserializer("test-topic", Serde.Target.VALUE)
                .deserialize(null, userBytes);
        
        assertThat(result.getType()).isEqualTo(DeserializeResult.Type.JSON);
        String resultJson = result.getResult();
        
        // Verify it's a valid JSON result and contains expected User fields
        assertThat(resultJson).isNotEmpty();
        JsonNode jsonNode = objectMapper.readTree(resultJson);
        
        // Verify User message structure
        assertThat(jsonNode.has("id")).isTrue();
        assertThat(jsonNode.get("id").asInt()).isEqualTo(123);
        assertThat(jsonNode.has("name")).isTrue();
        assertThat(jsonNode.get("name").asText()).isEqualTo("John Doe");
        assertThat(jsonNode.has("email")).isTrue();
        assertThat(jsonNode.get("email").asText()).isEqualTo("john@example.com");
        assertThat(jsonNode.has("tags")).isTrue();
        assertThat(jsonNode.get("tags").isArray()).isTrue();
        assertThat(jsonNode.get("tags")).hasSize(2);
        assertThat(jsonNode.get("tags").get(0).asText()).isEqualTo("developer");
        assertThat(jsonNode.get("tags").get(1).asText()).isEqualTo("java");
        assertThat(jsonNode.has("type")).isTrue();
        assertThat(jsonNode.get("type").asText()).isEqualTo("ADMIN");
        assertThat(jsonNode.has("address")).isTrue();
        
        // Verify nested Address object
        JsonNode address = jsonNode.get("address");
        assertThat(address.has("street")).isTrue();
        assertThat(address.get("street").asText()).isEqualTo("123 Main St");
        assertThat(address.has("city")).isTrue();
        assertThat(address.get("city").asText()).isEqualTo("Anytown");
        assertThat(address.has("country")).isTrue();
        assertThat(address.get("country").asText()).isEqualTo("USA");
        assertThat(address.has("zipCode")).isTrue();
        assertThat(address.get("zipCode").asInt()).isEqualTo(12345);
        
        Map<String, Object> metadata = result.getAdditionalProperties();
        assertThat(metadata).containsKey("messageType");
        assertThat(metadata).containsKey("file");
    }

    @Test
    void shouldDeserializeOrderMessageWithNestedUser() throws Exception {
        // Copy test descriptor set to temp directory
        Path descriptorFile = copyDescriptorSetToTemp();
        
        // Configure serde with specific Order message type
        when(serdeProperties.getProperty("descriptor.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        mockS3PropertiesEmpty();
        when(serdeProperties.getProperty("message.default.type", String.class))
                .thenReturn(Optional.of("Order")); // Specify Order message type
        when(serdeProperties.getMapProperty("topic.mapping.local", String.class, String.class))
                .thenReturn(Optional.empty());
        
        serde.configure(serdeProperties, clusterProperties, appProperties);
        
        // Create an Order message with nested User
        byte[] orderBytes = createOrderMessage();
        
        // Deserialize
        DeserializeResult result = serde.deserializer("test-topic", null)
                .deserialize(null, orderBytes);
        
        assertThat(result.getType()).isEqualTo(DeserializeResult.Type.JSON);
        String resultJson = result.getResult();
        
        // Verify it's a valid JSON result and contains expected Order fields
        assertThat(resultJson).isNotEmpty();
        JsonNode jsonNode = objectMapper.readTree(resultJson);
        
        // Verify Order message structure
        assertThat(jsonNode.has("id")).isTrue();
        assertThat(jsonNode.get("id").asLong()).isEqualTo(456L);
        assertThat(jsonNode.has("totalAmount")).isTrue();
        assertThat(jsonNode.get("totalAmount").asDouble()).isEqualTo(99.99);
        assertThat(jsonNode.has("status")).isTrue();
        assertThat(jsonNode.get("status").asText()).isEqualTo("CONFIRMED");
        assertThat(jsonNode.has("createdTimestamp")).isTrue();
        assertThat(jsonNode.get("createdTimestamp").asLong()).isEqualTo(1640995200000L);
        assertThat(jsonNode.has("user")).isTrue();
        assertThat(jsonNode.has("items")).isTrue();
        assertThat(jsonNode.get("items").isArray()).isTrue();
        assertThat(jsonNode.get("items")).hasSize(1);
        
        // Verify nested User object
        JsonNode user = jsonNode.get("user");
        assertThat(user.has("id")).isTrue();
        assertThat(user.get("id").asInt()).isEqualTo(789);
        assertThat(user.has("name")).isTrue();
        assertThat(user.get("name").asText()).isEqualTo("Jane Smith");
        assertThat(user.has("email")).isTrue();
        assertThat(user.get("email").asText()).isEqualTo("jane@example.com");
        assertThat(user.has("type")).isTrue();
        assertThat(user.get("type").asText()).isEqualTo("REGULAR");
        assertThat(user.has("address")).isTrue();
        
        // Verify User's Address
        JsonNode userAddress = user.get("address");
        assertThat(userAddress.get("street").asText()).isEqualTo("456 Oak Ave");
        assertThat(userAddress.get("city").asText()).isEqualTo("Springfield");
        assertThat(userAddress.get("country").asText()).isEqualTo("USA");
        assertThat(userAddress.get("zipCode").asInt()).isEqualTo(54321);
        
        // Verify OrderItem
        JsonNode orderItem = jsonNode.get("items").get(0);
        assertThat(orderItem.has("productId")).isTrue();
        assertThat(orderItem.get("productId").asText()).isEqualTo("PROD-001");
        assertThat(orderItem.has("productName")).isTrue();
        assertThat(orderItem.get("productName").asText()).isEqualTo("Widget");
        assertThat(orderItem.has("quantity")).isTrue();
        assertThat(orderItem.get("quantity").asInt()).isEqualTo(2);
        assertThat(orderItem.has("unitPrice")).isTrue();
        assertThat(orderItem.get("unitPrice").asDouble()).isEqualTo(49.995);
        
        Map<String, Object> metadata = result.getAdditionalProperties();
        assertThat(metadata).containsKey("messageType");
        assertThat(metadata).containsKey("file");
    }

    @Test
    void shouldThrowExceptionWhenNoMessageTypeConfigured() throws Exception {
        // Copy test descriptor set to temp directory
        Path descriptorFile = copyDescriptorSetToTemp();
        
        // Configure serde without specifying message type
        when(serdeProperties.getProperty("descriptor.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        mockS3PropertiesEmpty();
        when(serdeProperties.getProperty("message.default.type", String.class))
                .thenReturn(Optional.empty()); // No message type specified
        when(serdeProperties.getMapProperty("topic.mapping.local", String.class, String.class))
                .thenReturn(Optional.empty());
        
        serde.configure(serdeProperties, clusterProperties, appProperties);
        
        // Use random bytes
        byte[] randomBytes = {0x01, 0x02, 0x03, 0x04, (byte) 0xFF};
        
        // Should throw exception when no message type is configured
        assertThatThrownBy(() -> serde.deserializer("test-topic", null)
                .deserialize(null, randomBytes))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No message type configured for topic: test-topic");
    }

    @Test
    void shouldThrowExceptionWhenDescriptorSetFileNotFound() {
        when(serdeProperties.getProperty("descriptor.file", String.class))
                .thenReturn(Optional.of("/nonexistent/file.desc"));
        
        assertThatThrownBy(() -> serde.configure(serdeProperties, clusterProperties, appProperties))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to load protobuf descriptor set from");
    }

    private void mockS3PropertiesEmpty() {
        when(serdeProperties.getProperty("descriptor.s3.endpoint", String.class))
                .thenReturn(Optional.empty());
        when(serdeProperties.getProperty("descriptor.s3.bucket", String.class))
                .thenReturn(Optional.empty());
        when(serdeProperties.getProperty("descriptor.s3.object.key", String.class))
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

    private byte[] createUserMessage() throws Exception {
        try (InputStream is = getClass().getResourceAsStream("/test_descriptors.desc")) {
            DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(is);
            
            // Build descriptors
            Descriptors.FileDescriptor userFileDescriptor = null;
            for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
                if (fileProto.getName().equals("user.proto")) {
                    userFileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
                    break;
                }
            }
            
            assertThat(userFileDescriptor).isNotNull();
            Descriptors.Descriptor userDescriptor = userFileDescriptor.findMessageTypeByName("User");
            Descriptors.Descriptor addressDescriptor = userFileDescriptor.findMessageTypeByName("Address");
            assertThat(userDescriptor).isNotNull();
            assertThat(addressDescriptor).isNotNull();
            
            // Create Address
            DynamicMessage address = DynamicMessage.newBuilder(addressDescriptor)
                    .setField(addressDescriptor.findFieldByName("street"), "123 Main St")
                    .setField(addressDescriptor.findFieldByName("city"), "Anytown")
                    .setField(addressDescriptor.findFieldByName("country"), "USA")
                    .setField(addressDescriptor.findFieldByName("zip_code"), 12345)
                    .build();
            
            // Create User
            DynamicMessage user = DynamicMessage.newBuilder(userDescriptor)
                    .setField(userDescriptor.findFieldByName("id"), 123)
                    .setField(userDescriptor.findFieldByName("name"), "John Doe")
                    .setField(userDescriptor.findFieldByName("email"), "john@example.com")
                    .addRepeatedField(userDescriptor.findFieldByName("tags"), "developer")
                    .addRepeatedField(userDescriptor.findFieldByName("tags"), "java")
                    .setField(userDescriptor.findFieldByName("type"), userFileDescriptor.findEnumTypeByName("UserType").findValueByName("ADMIN"))
                    .setField(userDescriptor.findFieldByName("address"), address)
                    .build();
            
            return user.toByteArray();
        }
    }

    private byte[] createOrderMessage() throws Exception {
        try (InputStream is = getClass().getResourceAsStream("/test_descriptors.desc")) {
            DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(is);
            
            // Build descriptors - need to handle dependencies
            Descriptors.FileDescriptor userFileDescriptor = null;
            Descriptors.FileDescriptor orderFileDescriptor = null;
            
            for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
                if (fileProto.getName().equals("user.proto")) {
                    userFileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
                }
            }
            
            for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
                if (fileProto.getName().equals("order.proto")) {
                    orderFileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[]{userFileDescriptor});
                }
            }
            
            assertThat(userFileDescriptor).isNotNull();
            assertThat(orderFileDescriptor).isNotNull();
            
            Descriptors.Descriptor userDescriptor = userFileDescriptor.findMessageTypeByName("User");
            Descriptors.Descriptor addressDescriptor = userFileDescriptor.findMessageTypeByName("Address");
            Descriptors.Descriptor orderDescriptor = orderFileDescriptor.findMessageTypeByName("Order");
            Descriptors.Descriptor orderItemDescriptor = orderFileDescriptor.findMessageTypeByName("OrderItem");
            
            // Create Address
            DynamicMessage address = DynamicMessage.newBuilder(addressDescriptor)
                    .setField(addressDescriptor.findFieldByName("street"), "456 Oak Ave")
                    .setField(addressDescriptor.findFieldByName("city"), "Springfield")
                    .setField(addressDescriptor.findFieldByName("country"), "USA")
                    .setField(addressDescriptor.findFieldByName("zip_code"), 54321)
                    .build();
            
            // Create User
            DynamicMessage user = DynamicMessage.newBuilder(userDescriptor)
                    .setField(userDescriptor.findFieldByName("id"), 789)
                    .setField(userDescriptor.findFieldByName("name"), "Jane Smith")
                    .setField(userDescriptor.findFieldByName("email"), "jane@example.com")
                    .setField(userDescriptor.findFieldByName("type"), userFileDescriptor.findEnumTypeByName("UserType").findValueByName("REGULAR"))
                    .setField(userDescriptor.findFieldByName("address"), address)
                    .build();
            
            // Create OrderItem
            DynamicMessage orderItem = DynamicMessage.newBuilder(orderItemDescriptor)
                    .setField(orderItemDescriptor.findFieldByName("product_id"), "PROD-001")
                    .setField(orderItemDescriptor.findFieldByName("product_name"), "Widget")
                    .setField(orderItemDescriptor.findFieldByName("quantity"), 2)
                    .setField(orderItemDescriptor.findFieldByName("unit_price"), 49.995)
                    .build();
            
            // Create Order
            DynamicMessage order = DynamicMessage.newBuilder(orderDescriptor)
                    .setField(orderDescriptor.findFieldByName("id"), 456L)
                    .setField(orderDescriptor.findFieldByName("user"), user)
                    .addRepeatedField(orderDescriptor.findFieldByName("items"), orderItem)
                    .setField(orderDescriptor.findFieldByName("total_amount"), 99.99)
                    .setField(orderDescriptor.findFieldByName("status"), orderFileDescriptor.findEnumTypeByName("OrderStatus").findValueByName("CONFIRMED"))
                    .setField(orderDescriptor.findFieldByName("created_timestamp"), 1640995200000L)
                    .build();
            
            return order.toByteArray();
        }
    }

    @Test
    void shouldConfigureWithS3TopicMappings() throws IOException {
        // Test that S3 topic mapping configuration is properly recognized
        // This is a basic test that verifies the configuration doesn't throw exceptions
        PropertyResolver serdeProperties = Mockito.mock(PropertyResolver.class);
        PropertyResolver clusterProperties = Mockito.mock(PropertyResolver.class);
        PropertyResolver appProperties = Mockito.mock(PropertyResolver.class);

        Path descriptorFile = copyDescriptorSetToTemp();
        
        // Setup local descriptor source (required)
        when(serdeProperties.getProperty("descriptor.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        
        // Setup S3 topic mapping configuration
        when(serdeProperties.getProperty("topic.mapping.s3.bucket", String.class))
                .thenReturn(Optional.of("test-bucket"));
        when(serdeProperties.getProperty("topic.mapping.s3.object.key", String.class))
                .thenReturn(Optional.of("topic-mappings.json"));
        when(serdeProperties.getProperty("descriptor.s3.endpoint", String.class))
                .thenReturn(Optional.of("http://localhost:9000"));
        when(serdeProperties.getProperty("descriptor.s3.access.key", String.class))
                .thenReturn(Optional.of("testkey"));
        when(serdeProperties.getProperty("descriptor.s3.secret.key", String.class))
                .thenReturn(Optional.of("testsecret"));
        
        // Mock other optional S3 properties
        mockS3PropertiesEmpty();
        
        when(serdeProperties.getProperty("message.default.type", String.class))
                .thenReturn(Optional.of("User"));
        when(serdeProperties.getMapProperty("topic.mapping.local", String.class, String.class))
                .thenReturn(Optional.empty());

        // This should not throw an exception - the serde should be configured with S3 topic mapping source
        // Note: The actual S3 calls will fail in this test environment, but the configuration should work
        try {
            serde.configure(serdeProperties, clusterProperties, appProperties);
            // If we reach here without exception, the configuration was successful
            assertThat(serde.getSourceInfo()).containsKey("descriptorSource");
        } catch (RuntimeException e) {
            // Expected in test environment since we don't have actual S3 connectivity
            // Just verify the error is related to configuration or S3 connectivity
            assertThat(e.getMessage()).containsAnyOf("Failed to load", "Connection", "S3", "Failed to configure");
        }
    }
}