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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class ProtobufDescriptorSetSerdeTest {

    private ProtobufDescriptorSetSerde serde;
    private PropertyResolver serdeProperties;
    private PropertyResolver clusterProperties;
    private PropertyResolver appProperties;
    
    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        serde = new ProtobufDescriptorSetSerde();
        serdeProperties = Mockito.mock(PropertyResolver.class);
        clusterProperties = Mockito.mock(PropertyResolver.class);
        appProperties = Mockito.mock(PropertyResolver.class);
    }

    @Test
    void shouldThrowExceptionWhenDescriptorSetFilePropertyIsMissing() {
        when(serdeProperties.getProperty("protobuf.descriptor.set.file", String.class))
                .thenReturn(Optional.empty());

        assertThatThrownBy(() -> serde.configure(serdeProperties, clusterProperties, appProperties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("protobuf.descriptor.set.file property is required");
    }

    @Test
    void shouldHaveDescription() {
        ProtobufDescriptorSetSerde serde = new ProtobufDescriptorSetSerde();
        
        assertThat(serde.getDescription())
                .isPresent()
                .contains("Protobuf Descriptor Set Serde - deserializes protobuf messages using descriptor set file");
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
        when(serdeProperties.getProperty("protobuf.descriptor.set.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        when(serdeProperties.getProperty("protobuf.message.name", String.class))
                .thenReturn(Optional.of("test.User"));
        when(serdeProperties.getMapProperty("protobuf.topic.message.map", String.class, String.class))
                .thenReturn(Optional.empty());
        
        serde.configure(serdeProperties, clusterProperties, appProperties);
        
        // After configuration - descriptors loaded with default message type
        assertThat(serde.canDeserialize("test-topic", Serde.Target.VALUE)).isTrue();
        assertThat(serde.canSerialize("test-topic", Serde.Target.VALUE)).isFalse(); // Still false - no serialization support
    }

    @Test
    void shouldLoadDescriptorSetAndDeserializeUserMessage() throws Exception {
        // Copy test descriptor set to temp directory
        Path descriptorFile = copyDescriptorSetToTemp();
        
        // Configure serde with User message type
        when(serdeProperties.getProperty("protobuf.descriptor.set.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        when(serdeProperties.getProperty("protobuf.message.name", String.class))
                .thenReturn(Optional.of("User"));
        when(serdeProperties.getMapProperty("protobuf.topic.message.map", String.class, String.class))
                .thenReturn(Optional.empty());
        
        serde.configure(serdeProperties, clusterProperties, appProperties);
        
        // Create a User message
        byte[] userBytes = createUserMessage();
        
        // Deserialize
        DeserializeResult result = serde.deserializer("test-topic", Serde.Target.VALUE)
                .deserialize(null, userBytes);
        
        assertThat(result.getType()).isEqualTo(DeserializeResult.Type.JSON);
        String resultJson = result.getResult();
        
        // Verify it's a valid JSON result (the serde successfully parsed the protobuf)
        assertThat(resultJson).isNotEmpty();
        assertThat(resultJson).startsWith("{");
        assertThat(resultJson).endsWith("}");
        
        Map<String, Object> metadata = result.getAdditionalProperties();
        assertThat(metadata).containsKey("messageType");
        assertThat(metadata).containsKey("file");
    }

    @Test
    void shouldDeserializeOrderMessageWithNestedUser() throws Exception {
        // Copy test descriptor set to temp directory
        Path descriptorFile = copyDescriptorSetToTemp();
        
        // Configure serde with specific Order message type
        when(serdeProperties.getProperty("protobuf.descriptor.set.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        when(serdeProperties.getProperty("protobuf.message.name", String.class))
                .thenReturn(Optional.of("Order")); // Specify Order message type
        when(serdeProperties.getMapProperty("protobuf.topic.message.map", String.class, String.class))
                .thenReturn(Optional.empty());
        
        serde.configure(serdeProperties, clusterProperties, appProperties);
        
        // Create an Order message with nested User
        byte[] orderBytes = createOrderMessage();
        
        // Deserialize
        DeserializeResult result = serde.deserializer("test-topic", null)
                .deserialize(null, orderBytes);
        
        assertThat(result.getType()).isEqualTo(DeserializeResult.Type.JSON);
        String resultJson = result.getResult();
        
        // Verify it's a valid JSON result (the serde successfully parsed the protobuf)
        assertThat(resultJson).isNotEmpty();
        assertThat(resultJson).startsWith("{");
        assertThat(resultJson).endsWith("}");
        
        Map<String, Object> metadata = result.getAdditionalProperties();
        assertThat(metadata).containsKey("messageType");
        assertThat(metadata).containsKey("file");
    }

    @Test
    void shouldThrowExceptionWhenNoMessageTypeConfigured() throws Exception {
        // Copy test descriptor set to temp directory
        Path descriptorFile = copyDescriptorSetToTemp();
        
        // Configure serde without specifying message type
        when(serdeProperties.getProperty("protobuf.descriptor.set.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        when(serdeProperties.getProperty("protobuf.message.name", String.class))
                .thenReturn(Optional.empty()); // No message type specified
        when(serdeProperties.getMapProperty("protobuf.topic.message.map", String.class, String.class))
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
        when(serdeProperties.getProperty("protobuf.descriptor.set.file", String.class))
                .thenReturn(Optional.of("/nonexistent/file.desc"));
        
        assertThatThrownBy(() -> serde.configure(serdeProperties, clusterProperties, appProperties))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to load protobuf descriptor set from");
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
}