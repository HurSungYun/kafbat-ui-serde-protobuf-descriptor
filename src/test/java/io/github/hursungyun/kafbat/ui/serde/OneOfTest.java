package io.github.hursungyun.kafbat.ui.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hursungyun.kafbat.ui.serde.test.NestedProtos;
import io.kafbat.ui.serde.api.DeserializeResult;
import io.kafbat.ui.serde.api.PropertyResolver;
import io.kafbat.ui.serde.api.Serde;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Test suite for protobuf oneOf field support. Tests verify: - Serialization of oneOf messages -
 * Deserialization of oneOf messages - Round-trip serialization (deserialize → serialize → verify)
 */
class OneOfTest {

    @TempDir Path tempDir;

    private ProtobufDescriptorSetSerde serde;
    private PropertyResolver serdeProperties;
    private PropertyResolver clusterProperties;
    private PropertyResolver appProperties;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        serde = new ProtobufDescriptorSetSerde();
        serdeProperties = Mockito.mock(PropertyResolver.class);
        clusterProperties = Mockito.mock(PropertyResolver.class);
        appProperties = Mockito.mock(PropertyResolver.class);
        objectMapper = new ObjectMapper();
    }

    @Test
    void shouldHandleOneOfWithEmailNotificationAndRoundTrip() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Notification");

        String originalJson =
                """
                {
                    "id": "NOTIF-001",
                    "timestamp": 1704067200000,
                    "email": {
                        "recipient": "user@example.com",
                        "subject": "Welcome",
                        "body": {
                            "text": "Welcome to our service",
                            "html": "<p>Welcome to our service</p>",
                            "attachments": [
                                {
                                    "filename": "guide.pdf",
                                    "mimeType": "application/pdf",
                                    "size": 524288
                                }
                            ]
                        }
                    }
                }
                """;

        // Step 1: Serialize original JSON to protobuf
        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(originalJson);

        NestedProtos.Notification notification = NestedProtos.Notification.parseFrom(protobufBytes);
        assertThat(notification.hasEmail()).isTrue();
        assertThat(notification.hasSms()).isFalse();
        assertThat(notification.hasPush()).isFalse();

        // Step 2: Deserialize protobuf to JSON
        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        String deserializedJson = result.getResult();
        JsonNode deserializedNode = objectMapper.readTree(deserializedJson);

        // Verify deserialized JSON has the email field
        assertThat(deserializedNode.has("email")).isTrue();
        assertThat(deserializedNode.get("email").get("recipient").asText())
                .isEqualTo("user@example.com");

        // Step 3: ROUND-TRIP - Serialize the deserialized JSON back to protobuf
        byte[] roundTripProtobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(deserializedJson);

        // Step 4: Verify round-trip protobuf matches original
        NestedProtos.Notification roundTripNotification =
                NestedProtos.Notification.parseFrom(roundTripProtobufBytes);

        assertThat(roundTripNotification.getId()).isEqualTo(notification.getId());
        assertThat(roundTripNotification.getTimestamp()).isEqualTo(notification.getTimestamp());
        assertThat(roundTripNotification.hasEmail()).isTrue();
        assertThat(roundTripNotification.hasSms()).isFalse();
        assertThat(roundTripNotification.hasPush()).isFalse();
        assertThat(roundTripNotification.getEmail().getRecipient())
                .isEqualTo(notification.getEmail().getRecipient());
        assertThat(roundTripNotification.getEmail().getSubject())
                .isEqualTo(notification.getEmail().getSubject());
    }

    @Test
    void shouldHandleOneOfWithSmsNotificationAndRoundTrip() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Notification");

        String originalJson =
                """
                {
                    "id": "NOTIF-002",
                    "timestamp": 1704153600000,
                    "sms": {
                        "phoneNumber": "+1-555-1234",
                        "message": "Your code is 123456",
                        "metadata": {
                            "senderId": "MyApp",
                            "countryCode": "US"
                        }
                    }
                }
                """;

        // Step 1: Serialize original JSON to protobuf
        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(originalJson);

        NestedProtos.Notification notification = NestedProtos.Notification.parseFrom(protobufBytes);
        assertThat(notification.hasSms()).isTrue();
        assertThat(notification.hasEmail()).isFalse();

        // Step 2: Deserialize protobuf to JSON
        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        String deserializedJson = result.getResult();
        JsonNode deserializedNode = objectMapper.readTree(deserializedJson);

        assertThat(deserializedNode.has("sms")).isTrue();
        assertThat(deserializedNode.get("sms").get("phoneNumber").asText())
                .isEqualTo("+1-555-1234");

        // Step 3: ROUND-TRIP - Serialize the deserialized JSON back to protobuf
        byte[] roundTripProtobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(deserializedJson);

        // Step 4: Verify round-trip protobuf matches original
        NestedProtos.Notification roundTripNotification =
                NestedProtos.Notification.parseFrom(roundTripProtobufBytes);

        assertThat(roundTripNotification.hasSms()).isTrue();
        assertThat(roundTripNotification.hasEmail()).isFalse();
        assertThat(roundTripNotification.getSms().getPhoneNumber())
                .isEqualTo(notification.getSms().getPhoneNumber());
        assertThat(roundTripNotification.getSms().getMessage())
                .isEqualTo(notification.getSms().getMessage());
    }

    @Test
    void shouldHandleOneOfWithPushNotificationAndRoundTrip() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Notification");

        String originalJson =
                """
                {
                    "id": "NOTIF-003",
                    "timestamp": 1704240000000,
                    "push": {
                        "deviceToken": "device-token-abc123",
                        "title": "New Message",
                        "body": "You have a new message",
                        "data": {
                            "messageId": "msg-123",
                            "priority": "high"
                        }
                    }
                }
                """;

        // Step 1: Serialize original JSON to protobuf
        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(originalJson);

        NestedProtos.Notification notification = NestedProtos.Notification.parseFrom(protobufBytes);
        assertThat(notification.hasPush()).isTrue();
        assertThat(notification.hasEmail()).isFalse();
        assertThat(notification.hasSms()).isFalse();

        // Step 2: Deserialize protobuf to JSON
        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        String deserializedJson = result.getResult();
        JsonNode deserializedNode = objectMapper.readTree(deserializedJson);

        assertThat(deserializedNode.has("push")).isTrue();
        assertThat(deserializedNode.get("push").get("title").asText()).isEqualTo("New Message");

        // Step 3: ROUND-TRIP - Serialize the deserialized JSON back to protobuf
        byte[] roundTripProtobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(deserializedJson);

        // Step 4: Verify round-trip protobuf matches original
        NestedProtos.Notification roundTripNotification =
                NestedProtos.Notification.parseFrom(roundTripProtobufBytes);

        assertThat(roundTripNotification.hasPush()).isTrue();
        assertThat(roundTripNotification.hasEmail()).isFalse();
        assertThat(roundTripNotification.hasSms()).isFalse();
        assertThat(roundTripNotification.getPush().getDeviceToken())
                .isEqualTo(notification.getPush().getDeviceToken());
        assertThat(roundTripNotification.getPush().getTitle())
                .isEqualTo(notification.getPush().getTitle());
    }

    private void configureSerde(Path descriptorFile, String defaultMessageType) {
        when(serdeProperties.getProperty("descriptor.value.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        mockS3PropertiesEmpty();
        when(serdeProperties.getProperty("message.value.default.type", String.class))
                .thenReturn(Optional.of(defaultMessageType));
        when(serdeProperties.getMapProperty(
                        "topic.mapping.value.local", String.class, String.class))
                .thenReturn(Optional.empty());

        serde.configure(serdeProperties, clusterProperties, appProperties);
    }

    private void mockS3PropertiesEmpty() {
        when(serdeProperties.getProperty("s3.endpoint", String.class)).thenReturn(Optional.empty());
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
