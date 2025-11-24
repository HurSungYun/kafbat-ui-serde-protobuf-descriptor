package io.github.hursungyun.kafbat.ui.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import io.github.hursungyun.kafbat.ui.serde.test.WktProtos;
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
 * Test suite for Google Well-Known Types (WKT) support in protobuf serialization. Focuses on
 * Timestamp type which uses RFC 3339 format (e.g., "2024-01-01T12:00:00.123456789Z").
 */
class GoogleWellKnownTypesTest {

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
    void shouldHandleTimestampWithNanosecondPrecision() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Event");

        // RFC 3339 format with nanosecond precision
        String eventJson =
                """
                {
                    "id": "evt-001",
                    "name": "System Alert",
                    "createdAt": "2024-01-15T10:30:45.123456789Z",
                    "updatedAt": "2024-01-15T10:35:22.987654321Z",
                    "scheduledAt": "2024-01-15T11:00:00.500000000Z",
                    "type": "EVENT_TYPE_ALERT",
                    "status": "EVENT_STATUS_ACTIVE",
                    "priority": "10",
                    "description": "High priority system alert",
                    "userId": "user-123"
                }
                """;

        // Serialize JSON to protobuf
        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(eventJson);

        // Verify protobuf structure
        WktProtos.Event event = WktProtos.Event.parseFrom(protobufBytes);
        assertThat(event.getId()).isEqualTo("evt-001");
        assertThat(event.getName()).isEqualTo("System Alert");
        assertThat(event.getType()).isEqualTo(WktProtos.EventType.EVENT_TYPE_ALERT);
        assertThat(event.getStatus()).isEqualTo(WktProtos.EventStatus.EVENT_STATUS_ACTIVE);
        assertThat(event.getPriority()).isEqualTo(10L);

        // Verify Timestamp fields with nanosecond precision
        Timestamp createdAt = event.getCreatedAt();
        assertThat(createdAt.getNanos()).isEqualTo(123456789);

        Timestamp updatedAt = event.getUpdatedAt();
        assertThat(updatedAt.getNanos()).isEqualTo(987654321);

        Timestamp scheduledAt = event.getScheduledAt();
        assertThat(scheduledAt.getNanos()).isEqualTo(500000000);

        // Deserialize back to JSON
        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        JsonNode jsonNode = objectMapper.readTree(result.getResult());

        // Verify JSON structure
        assertThat(jsonNode.get("id").asText()).isEqualTo("evt-001");
        assertThat(jsonNode.get("name").asText()).isEqualTo("System Alert");
        assertThat(jsonNode.get("type").asText()).isEqualTo("EVENT_TYPE_ALERT");
        assertThat(jsonNode.get("priority").asLong()).isEqualTo(10L);

        // Verify Timestamp fields are properly formatted back to RFC 3339
        assertThat(jsonNode.get("createdAt").asText()).isEqualTo("2024-01-15T10:30:45.123456789Z");
        assertThat(jsonNode.get("updatedAt").asText()).isEqualTo("2024-01-15T10:35:22.987654321Z");
        assertThat(jsonNode.get("scheduledAt").asText()).isEqualTo("2024-01-15T11:00:00.500Z");
    }

    @Test
    void shouldHandleTimestampWithoutNanoseconds() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Event");

        // RFC 3339 format without nanoseconds (second precision)
        String eventJson =
                """
                {
                    "id": "evt-002",
                    "name": "Daily Reminder",
                    "createdAt": "2024-01-20T09:00:00Z",
                    "updatedAt": "2024-01-20T09:00:00Z",
                    "scheduledAt": "2024-01-20T10:00:00Z",
                    "type": "EVENT_TYPE_REMINDER",
                    "status": "EVENT_STATUS_PENDING",
                    "priority": "5",
                    "description": null,
                    "userId": null
                }
                """;

        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(eventJson);

        WktProtos.Event event = WktProtos.Event.parseFrom(protobufBytes);

        // Verify Timestamp fields - nanos should be 0 when not specified
        assertThat(event.getCreatedAt().getNanos()).isEqualTo(0);
        assertThat(event.getScheduledAt().getNanos()).isEqualTo(0);

        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        JsonNode jsonNode = objectMapper.readTree(result.getResult());
        assertThat(jsonNode.get("createdAt").asText()).isEqualTo("2024-01-20T09:00:00Z");
        assertThat(jsonNode.get("scheduledAt").asText()).isEqualTo("2024-01-20T10:00:00Z");
    }

    @Test
    void shouldHandleTimestampWithMillisecondPrecision() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Event");

        // RFC 3339 format with millisecond precision
        String eventJson =
                """
                {
                    "id": "evt-003",
                    "name": "API Notification",
                    "createdAt": "2024-02-01T15:30:45.123Z",
                    "updatedAt": "2024-02-01T15:30:45.500Z",
                    "scheduledAt": "2024-02-01T15:30:45.999Z",
                    "type": "EVENT_TYPE_NOTIFICATION",
                    "status": "EVENT_STATUS_COMPLETED",
                    "priority": "3",
                    "description": null,
                    "userId": null
                }
                """;

        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(eventJson);

        WktProtos.Event event = WktProtos.Event.parseFrom(protobufBytes);

        // Verify milliseconds are converted to nanoseconds correctly
        assertThat(event.getCreatedAt().getNanos()).isEqualTo(123000000); // 123ms = 123000000ns
        assertThat(event.getUpdatedAt().getNanos()).isEqualTo(500000000); // 500ms = 500000000ns
        assertThat(event.getScheduledAt().getNanos()).isEqualTo(999000000); // 999ms = 999000000ns

        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        JsonNode jsonNode = objectMapper.readTree(result.getResult());
        assertThat(jsonNode.get("createdAt").asText()).isEqualTo("2024-02-01T15:30:45.123Z");
        assertThat(jsonNode.get("updatedAt").asText()).isEqualTo("2024-02-01T15:30:45.500Z");
        assertThat(jsonNode.get("scheduledAt").asText()).isEqualTo("2024-02-01T15:30:45.999Z");
    }

    @Test
    void shouldHandleOptionalFieldsNotSet() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Event");

        // Test with optional fields not set
        String eventJson =
                """
                {
                    "id": "evt-004",
                    "name": "Background Task",
                    "createdAt": "2024-03-01T12:00:00Z",
                    "updatedAt": "2024-03-01T12:00:00Z",
                    "scheduledAt": "2024-03-01T13:00:00Z",
                    "type": "EVENT_TYPE_NOTIFICATION",
                    "status": "EVENT_STATUS_ACTIVE",
                    "priority": "1",
                    "description": null,
                    "userId": null
                }
                """;

        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(eventJson);

        WktProtos.Event event = WktProtos.Event.parseFrom(protobufBytes);

        // Verify optional fields are not set
        assertThat(event.hasDescription()).isFalse();
        assertThat(event.hasUserId()).isFalse();

        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        JsonNode jsonNode = objectMapper.readTree(result.getResult());
        assertThat(jsonNode.get("type").asText()).isEqualTo("EVENT_TYPE_NOTIFICATION");
        assertThat(jsonNode.get("status").asText()).isEqualTo("EVENT_STATUS_ACTIVE");
        assertThat(jsonNode.has("description")).isFalse();
        assertThat(jsonNode.has("userId")).isFalse();
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
