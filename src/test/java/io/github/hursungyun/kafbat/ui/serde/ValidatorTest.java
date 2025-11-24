package io.github.hursungyun.kafbat.ui.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import io.github.hursungyun.kafbat.ui.serde.test.NestedProtos;
import io.github.hursungyun.kafbat.ui.serde.test.WktProtos;
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
 * Comprehensive test suite for ProtobufMessageValidator to ensure correct handling of: 1. Synthetic
 * oneOf groups (created by proto3 optional fields) 2. Real oneOf groups (explicit business logic
 * constraints) 3. Mixed scenarios with both types 4. Nested messages with optional fields
 */
class ValidatorTest {

    @TempDir Path tempDir;

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

    // =====================================================================
    // SYNTHETIC ONEOF TESTS (proto3 optional fields)
    // =====================================================================

    @Test
    void shouldAllowOptionalFieldsToBeOmitted() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Event");

        // Event message has: optional string description, optional string user_id
        // Both can be omitted entirely
        String json =
                """
                {
                    "id": "evt-001",
                    "name": "Test Event",
                    "createdAt": "2024-01-15T10:30:45Z",
                    "updatedAt": "2024-01-15T10:30:45Z",
                    "scheduledAt": "2024-01-15T10:30:45Z",
                    "type": "EVENT_TYPE_ALERT",
                    "status": "EVENT_STATUS_PENDING",
                    "priority": 1
                }
                """;

        // Should NOT throw - optional fields can be omitted
        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(json);

        WktProtos.Event event = WktProtos.Event.parseFrom(protobufBytes);
        assertThat(event.getId()).isEqualTo("evt-001");
        assertThat(event.hasDescription()).isFalse();
        assertThat(event.hasUserId()).isFalse();
    }

    @Test
    void shouldAllowOptionalFieldsToBeNull() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Event");

        // Optional fields explicitly set to null
        String json =
                """
                {
                    "id": "evt-002",
                    "name": "Test Event",
                    "createdAt": "2024-01-15T10:30:45Z",
                    "updatedAt": "2024-01-15T10:30:45Z",
                    "scheduledAt": "2024-01-15T10:30:45Z",
                    "type": "EVENT_TYPE_ALERT",
                    "status": "EVENT_STATUS_PENDING",
                    "priority": 1,
                    "description": null,
                    "userId": null
                }
                """;

        // Should NOT throw - optional fields can be null
        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(json);

        WktProtos.Event event = WktProtos.Event.parseFrom(protobufBytes);
        assertThat(event.getId()).isEqualTo("evt-002");
        assertThat(event.hasDescription()).isFalse();
        assertThat(event.hasUserId()).isFalse();
    }

    @Test
    void shouldAllowOptionalFieldsToHaveValues() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Event");

        // Optional fields with actual values
        String json =
                """
                {
                    "id": "evt-003",
                    "name": "Test Event",
                    "createdAt": "2024-01-15T10:30:45Z",
                    "updatedAt": "2024-01-15T10:30:45Z",
                    "scheduledAt": "2024-01-15T10:30:45Z",
                    "type": "EVENT_TYPE_ALERT",
                    "status": "EVENT_STATUS_PENDING",
                    "priority": 1,
                    "description": "Important system alert",
                    "userId": "user-123"
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(json);

        WktProtos.Event event = WktProtos.Event.parseFrom(protobufBytes);
        assertThat(event.getId()).isEqualTo("evt-003");
        assertThat(event.hasDescription()).isTrue();
        assertThat(event.getDescription()).isEqualTo("Important system alert");
        assertThat(event.hasUserId()).isTrue();
        assertThat(event.getUserId()).isEqualTo("user-123");
    }

    @Test
    void shouldAllowPartiallySetOptionalFields() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Event");

        // Only one optional field set, the other omitted
        String json =
                """
                {
                    "id": "evt-004",
                    "name": "Test Event",
                    "createdAt": "2024-01-15T10:30:45Z",
                    "updatedAt": "2024-01-15T10:30:45Z",
                    "scheduledAt": "2024-01-15T10:30:45Z",
                    "type": "EVENT_TYPE_ALERT",
                    "status": "EVENT_STATUS_PENDING",
                    "priority": 1,
                    "description": "Only description is set",
                    "userId": null
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(json);

        WktProtos.Event event = WktProtos.Event.parseFrom(protobufBytes);
        assertThat(event.hasDescription()).isTrue();
        assertThat(event.getDescription()).isEqualTo("Only description is set");
        assertThat(event.hasUserId()).isFalse();
    }

    // =====================================================================
    // REAL ONEOF TESTS (explicit business logic)
    // =====================================================================

    @Test
    void shouldRequireRealOneOfToHaveAtLeastOneVariant() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Notification");

        // Notification has real oneOf: email | sms | push
        // None of them are set - this should FAIL
        String json =
                """
                {
                    "id": "NOTIF-001",
                    "timestamp": 1704067200000
                }
                """;

        // Should throw because real oneOf requires at least one variant
        // Exception is wrapped in RuntimeException, so check the cause
        assertThatThrownBy(() -> serde.serializer("test-topic", Serde.Target.VALUE).serialize(json))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to serialize JSON to protobuf")
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("oneOf content")
                .hasMessageContaining("email, sms, push");
    }

    @Test
    void shouldAcceptRealOneOfWithEmailVariant() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Notification");

        String json =
                """
                {
                    "id": "NOTIF-002",
                    "timestamp": 1704067200000,
                    "email": {
                        "recipient": "user@example.com",
                        "subject": "Test",
                        "body": {
                            "text": "Hello",
                            "html": "<p>Hello</p>",
                            "attachments": []
                        }
                    }
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(json);

        NestedProtos.Notification notification = NestedProtos.Notification.parseFrom(protobufBytes);
        assertThat(notification.hasEmail()).isTrue();
        assertThat(notification.hasSms()).isFalse();
        assertThat(notification.hasPush()).isFalse();
    }

    @Test
    void shouldAcceptRealOneOfWithSmsVariant() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Notification");

        String json =
                """
                {
                    "id": "NOTIF-003",
                    "timestamp": 1704067200000,
                    "sms": {
                        "phoneNumber": "+1-555-1234",
                        "message": "Test SMS",
                        "metadata": {
                            "senderId": "TestApp",
                            "countryCode": "US"
                        }
                    }
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(json);

        NestedProtos.Notification notification = NestedProtos.Notification.parseFrom(protobufBytes);
        assertThat(notification.hasSms()).isTrue();
        assertThat(notification.hasEmail()).isFalse();
        assertThat(notification.hasPush()).isFalse();
    }

    @Test
    void shouldAcceptRealOneOfWithPushVariant() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Notification");

        String json =
                """
                {
                    "id": "NOTIF-004",
                    "timestamp": 1704067200000,
                    "push": {
                        "deviceToken": "token-123",
                        "title": "Test Push",
                        "body": "Push notification body",
                        "data": {
                            "key": "value"
                        }
                    }
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(json);

        NestedProtos.Notification notification = NestedProtos.Notification.parseFrom(protobufBytes);
        assertThat(notification.hasPush()).isTrue();
        assertThat(notification.hasEmail()).isFalse();
        assertThat(notification.hasSms()).isFalse();
    }

    // =====================================================================
    // ROUND-TRIP TESTS (ensure validation doesn't break round-trip)
    // =====================================================================

    @Test
    void shouldSupportRoundTripWithOptionalFields() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Event");

        String originalJson =
                """
                {
                    "id": "evt-005",
                    "name": "Round Trip Test",
                    "createdAt": "2024-01-15T10:30:45Z",
                    "updatedAt": "2024-01-15T10:30:45Z",
                    "scheduledAt": "2024-01-15T10:30:45Z",
                    "type": "EVENT_TYPE_ALERT",
                    "status": "EVENT_STATUS_PENDING",
                    "priority": 1,
                    "description": "Test description",
                    "userId": null
                }
                """;

        // Serialize to protobuf
        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(originalJson);

        // Deserialize back to JSON
        String deserializedJson =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes)
                        .getResult();

        // Re-serialize the deserialized JSON (round-trip)
        byte[] roundTripBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(deserializedJson);

        // Verify round-trip produces same protobuf
        WktProtos.Event originalEvent = WktProtos.Event.parseFrom(protobufBytes);
        WktProtos.Event roundTripEvent = WktProtos.Event.parseFrom(roundTripBytes);

        assertThat(roundTripEvent.getId()).isEqualTo(originalEvent.getId());
        assertThat(roundTripEvent.getName()).isEqualTo(originalEvent.getName());
        assertThat(roundTripEvent.hasDescription()).isEqualTo(originalEvent.hasDescription());
        if (originalEvent.hasDescription()) {
            assertThat(roundTripEvent.getDescription()).isEqualTo(originalEvent.getDescription());
        }
    }

    @Test
    void shouldSupportRoundTripWithRealOneOf() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Notification");

        String originalJson =
                """
                {
                    "id": "NOTIF-005",
                    "timestamp": 1704067200000,
                    "email": {
                        "recipient": "test@example.com",
                        "subject": "Round Trip",
                        "body": {
                            "text": "Test",
                            "html": "<p>Test</p>",
                            "attachments": []
                        }
                    }
                }
                """;

        // Serialize to protobuf
        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(originalJson);

        // Deserialize back to JSON
        String deserializedJson =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes)
                        .getResult();

        // Re-serialize the deserialized JSON (round-trip)
        byte[] roundTripBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(deserializedJson);

        // Verify round-trip produces same protobuf
        NestedProtos.Notification originalNotif =
                NestedProtos.Notification.parseFrom(protobufBytes);
        NestedProtos.Notification roundTripNotif =
                NestedProtos.Notification.parseFrom(roundTripBytes);

        assertThat(roundTripNotif.hasEmail()).isTrue();
        assertThat(roundTripNotif.getEmail().getRecipient())
                .isEqualTo(originalNotif.getEmail().getRecipient());
    }

    // =====================================================================
    // EDGE CASES
    // =====================================================================

    @Test
    void shouldHandleEmptyOptionalFields() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Event");

        // Optional string with empty string value (not null)
        String json =
                """
                {
                    "id": "evt-006",
                    "name": "Test Event",
                    "createdAt": "2024-01-15T10:30:45Z",
                    "updatedAt": "2024-01-15T10:30:45Z",
                    "scheduledAt": "2024-01-15T10:30:45Z",
                    "type": "EVENT_TYPE_ALERT",
                    "status": "EVENT_STATUS_PENDING",
                    "priority": 1,
                    "description": "",
                    "userId": null
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(json);

        WktProtos.Event event = WktProtos.Event.parseFrom(protobufBytes);
        assertThat(event.hasDescription()).isTrue();
        assertThat(event.getDescription()).isEmpty();
        assertThat(event.hasUserId()).isFalse();
    }

    @Test
    void shouldDistinguishBetweenSyntheticAndRealOneOf() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();

        // Test 1: Event message - has ONLY synthetic oneOf (optional fields)
        // Should NOT require any fields to be set
        configureSerde(descriptorFile, "test.Event");
        String eventJson =
                """
                {
                    "id": "evt-007",
                    "name": "Test",
                    "createdAt": "2024-01-15T10:30:45Z",
                    "updatedAt": "2024-01-15T10:30:45Z",
                    "scheduledAt": "2024-01-15T10:30:45Z",
                    "type": "EVENT_TYPE_ALERT",
                    "status": "EVENT_STATUS_PENDING",
                    "priority": 1
                }
                """;

        // Should succeed - no optional fields required
        byte[] eventBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(eventJson);
        assertThat(eventBytes).isNotEmpty();

        // Test 2: Notification message - has REAL oneOf (content: email|sms|push)
        // Should REQUIRE at least one variant
        configureSerde(descriptorFile, "test.Notification");
        String notificationJson =
                """
                {
                    "id": "NOTIF-007",
                    "timestamp": 1704067200000
                }
                """;

        // Should fail - real oneOf requires at least one variant
        // Exception is wrapped in RuntimeException, so check the cause
        assertThatThrownBy(
                        () ->
                                serde.serializer("test-topic", Serde.Target.VALUE)
                                        .serialize(notificationJson))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to serialize JSON to protobuf")
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("oneOf content")
                .hasMessageContaining("email, sms, push");
    }

    // =====================================================================
    // ADVANCED TEST CASES
    // =====================================================================

    @Test
    void shouldHandleMessageWithBothSyntheticAndRealOneOf() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.MixedOneOfMessage");

        // MixedOneOfMessage has:
        // - Real oneOf: payment_method (credit_card|bank_transfer|paypal)
        // - Synthetic oneOfs: optional description, notes, amount

        String json =
                """
                {
                    "id": "payment-001",
                    "creditCard": {
                        "cardNumber": "4111111111111111",
                        "cvv": "123",
                        "expiry": "12/25"
                    },
                    "description": "Monthly subscription",
                    "notes": null,
                    "amount": 1999
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(json);

        io.github.hursungyun.kafbat.ui.serde.test.ValidatorTestProtos.MixedOneOfMessage message =
                io.github.hursungyun.kafbat.ui.serde.test.ValidatorTestProtos.MixedOneOfMessage
                        .parseFrom(protobufBytes);

        assertThat(message.getId()).isEqualTo("payment-001");
        assertThat(message.hasCreditCard()).isTrue();
        assertThat(message.hasDescription()).isTrue();
        assertThat(message.getDescription()).isEqualTo("Monthly subscription");
        assertThat(message.hasAmount()).isTrue();
        assertThat(message.getAmount()).isEqualTo(1999);
    }

    @Test
    void shouldRequireRealOneOfButAllowOptionalFieldsInMixedMessage() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.MixedOneOfMessage");

        // Missing real oneOf (payment_method) but optional fields omitted - should FAIL
        String json =
                """
                {
                    "id": "payment-002"
                }
                """;

        assertThatThrownBy(() -> serde.serializer("test-topic", Serde.Target.VALUE).serialize(json))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to serialize JSON to protobuf")
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("oneOf payment_method")
                .hasMessageContaining("creditCard, bankTransfer, paypal");
    }

    @Test
    void shouldHandleMultipleRealOneOfsInSameMessage() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.MultipleOneOfMessage");

        // Message has TWO real oneOfs: notification_channel and priority_level
        // Both must have at least one variant set
        String json =
                """
                {
                    "id": "multi-001",
                    "email": "user@example.com",
                    "high": "high-priority"
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(json);

        io.github.hursungyun.kafbat.ui.serde.test.ValidatorTestProtos.MultipleOneOfMessage message =
                io.github.hursungyun.kafbat.ui.serde.test.ValidatorTestProtos.MultipleOneOfMessage
                        .parseFrom(protobufBytes);

        assertThat(message.getId()).isEqualTo("multi-001");
        assertThat(message.hasEmail()).isTrue();
        assertThat(message.hasHigh()).isTrue();
    }

    @Test
    void shouldFailWhenFirstOneOfMissingInMultipleOneOfMessage() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.MultipleOneOfMessage");

        // Missing first oneOf (notification_channel)
        String json =
                """
                {
                    "id": "multi-002",
                    "high": "high-priority"
                }
                """;

        assertThatThrownBy(() -> serde.serializer("test-topic", Serde.Target.VALUE).serialize(json))
                .isInstanceOf(RuntimeException.class)
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("oneOf notification_channel");
    }

    @Test
    void shouldFailWhenSecondOneOfMissingInMultipleOneOfMessage() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.MultipleOneOfMessage");

        // Missing second oneOf (priority_level)
        String json =
                """
                {
                    "id": "multi-003",
                    "email": "user@example.com"
                }
                """;

        assertThatThrownBy(() -> serde.serializer("test-topic", Serde.Target.VALUE).serialize(json))
                .isInstanceOf(RuntimeException.class)
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("oneOf priority_level");
    }

    @Test
    void shouldValidateOneOfInNestedMessages() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Container");

        // Container has a single_item field which is MixedOneOfMessage
        // The nested message's real oneOf must be validated
        String json =
                """
                {
                    "id": "container-001",
                    "singleItem": {
                        "id": "item-001",
                        "paypal": {
                            "email": "paypal@example.com",
                            "transactionId": "TXN-123"
                        },
                        "description": "PayPal payment"
                    },
                    "items": []
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(json);

        io.github.hursungyun.kafbat.ui.serde.test.ValidatorTestProtos.Container container =
                io.github.hursungyun.kafbat.ui.serde.test.ValidatorTestProtos.Container.parseFrom(
                        protobufBytes);

        assertThat(container.getId()).isEqualTo("container-001");
        assertThat(container.hasSingleItem()).isTrue();
        assertThat(container.getSingleItem().hasPaypal()).isTrue();
    }

    @Test
    void shouldFailWhenNestedMessageMissingRequiredOneOf() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Container");

        // Nested singleItem is missing its required oneOf (payment_method)
        String json =
                """
                {
                    "id": "container-002",
                    "singleItem": {
                        "id": "item-002",
                        "description": "Missing payment method"
                    },
                    "items": []
                }
                """;

        assertThatThrownBy(() -> serde.serializer("test-topic", Serde.Target.VALUE).serialize(json))
                .isInstanceOf(RuntimeException.class)
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("oneOf 'payment_method'");
    }

    @Test
    void shouldValidateOneOfInRepeatedNestedMessages() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Container");

        // Container has repeated items (array of MixedOneOfMessage)
        // Each item's real oneOf must be validated
        String json =
                """
                {
                    "id": "container-003",
                    "items": [
                        {
                            "id": "item-001",
                            "creditCard": {
                                "cardNumber": "4111111111111111",
                                "cvv": "123",
                                "expiry": "12/25"
                            }
                        },
                        {
                            "id": "item-002",
                            "bankTransfer": {
                                "accountNumber": "123456789",
                                "routingNumber": "987654321",
                                "bankName": "Test Bank"
                            }
                        }
                    ],
                    "singleItem": null
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(json);

        io.github.hursungyun.kafbat.ui.serde.test.ValidatorTestProtos.Container container =
                io.github.hursungyun.kafbat.ui.serde.test.ValidatorTestProtos.Container.parseFrom(
                        protobufBytes);

        assertThat(container.getId()).isEqualTo("container-003");
        assertThat(container.getItemsCount()).isEqualTo(2);
        assertThat(container.getItems(0).hasCreditCard()).isTrue();
        assertThat(container.getItems(1).hasBankTransfer()).isTrue();
    }

    @Test
    void shouldFailWhenOneItemInRepeatedArrayMissingRequiredOneOf() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Container");

        // Second item in array is missing required oneOf
        String json =
                """
                {
                    "id": "container-004",
                    "items": [
                        {
                            "id": "item-001",
                            "creditCard": {
                                "cardNumber": "4111111111111111",
                                "cvv": "123",
                                "expiry": "12/25"
                            }
                        },
                        {
                            "id": "item-002",
                            "description": "Missing payment method"
                        }
                    ],
                    "singleItem": null
                }
                """;

        assertThatThrownBy(() -> serde.serializer("test-topic", Serde.Target.VALUE).serialize(json))
                .isInstanceOf(RuntimeException.class)
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("oneOf 'payment_method'");
    }

    @Test
    void shouldHandleOptionalFieldContainingMessageWithOneOf() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Wrapper");

        // Wrapper has optional_payment field (proto3 optional)
        // When present, the nested message's oneOf must still be validated
        String json =
                """
                {
                    "id": "wrapper-001",
                    "optionalPayment": {
                        "id": "payment-001",
                        "creditCard": {
                            "cardNumber": "4111111111111111",
                            "cvv": "123",
                            "expiry": "12/25"
                        }
                    }
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(json);

        io.github.hursungyun.kafbat.ui.serde.test.ValidatorTestProtos.Wrapper wrapper =
                io.github.hursungyun.kafbat.ui.serde.test.ValidatorTestProtos.Wrapper.parseFrom(
                        protobufBytes);

        assertThat(wrapper.getId()).isEqualTo("wrapper-001");
        assertThat(wrapper.hasOptionalPayment()).isTrue();
        assertThat(wrapper.getOptionalPayment().hasCreditCard()).isTrue();
    }

    @Test
    void shouldAllowOptionalFieldContainingMessageWithOneOfToBeOmitted() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Wrapper");

        // optional_payment can be completely omitted
        String json =
                """
                {
                    "id": "wrapper-002"
                }
                """;

        byte[] protobufBytes = serde.serializer("test-topic", Serde.Target.VALUE).serialize(json);

        io.github.hursungyun.kafbat.ui.serde.test.ValidatorTestProtos.Wrapper wrapper =
                io.github.hursungyun.kafbat.ui.serde.test.ValidatorTestProtos.Wrapper.parseFrom(
                        protobufBytes);

        assertThat(wrapper.getId()).isEqualTo("wrapper-002");
        assertThat(wrapper.hasOptionalPayment()).isFalse();
    }

    @Test
    void shouldFailWhenOptionalFieldPresentButNestedOneOfMissing() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Wrapper");

        // optional_payment is present, but missing its required oneOf
        String json =
                """
                {
                    "id": "wrapper-003",
                    "optionalPayment": {
                        "id": "payment-002",
                        "description": "Missing payment method"
                    }
                }
                """;

        assertThatThrownBy(() -> serde.serializer("test-topic", Serde.Target.VALUE).serialize(json))
                .isInstanceOf(RuntimeException.class)
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("oneOf 'payment_method'");
    }

    // =====================================================================
    // Helper Methods
    // =====================================================================

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
