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
 * Comprehensive test suite for nested protobuf messages covering: - Deep nesting (4+ levels) -
 * Maps with nested messages - oneOf with nested messages - Repeated nested messages -
 * Optional/nullable nested messages - Complex real-world scenarios
 */
class NestedMessagesTest {

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
    void shouldHandleDeeplyNestedOrganizationStructure() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Organization");

        // Create a 4-level nested structure: Organization -> Department -> Team -> Employee
        // -> ContactInfo
        String organizationJson =
                """
                {
                    "id": "ORG-001",
                    "name": "TechCorp",
                    "departments": [
                        {
                            "id": "DEPT-001",
                            "name": "Engineering",
                            "teams": [
                                {
                                    "id": "TEAM-001",
                                    "name": "Backend",
                                    "employees": [
                                        {
                                            "id": "EMP-001",
                                            "name": "Alice Johnson",
                                            "email": "alice@techcorp.com",
                                            "contact": {
                                                "phone": "+1-555-0100",
                                                "mobile": "+1-555-0101",
                                                "emergency": {
                                                    "name": "Bob Johnson",
                                                    "relationship": "Spouse",
                                                    "phone": "+1-555-0102"
                                                }
                                            }
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
                """;

        // Serialize and verify
        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(organizationJson);

        NestedProtos.Organization org = NestedProtos.Organization.parseFrom(protobufBytes);
        assertThat(org.getId()).isEqualTo("ORG-001");
        assertThat(org.getDepartments(0).getTeams(0).getEmployees(0).getContact().getPhone())
                .isEqualTo("+1-555-0100");
        assertThat(
                        org.getDepartments(0)
                                .getTeams(0)
                                .getEmployees(0)
                                .getContact()
                                .getEmergency()
                                .getName())
                .isEqualTo("Bob Johnson");

        // Deserialize and verify JSON
        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        JsonNode jsonNode = objectMapper.readTree(result.getResult());
        assertThat(
                        jsonNode.get("departments")
                                .get(0)
                                .get("teams")
                                .get(0)
                                .get("employees")
                                .get(0)
                                .get("contact")
                                .get("emergency")
                                .get("relationship")
                                .asText())
                .isEqualTo("Spouse");
    }

    @Test
    void shouldHandleMapWithNestedMessageValues() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Catalog");

        String catalogJson =
                """
                {
                    "id": "CAT-001",
                    "products": {
                        "PROD-001": {
                            "id": "PROD-001",
                            "name": "Laptop",
                            "price": 1299.99,
                            "tags": ["electronics", "computers"],
                            "specs": {
                                "attributes": {
                                    "brand": "TechBrand",
                                    "model": "X1",
                                    "color": "silver"
                                },
                                "dimensions": {
                                    "width": 35.5,
                                    "height": 2.5,
                                    "depth": 24.5,
                                    "unit": "cm"
                                }
                            }
                        },
                        "PROD-002": {
                            "id": "PROD-002",
                            "name": "Mouse",
                            "price": 29.99,
                            "tags": ["electronics", "accessories"],
                            "specs": {
                                "attributes": {
                                    "brand": "TechBrand",
                                    "type": "wireless"
                                },
                                "dimensions": {
                                    "width": 6.5,
                                    "height": 3.5,
                                    "depth": 10.0,
                                    "unit": "cm"
                                }
                            }
                        }
                    }
                }
                """;

        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(catalogJson);

        NestedProtos.Catalog catalog = NestedProtos.Catalog.parseFrom(protobufBytes);
        assertThat(catalog.getProductsMap()).hasSize(2);
        assertThat(catalog.getProductsMap().get("PROD-001").getPrice()).isEqualTo(1299.99);
        assertThat(catalog.getProductsMap().get("PROD-001").getSpecs().getDimensions().getUnit())
                .isEqualTo("cm");

        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        JsonNode jsonNode = objectMapper.readTree(result.getResult());
        assertThat(
                        jsonNode.get("products")
                                .get("PROD-001")
                                .get("specs")
                                .get("dimensions")
                                .get("width")
                                .asDouble())
                .isEqualTo(35.5);
    }

    @Test
    void shouldHandleOneOfWithEmailNotification() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Notification");

        String notificationJson =
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
                    },
                    "sms": null,
                    "push": null
                }
                """;

        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(notificationJson);

        NestedProtos.Notification notification =
                NestedProtos.Notification.parseFrom(protobufBytes);
        assertThat(notification.hasEmail()).isTrue();
        assertThat(notification.hasSms()).isFalse();
        assertThat(notification.hasPush()).isFalse();
        assertThat(notification.getEmail().getBody().getAttachments(0).getFilename())
                .isEqualTo("guide.pdf");

        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        JsonNode jsonNode = objectMapper.readTree(result.getResult());
        assertThat(jsonNode.has("email")).isTrue();
        assertThat(jsonNode.get("email").get("body").get("attachments").get(0).get("size").asInt())
                .isEqualTo(524288);
    }

    @Test
    void shouldHandleOneOfWithSmsNotification() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.Notification");

        String notificationJson =
                """
                {
                    "id": "NOTIF-002",
                    "timestamp": 1704153600000,
                    "email": null,
                    "sms": {
                        "phoneNumber": "+1-555-1234",
                        "message": "Your code is 123456",
                        "metadata": {
                            "senderId": "MyApp",
                            "countryCode": "US"
                        }
                    },
                    "push": null
                }
                """;

        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(notificationJson);

        NestedProtos.Notification notification =
                NestedProtos.Notification.parseFrom(protobufBytes);
        assertThat(notification.hasSms()).isTrue();
        assertThat(notification.hasEmail()).isFalse();
        assertThat(notification.getSms().getMetadata().getCountryCode()).isEqualTo("US");

        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        JsonNode jsonNode = objectMapper.readTree(result.getResult());
        assertThat(jsonNode.has("sms")).isTrue();
        assertThat(jsonNode.get("sms").get("metadata").get("senderId").asText())
                .isEqualTo("MyApp");
    }

    @Test
    void shouldHandleRepeatedNestedMessagesInShoppingCart() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.ShoppingCart");

        String cartJson =
                """
                {
                    "cartId": "CART-001",
                    "userId": "USER-001",
                    "items": [
                        {
                            "productId": "PROD-001",
                            "quantity": 2,
                            "unitPrice": 49.99,
                            "options": [
                                {
                                    "name": "Color",
                                    "value": "Blue",
                                    "priceAdjustment": 0.0
                                },
                                {
                                    "name": "Size",
                                    "value": "Large",
                                    "priceAdjustment": 5.0
                                }
                            ]
                        },
                        {
                            "productId": "PROD-002",
                            "quantity": 1,
                            "unitPrice": 99.99,
                            "options": []
                        }
                    ],
                    "payment": {
                        "method": "credit_card",
                        "billingAddress": {
                            "street": "123 Main St",
                            "city": "Springfield",
                            "state": "IL",
                            "postalCode": "62701",
                            "country": "USA"
                        }
                    }
                }
                """;

        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(cartJson);

        NestedProtos.ShoppingCart cart = NestedProtos.ShoppingCart.parseFrom(protobufBytes);
        assertThat(cart.getItemsCount()).isEqualTo(2);
        assertThat(cart.getItems(0).getOptionsCount()).isEqualTo(2);
        assertThat(cart.getItems(0).getOptions(1).getPriceAdjustment()).isEqualTo(5.0);

        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        JsonNode jsonNode = objectMapper.readTree(result.getResult());
        assertThat(jsonNode.get("items").get(0).get("options").size()).isEqualTo(2);
        assertThat(jsonNode.get("items").get(1).get("options").size()).isEqualTo(0);
    }

    @Test
    void shouldHandleOptionalNestedMessagesWithAllFields() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.BlogPost");

        String postJson =
                """
                {
                    "id": "POST-001",
                    "title": "Understanding Protobuf",
                    "content": "Protobuf is a powerful serialization format...",
                    "author": {
                        "id": "AUTHOR-001",
                        "name": "Jane Doe",
                        "profile": {
                            "bio": "Software Engineer",
                            "avatarUrl": "https://example.com/avatar.jpg",
                            "social": {
                                "twitter": "@janedoe",
                                "github": "janedoe",
                                "linkedin": "janedoe"
                            }
                        }
                    },
                    "comments": [
                        {
                            "id": "COMMENT-001",
                            "text": "Great article!",
                            "commenterName": "Reader1",
                            "timestamp": 1704067200000,
                            "replies": [
                                {
                                    "id": "REPLY-001",
                                    "text": "Thanks!",
                                    "commenterName": "Jane Doe",
                                    "timestamp": 1704153600000,
                                    "replies": []
                                }
                            ]
                        }
                    ],
                    "publishInfo": {
                        "publishedAt": 1704067200000,
                        "updatedAt": 1704240000000,
                        "status": "published"
                    }
                }
                """;

        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(postJson);

        NestedProtos.BlogPost post = NestedProtos.BlogPost.parseFrom(protobufBytes);
        assertThat(post.hasAuthor()).isTrue();
        assertThat(post.getAuthor().getProfile().getSocial().getTwitter()).isEqualTo("@janedoe");
        assertThat(post.getCommentsCount()).isEqualTo(1);
        assertThat(post.getComments(0).getRepliesCount()).isEqualTo(1);

        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        JsonNode jsonNode = objectMapper.readTree(result.getResult());
        assertThat(
                        jsonNode.get("author")
                                .get("profile")
                                .get("social")
                                .get("github")
                                .asText())
                .isEqualTo("janedoe");
        assertThat(jsonNode.get("comments").get(0).get("replies").get(0).get("text").asText())
                .isEqualTo("Thanks!");
    }

    @Test
    void shouldHandleOptionalNestedMessagesWithNullValues() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.BlogPost");

        String postJson =
                """
                {
                    "id": "POST-002",
                    "title": "Draft Post",
                    "content": "This is a draft...",
                    "author": null,
                    "comments": [],
                    "publishInfo": null
                }
                """;

        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(postJson);

        NestedProtos.BlogPost post = NestedProtos.BlogPost.parseFrom(protobufBytes);
        assertThat(post.hasAuthor()).isFalse();
        assertThat(post.getCommentsCount()).isEqualTo(0);
        assertThat(post.hasPublishInfo()).isFalse();

        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        JsonNode jsonNode = objectMapper.readTree(result.getResult());
        assertThat(jsonNode.get("comments").isArray()).isTrue();
        assertThat(jsonNode.get("comments").size()).isEqualTo(0);
    }

    @Test
    void shouldHandleComplexOrderRecordWithAllNesting() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile, "test.OrderRecord");

        String orderJson =
                """
                {
                    "orderId": "ORDER-12345",
                    "customer": {
                        "id": "CUST-001",
                        "name": "John Smith",
                        "email": "john@example.com",
                        "address": {
                            "street": "456 Oak Ave",
                            "city": "Boston",
                            "postalCode": "02101",
                            "country": "USA"
                        },
                        "loyalty": {
                            "tier": "GOLD",
                            "points": 1500,
                            "discountRate": 0.15
                        }
                    },
                    "lines": [
                        {
                            "sku": "SKU-001",
                            "description": "Wireless Keyboard",
                            "quantity": 2,
                            "price": {
                                "unitPrice": 79.99,
                                "discount": 10.00,
                                "tax": 12.60,
                                "total": 162.58
                            }
                        },
                        {
                            "sku": "SKU-002",
                            "description": "Gaming Mouse",
                            "quantity": 1,
                            "price": {
                                "unitPrice": 59.99,
                                "discount": 0.00,
                                "tax": 5.40,
                                "total": 65.39
                            }
                        }
                    ],
                    "totals": {
                        "subtotal": 219.97,
                        "tax": 18.00,
                        "shipping": 9.99,
                        "discount": 10.00,
                        "total": 237.96
                    },
                    "shipping": {
                        "method": "express",
                        "trackingNumber": "TRACK-123456",
                        "estimatedDelivery": 1704499200000,
                        "address": {
                            "recipient": "John Smith",
                            "street": "456 Oak Ave",
                            "city": "Boston",
                            "postalCode": "02101",
                            "country": "USA"
                        }
                    },
                    "status": "STATUS_CONFIRMED"
                }
                """;

        byte[] protobufBytes =
                serde.serializer("test-topic", Serde.Target.VALUE).serialize(orderJson);

        NestedProtos.OrderRecord order = NestedProtos.OrderRecord.parseFrom(protobufBytes);
        assertThat(order.getOrderId()).isEqualTo("ORDER-12345");
        assertThat(order.getCustomer().getLoyalty().getTier()).isEqualTo("GOLD");
        assertThat(order.getLinesCount()).isEqualTo(2);
        assertThat(order.getLines(0).getPrice().getTotal()).isEqualTo(162.58);
        assertThat(order.getStatus()).isEqualTo(NestedProtos.ProcessingStatus.STATUS_CONFIRMED);

        DeserializeResult result =
                serde.deserializer("test-topic", Serde.Target.VALUE)
                        .deserialize(null, protobufBytes);

        JsonNode jsonNode = objectMapper.readTree(result.getResult());
        assertThat(jsonNode.get("customer").get("loyalty").get("discountRate").asDouble())
                .isEqualTo(0.15);
        assertThat(jsonNode.get("shipping").get("address").get("recipient").asText())
                .isEqualTo("John Smith");
        assertThat(jsonNode.get("status").asText()).isEqualTo("STATUS_CONFIRMED");
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
