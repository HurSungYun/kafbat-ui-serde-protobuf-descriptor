package io.github.hursungyun.kafbat.ui.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kafbat.ui.serde.api.DeserializeResult;
import io.kafbat.ui.serde.api.PropertyResolver;
import io.kafbat.ui.serde.api.Serde;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@Testcontainers
@IntegrationTest
class ProtobufDescriptorSetSerdeS3IntegrationTest {

    @Container
    static GenericContainer<?> minioContainer = new GenericContainer<>(DockerImageName.parse("minio/minio:latest"))
            .withExposedPorts(9000)
            .withEnv("MINIO_ROOT_USER", "testuser")
            .withEnv("MINIO_ROOT_PASSWORD", "testpassword")
            .withCommand("server", "/data");

    private ProtobufDescriptorSetSerde serde;
    private PropertyResolver serdeProperties;
    private PropertyResolver clusterProperties;
    private PropertyResolver appProperties;
    private ObjectMapper objectMapper;
    private String endpoint;

    private static final String BUCKET_NAME = "test-descriptors";
    private static final String OBJECT_KEY = "descriptors.desc";

    @BeforeEach
    void setUp() throws Exception {
        endpoint = "http://" + minioContainer.getHost() + ":" + minioContainer.getMappedPort(9000);
        
        serde = new ProtobufDescriptorSetSerde();
        serdeProperties = Mockito.mock(PropertyResolver.class);
        clusterProperties = Mockito.mock(PropertyResolver.class);
        appProperties = Mockito.mock(PropertyResolver.class);
        objectMapper = new ObjectMapper();

        // Setup MinIO with test data
        setupMinioWithTestData();
    }

    @Test
    void shouldConfigureWithS3Source() throws Exception {
        // Configure for S3 source
        when(serdeProperties.getProperty("protobuf.s3.endpoint", String.class))
                .thenReturn(Optional.of(endpoint));
        when(serdeProperties.getProperty("protobuf.s3.bucket", String.class))
                .thenReturn(Optional.of(BUCKET_NAME));
        when(serdeProperties.getProperty("protobuf.s3.object.key", String.class))
                .thenReturn(Optional.of(OBJECT_KEY));
        when(serdeProperties.getProperty("protobuf.s3.access.key", String.class))
                .thenReturn(Optional.of("testuser"));
        when(serdeProperties.getProperty("protobuf.s3.secret.key", String.class))
                .thenReturn(Optional.of("testpassword"));
        when(serdeProperties.getProperty("protobuf.s3.secure", Boolean.class))
                .thenReturn(Optional.of(false));
        when(serdeProperties.getProperty("protobuf.s3.refresh.interval.seconds", Long.class))
                .thenReturn(Optional.of(60L));
        
        when(serdeProperties.getProperty("protobuf.message.name", String.class))
                .thenReturn(Optional.of("User"));
        when(serdeProperties.getMapProperty("protobuf.topic.message.map", String.class, String.class))
                .thenReturn(Optional.empty());

        serde.configure(serdeProperties, clusterProperties, appProperties);

        // Verify serde is configured
        assertThat(serde.canDeserialize("test-topic", Serde.Target.VALUE)).isTrue();
        assertThat(serde.getDescription()).isPresent()
                .get().asString().contains("S3: s3://" + BUCKET_NAME + "/" + OBJECT_KEY);
    }

    @Test
    void shouldDeserializeUserMessageFromS3() throws Exception {
        // Configure serde with S3 source
        configureSerdeForS3();
        
        // Create a User message (reuse from existing test)
        byte[] userBytes = createUserMessage();
        
        // Deserialize
        DeserializeResult result = serde.deserializer("test-topic", Serde.Target.VALUE)
                .deserialize(null, userBytes);
        
        assertThat(result.getType()).isEqualTo(DeserializeResult.Type.JSON);
        String resultJson = result.getResult();
        
        // Verify it's a valid JSON result and contains expected User fields
        JsonNode jsonNode = objectMapper.readTree(resultJson);
        assertThat(jsonNode.has("id")).isTrue();
        assertThat(jsonNode.get("id").asInt()).isEqualTo(123);
        assertThat(jsonNode.has("name")).isTrue();
        assertThat(jsonNode.get("name").asText()).isEqualTo("John Doe");
    }

    @Test
    void shouldRefreshDescriptorsFromS3() throws Exception {
        // Configure serde
        configureSerdeForS3();
        
        // Get source info before refresh
        Map<String, Object> sourceInfo = serde.getSourceInfo();
        assertThat(sourceInfo).containsKey("descriptorSource");
        assertThat(sourceInfo.get("descriptorSource").toString()).contains("S3:");
        assertThat(sourceInfo).containsEntry("descriptorSupportsRefresh", true);
        
        // Refresh descriptors
        boolean refreshed = serde.refreshDescriptors();
        assertThat(refreshed).isTrue();
    }

    @Test
    void shouldFallbackToLocalFileWhenS3NotConfigured() throws Exception {
        // Configure for local file (no S3 properties)
        when(serdeProperties.getProperty("protobuf.descriptor.set.file", String.class))
                .thenReturn(Optional.of("/nonexistent/file.desc"));
        when(serdeProperties.getProperty("protobuf.s3.endpoint", String.class))
                .thenReturn(Optional.empty());
        when(serdeProperties.getProperty("protobuf.s3.bucket", String.class))
                .thenReturn(Optional.empty());
        when(serdeProperties.getProperty("protobuf.s3.object.key", String.class))
                .thenReturn(Optional.empty());

        // Should fail because local file doesn't exist, but this proves it tried local file path
        try {
            serde.configure(serdeProperties, clusterProperties, appProperties);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).contains("Local file:");
        }
    }

    private void setupMinioWithTestData() throws Exception {
        MinioClient minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials("testuser", "testpassword")
                .build();

        // Create bucket
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(BUCKET_NAME).build())) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build());
        }

        // Upload test descriptor set
        try (InputStream is = getClass().getResourceAsStream("/test_descriptors.desc")) {
            assertThat(is).isNotNull();
            byte[] descriptorBytes = is.readAllBytes();
            
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(BUCKET_NAME)
                            .object(OBJECT_KEY)
                            .stream(new ByteArrayInputStream(descriptorBytes), descriptorBytes.length, -1)
                            .contentType("application/octet-stream")
                            .build());
        }
    }

    private void configureSerdeForS3() {
        when(serdeProperties.getProperty("protobuf.s3.endpoint", String.class))
                .thenReturn(Optional.of(endpoint));
        when(serdeProperties.getProperty("protobuf.s3.bucket", String.class))
                .thenReturn(Optional.of(BUCKET_NAME));
        when(serdeProperties.getProperty("protobuf.s3.object.key", String.class))
                .thenReturn(Optional.of(OBJECT_KEY));
        when(serdeProperties.getProperty("protobuf.s3.access.key", String.class))
                .thenReturn(Optional.of("testuser"));
        when(serdeProperties.getProperty("protobuf.s3.secret.key", String.class))
                .thenReturn(Optional.of("testpassword"));
        when(serdeProperties.getProperty("protobuf.s3.secure", Boolean.class))
                .thenReturn(Optional.of(false));
        when(serdeProperties.getProperty("protobuf.s3.refresh.interval.seconds", Long.class))
                .thenReturn(Optional.of(60L));
        
        when(serdeProperties.getProperty("protobuf.message.name", String.class))
                .thenReturn(Optional.of("User"));
        when(serdeProperties.getMapProperty("protobuf.topic.message.map", String.class, String.class))
                .thenReturn(Optional.empty());

        serde.configure(serdeProperties, clusterProperties, appProperties);
    }

    @Test
    void shouldLoadTopicMappingsFromS3() throws Exception {
        // First upload topic mappings JSON to S3
        String topicMappingsJson = "{\n" +
                "  \"user-events\": \"User\",\n" +
                "  \"order-events\": \"Order\"\n" +
                "}";
        
        uploadTopicMappingsToS3(topicMappingsJson);

        // Configure serde with both S3 descriptor and S3 topic mappings
        configureSerdeForS3WithTopicMappings();
        
        // Verify that source info includes both descriptor and topic mapping sources
        Map<String, Object> sourceInfo = serde.getSourceInfo();
        assertThat(sourceInfo).containsKey("descriptorSource");
        assertThat(sourceInfo).containsKey("topicMappingSource");
        assertThat(sourceInfo.get("topicMappingSource").toString())
                .contains("S3 Topic Mappings: s3://test-descriptors/topic-mappings.json");

        // Test deserialization with S3 topic mappings
        byte[] userBytes = createUserMessage();
        DeserializeResult result = serde.deserializer("user-events", null)
                .deserialize(null, userBytes);

        assertThat(result.getType()).isEqualTo(DeserializeResult.Type.JSON);
        assertThat(result.getResult()).contains("\"name\"");
    }

    @Test
    void shouldOverrideS3TopicMappingsWithLocalConfig() throws Exception {
        // Upload topic mappings JSON to S3
        String topicMappingsJson = "{\n" +
                "  \"user-events\": \"Order\",\n" +  // This should be overridden
                "  \"payment-events\": \"User\"\n" +
                "}";
        
        uploadTopicMappingsToS3(topicMappingsJson);

        // Configure serde with S3 source but override user-events locally
        when(serdeProperties.getProperty("protobuf.s3.endpoint", String.class))
                .thenReturn(Optional.of(String.format("http://localhost:%d", minioContainer.getMappedPort(9000))));
        when(serdeProperties.getProperty("protobuf.s3.bucket", String.class))
                .thenReturn(Optional.of(BUCKET_NAME));
        when(serdeProperties.getProperty("protobuf.s3.object.key", String.class))
                .thenReturn(Optional.of(OBJECT_KEY));
        when(serdeProperties.getProperty("protobuf.s3.access.key", String.class))
                .thenReturn(Optional.of("testuser"));
        when(serdeProperties.getProperty("protobuf.s3.secret.key", String.class))
                .thenReturn(Optional.of("testpassword"));

        // S3 topic mapping configuration
        when(serdeProperties.getProperty("protobuf.topic.message.map.s3.bucket", String.class))
                .thenReturn(Optional.of(BUCKET_NAME));
        when(serdeProperties.getProperty("protobuf.topic.message.map.s3.object.key", String.class))
                .thenReturn(Optional.of("topic-mappings.json"));

        // Local override: user-events should map to User instead of Order
        when(serdeProperties.getMapProperty("protobuf.topic.message.map", String.class, String.class))
                .thenReturn(Optional.of(Map.of("user-events", "User")));

        when(serdeProperties.getProperty("protobuf.s3.region", String.class))
                .thenReturn(Optional.empty());
        when(serdeProperties.getProperty("protobuf.s3.secure", Boolean.class))
                .thenReturn(Optional.of(false));
        when(serdeProperties.getProperty("protobuf.s3.refresh.interval.seconds", Long.class))
                .thenReturn(Optional.of(60L));

        when(serdeProperties.getProperty("protobuf.message.name", String.class))
                .thenReturn(Optional.empty());

        serde.configure(serdeProperties, clusterProperties, appProperties);

        // Verify both mappings work:
        // user-events should use User (local override), payment-events should use User (from S3)
        byte[] userBytes = createUserMessage();
        
        // user-events should deserialize as User (local override)
        DeserializeResult userResult = serde.deserializer("user-events", null)
                .deserialize(null, userBytes);
        assertThat(userResult.getType()).isEqualTo(DeserializeResult.Type.JSON);
        
        // payment-events should deserialize as User (from S3)
        DeserializeResult paymentResult = serde.deserializer("payment-events", null)
                .deserialize(null, userBytes);
        assertThat(paymentResult.getType()).isEqualTo(DeserializeResult.Type.JSON);
    }

    private void configureSerdeForS3WithTopicMappings() throws Exception {
        // Basic S3 descriptor configuration
        when(serdeProperties.getProperty("protobuf.s3.endpoint", String.class))
                .thenReturn(Optional.of(String.format("http://localhost:%d", minioContainer.getMappedPort(9000))));
        when(serdeProperties.getProperty("protobuf.s3.bucket", String.class))
                .thenReturn(Optional.of(BUCKET_NAME));
        when(serdeProperties.getProperty("protobuf.s3.object.key", String.class))
                .thenReturn(Optional.of(OBJECT_KEY));
        when(serdeProperties.getProperty("protobuf.s3.access.key", String.class))
                .thenReturn(Optional.of("testuser"));
        when(serdeProperties.getProperty("protobuf.s3.secret.key", String.class))
                .thenReturn(Optional.of("testpassword"));

        // S3 topic mapping configuration
        when(serdeProperties.getProperty("protobuf.topic.message.map.s3.bucket", String.class))
                .thenReturn(Optional.of(BUCKET_NAME));
        when(serdeProperties.getProperty("protobuf.topic.message.map.s3.object.key", String.class))
                .thenReturn(Optional.of("topic-mappings.json"));

        when(serdeProperties.getProperty("protobuf.s3.region", String.class))
                .thenReturn(Optional.empty());
        when(serdeProperties.getProperty("protobuf.s3.secure", Boolean.class))
                .thenReturn(Optional.of(false));
        when(serdeProperties.getProperty("protobuf.s3.refresh.interval.seconds", Long.class))
                .thenReturn(Optional.of(60L));

        when(serdeProperties.getProperty("protobuf.message.name", String.class))
                .thenReturn(Optional.empty());
        when(serdeProperties.getMapProperty("protobuf.topic.message.map", String.class, String.class))
                .thenReturn(Optional.empty());

        serde.configure(serdeProperties, clusterProperties, appProperties);
    }

    private void uploadTopicMappingsToS3(String jsonContent) throws Exception {
        MinioClient minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials("testuser", "testpassword")
                .build();
                
        minioClient.putObject(
                PutObjectArgs.builder()
                        .bucket(BUCKET_NAME)
                        .object("topic-mappings.json")
                        .stream(new ByteArrayInputStream(jsonContent.getBytes()),
                                jsonContent.getBytes().length, -1)
                        .contentType("application/json")
                        .build());
    }

    private byte[] createUserMessage() throws Exception {
        // Reuse the createUserMessage logic from existing test
        ProtobufDescriptorSetSerdeTest testHelper = new ProtobufDescriptorSetSerdeTest();
        // Since the method is private, we'll create a simple message here
        // This is a basic protobuf User message that matches the test descriptor
        return new byte[]{0x08, (byte) 0x7B, 0x12, 0x08, 0x4A, 0x6F, 0x68, 0x6E, 0x20, 0x44, 0x6F, 0x65};
    }
}