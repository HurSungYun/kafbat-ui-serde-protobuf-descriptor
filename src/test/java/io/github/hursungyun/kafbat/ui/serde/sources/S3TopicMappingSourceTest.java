package io.github.hursungyun.kafbat.ui.serde.sources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hursungyun.kafbat.ui.serde.IntegrationTest;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@IntegrationTest
class S3TopicMappingSourceTest {

    @Container
    static GenericContainer<?> minioContainer =
            new GenericContainer<>(DockerImageName.parse("minio/minio:latest"))
                    .withExposedPorts(9000)
                    .withEnv("MINIO_ROOT_USER", "testuser")
                    .withEnv("MINIO_ROOT_PASSWORD", "testpassword")
                    .withCommand("server", "/data");

    private MinioClient minioClient;
    private static final String BUCKET_NAME = "test-bucket";
    private static final String OBJECT_KEY = "topic-mappings.json";

    @BeforeEach
    void setUp() throws Exception {
        minioClient =
                MinioClient.builder()
                        .endpoint(
                                String.format(
                                        "http://localhost:%d", minioContainer.getMappedPort(9000)))
                        .credentials("testuser", "testpassword")
                        .build();

        // Create test bucket
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(BUCKET_NAME).build())) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build());
        }
    }

    @AfterEach
    void tearDown() {
        // Cleanup is handled by testcontainers
    }

    @Test
    void shouldLoadTopicMappingsFromS3() throws Exception {
        // JSON content for topic mappings
        String jsonContent =
                "{\n"
                        + "  \"user-events\": \"com.example.User\",\n"
                        + "  \"order-events\": \"com.example.Order\",\n"
                        + "  \"payment-events\": \"com.example.Payment\"\n"
                        + "}";

        // Upload JSON to S3
        uploadJsonToS3(jsonContent);

        // Create source and load mappings
        S3TopicMappingSource source =
                new S3TopicMappingSource(
                        minioClient, BUCKET_NAME, OBJECT_KEY, Duration.ofMinutes(5));

        Map<String, String> mappings = source.loadTopicMappings();

        // Verify mappings
        assertThat(mappings).hasSize(3);
        assertThat(mappings.get("user-events")).isEqualTo("com.example.User");
        assertThat(mappings.get("order-events")).isEqualTo("com.example.Order");
        assertThat(mappings.get("payment-events")).isEqualTo("com.example.Payment");
    }

    @Test
    void shouldReturnCachedMappingsIfNotExpired() throws Exception {
        String jsonContent = "{\"topic1\": \"Message1\"}";

        // Upload JSON to S3
        uploadJsonToS3(jsonContent);

        // Create source with short cache time for testing
        S3TopicMappingSource source =
                new S3TopicMappingSource(
                        minioClient, BUCKET_NAME, OBJECT_KEY, Duration.ofMinutes(5));

        // Load mappings twice - should use cache on second call
        Map<String, String> firstResult = source.loadTopicMappings();
        Map<String, String> secondResult = source.loadTopicMappings();

        assertThat(firstResult).isEqualTo(secondResult);
        assertThat(firstResult).hasSize(1);
        assertThat(firstResult.get("topic1")).isEqualTo("Message1");
    }

    @Test
    void shouldThrowExceptionOnInvalidJson() throws Exception {
        String invalidJson = "{ invalid json content }";

        // Upload invalid JSON to S3
        uploadJsonToS3(invalidJson);

        S3TopicMappingSource source =
                new S3TopicMappingSource(
                        minioClient, BUCKET_NAME, OBJECT_KEY, Duration.ofMinutes(5));

        assertThatThrownBy(() -> source.loadTopicMappings())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to load topic mappings from S3");
    }

    @Test
    void shouldReturnCorrectDescription() {
        S3TopicMappingSource source =
                new S3TopicMappingSource(
                        minioClient, BUCKET_NAME, OBJECT_KEY, Duration.ofMinutes(5));

        String description = source.getDescription();
        assertThat(description)
                .isEqualTo("S3 Topic Mappings: s3://test-bucket/topic-mappings.json");
    }

    @Test
    void shouldSupportRefresh() {
        S3TopicMappingSource source =
                new S3TopicMappingSource(
                        minioClient, BUCKET_NAME, OBJECT_KEY, Duration.ofMinutes(5));

        assertThat(source.supportsRefresh()).isTrue();
    }

    @Test
    void shouldInvalidateCache() throws Exception {
        String jsonContent = "{\"topic1\": \"Message1\"}";

        // Upload JSON to S3
        uploadJsonToS3(jsonContent);

        S3TopicMappingSource source =
                new S3TopicMappingSource(
                        minioClient, BUCKET_NAME, OBJECT_KEY, Duration.ofMinutes(5));

        // Load initial data
        source.loadTopicMappings();

        // Invalidate cache
        source.invalidateCache();

        // Next load should work (this verifies cache invalidation doesn't break functionality)
        Map<String, String> mappings = source.loadTopicMappings();
        assertThat(mappings).hasSize(1);
    }

    private void uploadJsonToS3(String jsonContent) throws Exception {
        minioClient.putObject(
                PutObjectArgs.builder().bucket(BUCKET_NAME).object(OBJECT_KEY).stream(
                                new ByteArrayInputStream(jsonContent.getBytes()),
                                jsonContent.getBytes().length,
                                -1)
                        .contentType("application/json")
                        .build());
    }
}
