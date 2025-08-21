package io.github.hursungyun.kafbat.ui.serde;

import com.google.protobuf.DescriptorProtos;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class S3DescriptorSourceTest {

    @Container
    static GenericContainer<?> minioContainer = new GenericContainer<>(DockerImageName.parse("minio/minio:latest"))
            .withExposedPorts(9000)
            .withEnv("MINIO_ROOT_USER", "testuser")
            .withEnv("MINIO_ROOT_PASSWORD", "testpassword")
            .withCommand("server", "/data");

    private MinioClient minioClient;
    private static final String BUCKET_NAME = "test-bucket";
    private static final String OBJECT_KEY = "test-descriptors.desc";

    @BeforeEach
    void setUp() throws Exception {
        String endpoint = "http://" + minioContainer.getHost() + ":" + minioContainer.getMappedPort(9000);
        
        minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials("testuser", "testpassword")
                .build();

        // Create test bucket
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(BUCKET_NAME).build())) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build());
        }

        // Upload test descriptor set
        uploadTestDescriptorSet();
    }

    @AfterEach
    void tearDown() {
        // Cleanup is handled by testcontainers
    }

    @Test
    void shouldLoadDescriptorSetFromS3() throws Exception {
        S3DescriptorSource source = new S3DescriptorSource(
                minioClient, BUCKET_NAME, OBJECT_KEY, Duration.ofMinutes(5));

        DescriptorProtos.FileDescriptorSet descriptorSet = source.loadDescriptorSet();
        
        assertThat(descriptorSet).isNotNull();
        assertThat(descriptorSet.getFileCount()).isGreaterThan(0);
    }

    @Test
    void shouldCacheDescriptorSet() throws Exception {
        S3DescriptorSource source = new S3DescriptorSource(
                minioClient, BUCKET_NAME, OBJECT_KEY, Duration.ofMinutes(5));

        // First load
        DescriptorProtos.FileDescriptorSet first = source.loadDescriptorSet();
        
        // Second load should use cache (same object reference due to caching)
        DescriptorProtos.FileDescriptorSet second = source.loadDescriptorSet();
        
        assertThat(first).isEqualTo(second);
    }

    @Test
    void shouldRefreshAfterCacheInvalidation() throws Exception {
        S3DescriptorSource source = new S3DescriptorSource(
                minioClient, BUCKET_NAME, OBJECT_KEY, Duration.ofMinutes(5));

        // First load
        source.loadDescriptorSet();
        
        // Update object in S3
        uploadTestDescriptorSet();
        
        // Invalidate cache
        source.invalidateCache();
        
        // Next load should fetch from S3 again
        DescriptorProtos.FileDescriptorSet refreshed = source.loadDescriptorSet();
        assertThat(refreshed).isNotNull();
    }

    @Test
    void shouldGetLastModified() throws Exception {
        S3DescriptorSource source = new S3DescriptorSource(
                minioClient, BUCKET_NAME, OBJECT_KEY, Duration.ofMinutes(5));

        Optional<Instant> lastModified = source.getLastModified();
        
        assertThat(lastModified).isPresent();
        assertThat(lastModified.get()).isBefore(Instant.now());
    }

    @Test
    void shouldGetDescription() {
        S3DescriptorSource source = new S3DescriptorSource(
                minioClient, BUCKET_NAME, OBJECT_KEY, Duration.ofMinutes(5));

        String description = source.getDescription();
        
        assertThat(description).isEqualTo("S3: s3://" + BUCKET_NAME + "/" + OBJECT_KEY);
    }

    @Test
    void shouldSupportRefresh() {
        S3DescriptorSource source = new S3DescriptorSource(
                minioClient, BUCKET_NAME, OBJECT_KEY, Duration.ofMinutes(5));

        assertThat(source.supportsRefresh()).isTrue();
    }

    @Test
    void shouldThrowExceptionForNonexistentObject() {
        S3DescriptorSource source = new S3DescriptorSource(
                minioClient, BUCKET_NAME, "nonexistent.desc", Duration.ofMinutes(5));

        assertThatThrownBy(source::loadDescriptorSet)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to load descriptor set from S3");
    }

    private void uploadTestDescriptorSet() throws Exception {
        // Load test descriptor set from resources
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
}