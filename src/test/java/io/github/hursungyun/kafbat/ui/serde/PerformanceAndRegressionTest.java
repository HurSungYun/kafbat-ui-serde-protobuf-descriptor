package io.github.hursungyun.kafbat.ui.serde;

import io.kafbat.ui.serde.api.PropertyResolver;
import io.kafbat.ui.serde.api.Serde;
import io.github.hursungyun.kafbat.ui.serde.test.UserProtos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/**
 * Performance and regression tests for production readiness
 */
class PerformanceAndRegressionTest {

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
    void shouldHandleLargeMessages() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile);

        // Create large message (1MB+ with repeated fields)
        UserProtos.User.Builder userBuilder = UserProtos.User.newBuilder()
                .setId(12345)
                .setName("Large User")
                .setEmail("large@example.com");

        // Add many repeated fields to create large message
        String longContent = "tag_content_".repeat(100); // ~1100 chars each
        for (int i = 0; i < 1000; i++) {
            userBuilder.addTags(longContent + "_" + i);
        }

        UserProtos.User largeUser = userBuilder.build();
        byte[] largeMessageBytes = largeUser.toByteArray();
        
        // Verify message is reasonably large (should be > 500KB)
        assertThat(largeMessageBytes.length).isGreaterThan(500 * 1024);

        // Test deserialization performance
        long startTime = System.currentTimeMillis();
        var result = serde.deserializer("test-topic", Serde.Target.VALUE)
                .deserialize(null, largeMessageBytes);
        long duration = System.currentTimeMillis() - startTime;

        assertThat(result.getResult()).contains("Large User");
        assertThat(result.getResult()).contains("tag_content_");
        assertThat(duration).isLessThan(1000); // Should complete within 1 second
    }

    @Test
    void shouldBeThreadSafe() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile);

        // Create test message
        UserProtos.User user = UserProtos.User.newBuilder()
                .setId(123)
                .setName("Concurrent User")
                .setEmail("concurrent@example.com")
                .build();
        byte[] messageBytes = user.toByteArray();

        // Test concurrent access
        int threadCount = 10;
        int messagesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < messagesPerThread; j++) {
                        var result = serde.deserializer("test-topic", Serde.Target.VALUE)
                                .deserialize(null, messageBytes);
                        if (result.getResult().contains("Concurrent User")) {
                            successCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(successCount.get()).isEqualTo(threadCount * messagesPerThread);
        assertThat(errorCount.get()).isZero();
    }

    @Test
    void shouldHandleNoMessageTypeConfiguredGracefully() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        
        // Configure without default message type or topic mappings
        when(serdeProperties.getProperty("descriptor.value.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        mockS3PropertiesEmpty();
        when(serdeProperties.getProperty("message.value.default.type", String.class))
                .thenReturn(Optional.empty());
        when(serdeProperties.getMapProperty("topic.mapping.value.local", String.class, String.class))
                .thenReturn(Optional.empty());

        serde.configure(serdeProperties, clusterProperties, appProperties);

        // Should indicate cannot deserialize
        assertThat(serde.canDeserialize("unmapped-topic", Serde.Target.VALUE)).isFalse();

        // Should throw meaningful error when attempting to deserialize
        byte[] testBytes = {0x01, 0x02, 0x03};
        assertThatThrownBy(() -> serde.deserializer("unmapped-topic", Serde.Target.VALUE)
                .deserialize(null, testBytes))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No message type configured for topic: unmapped-topic");
    }

    @Test
    void shouldHandleInvalidMessageBytesGracefully() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile);

        // Test with completely invalid protobuf bytes
        byte[] invalidBytes = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        
        assertThatThrownBy(() -> serde.deserializer("test-topic", Serde.Target.VALUE)
                .deserialize(null, invalidBytes))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to deserialize protobuf message for topic test-topic");
    }

    @Test
    void shouldHandleEmptyMessageBytes() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile);

        // Test with empty byte array - protobuf should handle this gracefully
        // Empty message is actually valid in protobuf (all fields have default values)
        byte[] emptyBytes = new byte[0];
        
        var result = serde.deserializer("test-topic", Serde.Target.VALUE)
                .deserialize(null, emptyBytes);
        
        // Should deserialize to empty User message with default values
        assertThat(result.getResult()).contains("id");
        assertThat(result.getType()).isEqualTo(io.kafbat.ui.serde.api.DeserializeResult.Type.JSON);
    }

    @Test
    void shouldNotSupportKeyDeserialization() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile);

        // Keys are not supported
        assertThat(serde.canDeserialize("test-topic", Serde.Target.KEY)).isFalse();
    }

    @Test
    void shouldSupportSerialization() throws Exception {
        Path descriptorFile = copyDescriptorSetToTemp();
        configureSerde(descriptorFile);

        // Serialization now supported for configured message types
        assertThat(serde.canSerialize("test-topic", Serde.Target.VALUE)).isTrue();
        assertThat(serde.canSerialize("test-topic", Serde.Target.KEY)).isFalse(); // Keys still not supported
    }

    private void configureSerde(Path descriptorFile) {
        when(serdeProperties.getProperty("descriptor.value.file", String.class))
                .thenReturn(Optional.of(descriptorFile.toString()));
        mockS3PropertiesEmpty();
        when(serdeProperties.getProperty("message.value.default.type", String.class))
                .thenReturn(Optional.of("test.User"));
        when(serdeProperties.getMapProperty("topic.mapping.value.local", String.class, String.class))
                .thenReturn(Optional.empty());

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