package io.github.hursungyun.kafbat.ui.serde.sources;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Source for loading topic-to-message-type mappings from S3 JSON files
 */
public class S3TopicMappingSource {

    private final MinioClient minioClient;
    private final String bucketName;
    private final String objectKey;
    private final Duration refreshInterval;
    private final ObjectMapper objectMapper;

    // Caching
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile Map<String, String> cachedTopicMappings;
    private volatile Instant lastRefresh;
    private volatile String lastETag;

    public S3TopicMappingSource(MinioClient minioClient, String bucketName, String objectKey, Duration refreshInterval) {
        this.minioClient = minioClient;
        this.bucketName = bucketName;
        this.objectKey = objectKey;
        this.refreshInterval = refreshInterval;
        this.objectMapper = new ObjectMapper();
    }

    public Map<String, String> loadTopicMappings() throws IOException {
        lock.readLock().lock();
        try {
            // Check if we have a cached version and it's still valid
            if (cachedTopicMappings != null && !shouldRefresh()) {
                return cachedTopicMappings;
            }
        } finally {
            lock.readLock().unlock();
        }

        // Need to refresh - acquire write lock
        lock.writeLock().lock();
        try {
            // Double-check pattern - another thread might have refreshed while we waited
            if (cachedTopicMappings != null && !shouldRefresh()) {
                return cachedTopicMappings;
            }

            // Check if object has changed on S3
            StatObjectResponse stat = getObjectStat();
            String currentETag = stat.etag();

            if (cachedTopicMappings != null && currentETag.equals(lastETag)) {
                // Object hasn't changed, just update refresh time
                lastRefresh = Instant.now();
                return cachedTopicMappings;
            }

            // Load fresh copy from S3
            try (InputStream inputStream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectKey)
                            .build())) {

                // Parse JSON as Map<String, String>
                TypeReference<Map<String, String>> typeRef = new TypeReference<Map<String, String>>() {};
                cachedTopicMappings = objectMapper.readValue(inputStream, typeRef);
                lastRefresh = Instant.now();
                lastETag = currentETag;

                return cachedTopicMappings;
            }

        } catch (Exception e) {
            throw new IOException("Failed to load topic mappings from S3: s3://" + bucketName + "/" + objectKey, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Optional<Instant> getLastModified() {
        try {
            StatObjectResponse stat = getObjectStat();
            return Optional.of(stat.lastModified().toInstant());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public String getDescription() {
        return "S3 Topic Mappings: s3://" + bucketName + "/" + objectKey;
    }

    public boolean supportsRefresh() {
        return true;
    }

    /**
     * Force refresh of the cached topic mappings on next load
     */
    public void invalidateCache() {
        lock.writeLock().lock();
        try {
            lastRefresh = null;
            lastETag = null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean shouldRefresh() {
        return lastRefresh == null ||
               Duration.between(lastRefresh, Instant.now()).compareTo(refreshInterval) >= 0;
    }

    private StatObjectResponse getObjectStat() throws IOException {
        try {
            return minioClient.statObject(
                    StatObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectKey)
                            .build());
        } catch (ErrorResponseException | InsufficientDataException | InternalException |
                 InvalidKeyException | InvalidResponseException | NoSuchAlgorithmException |
                 ServerException | XmlParserException e) {
            throw new IOException("Failed to get S3 object stat for s3://" + bucketName + "/" + objectKey, e);
        }
    }
}