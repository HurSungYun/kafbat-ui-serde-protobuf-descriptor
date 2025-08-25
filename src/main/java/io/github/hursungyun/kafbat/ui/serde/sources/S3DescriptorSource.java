package io.github.hursungyun.kafbat.ui.serde.sources;

import com.google.protobuf.DescriptorProtos;
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
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Descriptor source that loads from S3 with caching support */
public class S3DescriptorSource implements DescriptorSource {

    private static final Logger logger = LoggerFactory.getLogger(S3DescriptorSource.class);

    private final MinioClient minioClient;
    private final String bucketName;
    private final String objectKey;
    private final Duration refreshInterval;

    // Caching
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile DescriptorProtos.FileDescriptorSet cachedDescriptorSet;
    private volatile Instant lastRefresh;
    private volatile String lastETag;

    public S3DescriptorSource(
            MinioClient minioClient,
            String bucketName,
            String objectKey,
            Duration refreshInterval) {
        if (minioClient == null) {
            throw new IllegalArgumentException("minioClient cannot be null");
        }
        if (bucketName == null || bucketName.trim().isEmpty()) {
            throw new IllegalArgumentException("bucketName cannot be null or empty");
        }
        if (objectKey == null || objectKey.trim().isEmpty()) {
            throw new IllegalArgumentException("objectKey cannot be null or empty");
        }
        if (refreshInterval == null || refreshInterval.isNegative()) {
            throw new IllegalArgumentException("refreshInterval cannot be null or negative");
        }

        this.minioClient = minioClient;
        this.bucketName = bucketName.trim();
        this.objectKey = objectKey.trim();
        this.refreshInterval = refreshInterval;
    }

    @Override
    public DescriptorProtos.FileDescriptorSet loadDescriptorSet() throws IOException {
        lock.readLock().lock();
        try {
            // Check if we have a cached version and it's still valid
            if (cachedDescriptorSet != null && !shouldRefresh()) {
                return cachedDescriptorSet;
            }
        } finally {
            lock.readLock().unlock();
        }

        // Need to refresh - acquire write lock
        lock.writeLock().lock();
        try {
            // Double-check pattern - another thread might have refreshed while we waited
            if (cachedDescriptorSet != null && !shouldRefresh()) {
                return cachedDescriptorSet;
            }

            // Check if object has changed on S3
            StatObjectResponse stat = getObjectStat();
            String currentETag = stat.etag();

            if (cachedDescriptorSet != null && currentETag.equals(lastETag)) {
                // Object hasn't changed, just update refresh time
                lastRefresh = Instant.now();
                return cachedDescriptorSet;
            }

            // Load fresh copy from S3 (atomic update)
            DescriptorProtos.FileDescriptorSet newDescriptorSet;
            try (InputStream inputStream =
                    minioClient.getObject(
                            GetObjectArgs.builder().bucket(bucketName).object(objectKey).build())) {

                newDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(inputStream);
            }

            // Atomic update - only update cache if parsing succeeded
            cachedDescriptorSet = newDescriptorSet;
            lastRefresh = Instant.now();
            lastETag = currentETag;

            logger.debug(
                    "Successfully refreshed descriptor set from S3: s3://{}/{}",
                    bucketName,
                    objectKey);
            return cachedDescriptorSet;

        } catch (Exception e) {
            // Log warning but preserve existing functionality
            logger.warn(
                    "Failed to refresh descriptor set from S3: s3://{}/{}. Error: {}. {}",
                    bucketName,
                    objectKey,
                    e.getMessage(),
                    cachedDescriptorSet != null
                            ? "Continuing with existing cached data."
                            : "No cached data available.");

            // If we have cached data, return it; otherwise throw exception
            if (cachedDescriptorSet != null) {
                logger.info(
                        "Using previously cached descriptor set from S3: s3://{}/{}",
                        bucketName,
                        objectKey);
                return cachedDescriptorSet;
            } else {
                // No cached data available - this is the first load, so throw exception
                throw new IOException(
                        "Failed to load descriptor set from S3: s3://"
                                + bucketName
                                + "/"
                                + objectKey
                                + " and no cached data available",
                        e);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<Instant> getLastModified() {
        try {
            StatObjectResponse stat = getObjectStat();
            return Optional.of(stat.lastModified().toInstant());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public String getDescription() {
        return "S3: s3://" + bucketName + "/" + objectKey;
    }

    @Override
    public boolean supportsRefresh() {
        return true;
    }

    /** Force refresh of the cached descriptor set on next load */
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
        return lastRefresh == null
                || Duration.between(lastRefresh, Instant.now()).compareTo(refreshInterval) >= 0;
    }

    private StatObjectResponse getObjectStat() throws IOException {
        try {
            return minioClient.statObject(
                    StatObjectArgs.builder().bucket(bucketName).object(objectKey).build());
        } catch (ErrorResponseException
                | InsufficientDataException
                | InternalException
                | InvalidKeyException
                | InvalidResponseException
                | NoSuchAlgorithmException
                | ServerException
                | XmlParserException e) {
            throw new IOException(
                    "Failed to get S3 object stat for s3://" + bucketName + "/" + objectKey, e);
        }
    }
}
