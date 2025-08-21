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

/**
 * Descriptor source that loads from S3 with caching support
 */
public class S3DescriptorSource implements DescriptorSource {
    
    private final MinioClient minioClient;
    private final String bucketName;
    private final String objectKey;
    private final Duration refreshInterval;
    
    // Caching
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile DescriptorProtos.FileDescriptorSet cachedDescriptorSet;
    private volatile Instant lastRefresh;
    private volatile String lastETag;
    
    public S3DescriptorSource(MinioClient minioClient, String bucketName, String objectKey, Duration refreshInterval) {
        this.minioClient = minioClient;
        this.bucketName = bucketName;
        this.objectKey = objectKey;
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
            
            // Load fresh copy from S3
            try (InputStream inputStream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectKey)
                            .build())) {
                
                cachedDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(inputStream);
                lastRefresh = Instant.now();
                lastETag = currentETag;
                
                return cachedDescriptorSet;
            }
            
        } catch (Exception e) {
            throw new IOException("Failed to load descriptor set from S3: s3://" + bucketName + "/" + objectKey, e);
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
    
    /**
     * Force refresh of the cached descriptor set on next load
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