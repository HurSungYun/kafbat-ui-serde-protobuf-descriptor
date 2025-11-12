package io.github.hursungyun.kafbat.ui.serde;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schedules periodic refresh of descriptor sources in the background using a daemon thread.
 *
 * <p>This scheduler runs the provided refresh task at a fixed interval. The thread is configured as
 * a daemon thread so it won't prevent JVM shutdown.
 */
class DescriptorRefreshScheduler {

    private static final Logger logger = LoggerFactory.getLogger(DescriptorRefreshScheduler.class);

    private final Runnable refreshTask;
    private final long intervalSeconds;
    private final String description;
    private ScheduledExecutorService scheduler;

    /**
     * Creates a new refresh scheduler.
     *
     * @param refreshTask The task to run periodically
     * @param intervalSeconds The interval in seconds between refresh executions
     * @param description A description of what's being refreshed (for logging)
     */
    DescriptorRefreshScheduler(Runnable refreshTask, long intervalSeconds, String description) {
        if (refreshTask == null) {
            throw new IllegalArgumentException("refreshTask cannot be null");
        }
        if (intervalSeconds <= 0) {
            throw new IllegalArgumentException(
                    "intervalSeconds must be positive, got: " + intervalSeconds);
        }
        if (description == null || description.trim().isEmpty()) {
            throw new IllegalArgumentException("description cannot be null or empty");
        }

        this.refreshTask = refreshTask;
        this.intervalSeconds = intervalSeconds;
        this.description = description;
    }

    /**
     * Starts the background refresh scheduler. If a scheduler is already running, it will be
     * stopped first.
     */
    void start() {
        // Stop existing scheduler if any
        stop();

        // Create new scheduler with daemon thread
        scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread thread = new Thread(r, "protobuf-descriptor-refresh");
                            thread.setDaemon(true);
                            return thread;
                        });

        // Schedule periodic refresh
        scheduler.scheduleAtFixedRate(
                () -> {
                    try {
                        logger.debug("Running background refresh");
                        refreshTask.run();
                    } catch (Exception e) {
                        logger.error("Background refresh failed", e);
                    }
                },
                intervalSeconds, // Initial delay
                intervalSeconds, // Period
                TimeUnit.SECONDS);

        logger.info(
                "Background refresh started with interval: {} seconds for sources: {}",
                intervalSeconds,
                description);
    }

    /**
     * Stops the background refresh scheduler if it's running. This method blocks until the
     * scheduler is fully shut down or timeout is reached.
     */
    void stop() {
        if (scheduler != null && !scheduler.isShutdown()) {
            logger.debug("Stopping background refresh scheduler");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            scheduler = null;
        }
    }

    /** Returns true if the scheduler is running */
    boolean isRunning() {
        return scheduler != null && !scheduler.isShutdown();
    }
}
