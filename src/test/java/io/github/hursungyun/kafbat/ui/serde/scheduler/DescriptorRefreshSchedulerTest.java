package io.github.hursungyun.kafbat.ui.serde.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DescriptorRefreshSchedulerTest {

    @Test
    void shouldRejectNullRefreshTask() {
        assertThatThrownBy(() -> new DescriptorRefreshScheduler(null, 60, "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refreshTask cannot be null");
    }

    @Test
    void shouldRejectNegativeInterval() {
        assertThatThrownBy(() -> new DescriptorRefreshScheduler(() -> {}, -1, "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intervalSeconds must be positive");
    }

    @Test
    void shouldRejectZeroInterval() {
        assertThatThrownBy(() -> new DescriptorRefreshScheduler(() -> {}, 0, "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intervalSeconds must be positive");
    }

    @Test
    void shouldRejectNullDescription() {
        assertThatThrownBy(() -> new DescriptorRefreshScheduler(() -> {}, 60, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("description cannot be null or empty");
    }

    @Test
    void shouldRejectEmptyDescription() {
        assertThatThrownBy(() -> new DescriptorRefreshScheduler(() -> {}, 60, "  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("description cannot be null or empty");
    }

    @Test
    void shouldExecuteRefreshTaskPeriodically() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(2); // Wait for at least 2 executions

        Runnable task =
                () -> {
                    counter.incrementAndGet();
                    latch.countDown();
                };

        DescriptorRefreshScheduler scheduler =
                new DescriptorRefreshScheduler(task, 1, "test source"); // 1 second interval

        assertThat(scheduler.isRunning()).isFalse();

        scheduler.start();
        assertThat(scheduler.isRunning()).isTrue();

        // Wait for at least 2 executions (with some buffer for timing)
        boolean completed = latch.await(5, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(counter.get()).isGreaterThanOrEqualTo(2);

        scheduler.stop();
        assertThat(scheduler.isRunning()).isFalse();
    }

    @Test
    void shouldHandleExceptionsInRefreshTask() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(2);

        Runnable task =
                () -> {
                    counter.incrementAndGet();
                    latch.countDown();
                    if (counter.get() == 1) {
                        throw new RuntimeException("Test exception");
                    }
                };

        DescriptorRefreshScheduler scheduler = new DescriptorRefreshScheduler(task, 1, "test");

        scheduler.start();

        // Wait for at least 2 executions - scheduler should continue after exception
        boolean completed = latch.await(5, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(counter.get()).isGreaterThanOrEqualTo(2);

        scheduler.stop();
    }

    @Test
    void shouldStopPreviousSchedulerWhenStartingNew() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        Runnable task = counter::incrementAndGet;

        DescriptorRefreshScheduler scheduler = new DescriptorRefreshScheduler(task, 1, "test");

        // Start once
        scheduler.start();
        assertThat(scheduler.isRunning()).isTrue();

        // Start again - should stop previous and start new
        scheduler.start();
        assertThat(scheduler.isRunning()).isTrue();

        Thread.sleep(2500); // Let it run for ~2 executions

        scheduler.stop();
        assertThat(scheduler.isRunning()).isFalse();

        // Should have executed at least once
        assertThat(counter.get()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void shouldAllowStopWithoutStart() {
        DescriptorRefreshScheduler scheduler = new DescriptorRefreshScheduler(() -> {}, 60, "test");

        // Should not throw
        scheduler.stop();
        assertThat(scheduler.isRunning()).isFalse();
    }

    @Test
    void shouldAllowMultipleStopCalls() throws InterruptedException {
        DescriptorRefreshScheduler scheduler = new DescriptorRefreshScheduler(() -> {}, 1, "test");

        scheduler.start();
        assertThat(scheduler.isRunning()).isTrue();

        scheduler.stop();
        assertThat(scheduler.isRunning()).isFalse();

        // Second stop should be safe
        scheduler.stop();
        assertThat(scheduler.isRunning()).isFalse();
    }
}
