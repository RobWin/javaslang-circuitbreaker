/*
 *
 *  Copyright 2016 Robert Winkler and Bohdan Storozhuk
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */
package io.github.resilience4j.ratelimiter.internal;

import io.github.resilience4j.core.lang.Nullable;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.event.RateLimiterOnDrainedEvent;
import io.github.resilience4j.ratelimiter.event.RateLimiterOnFailureEvent;
import io.github.resilience4j.ratelimiter.event.RateLimiterOnSuccessEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.System.nanoTime;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * A RateLimiter implementation that consists of {@link Semaphore} and scheduler that will refresh
 * permissions after each {@link RateLimiterConfig#getLimitRefreshPeriod()}, you can invoke
 * {@link SemaphoreBasedRateLimiter#shutdown()} to close the limiter.
 */
public class SemaphoreBasedRateLimiter implements RateLimiter {

    private static final String NAME_MUST_NOT_BE_NULL = "Name must not be null";
    private static final String CONFIG_MUST_NOT_BE_NULL = "Config must not be null";

    private final String name;
    private final AtomicReference<RateLimiterConfig> rateLimiterConfig;
    private final ScheduledExecutorService scheduler;
    private final Semaphore semaphore;
    private final SemaphoreBasedRateLimiterMetrics metrics;
    private final Map<String, String> tags;
    private final RateLimiterEventProcessor eventProcessor;
    private final ScheduledFuture<?> scheduledFuture;

    /**
     * Creates a RateLimiter.
     *
     * @param name              the name of the RateLimiter
     * @param rateLimiterConfig The RateLimiter configuration.
     */
    public SemaphoreBasedRateLimiter(final String name, final RateLimiterConfig rateLimiterConfig) {
        this(name, rateLimiterConfig, emptyMap());
    }

    /**
     * Creates a RateLimiter.
     *
     * @param name              the name of the RateLimiter
     * @param rateLimiterConfig The RateLimiter configuration.
     * @param tags              tags to assign to the RateLimiter
     */
    public SemaphoreBasedRateLimiter(final String name, final RateLimiterConfig rateLimiterConfig, Map<String, String> tags) {
        this(name, rateLimiterConfig, null, tags);
    }

    /**
     * Creates a RateLimiter.
     *
     * @param name              the name of the RateLimiter
     * @param rateLimiterConfig The RateLimiter configuration.
     * @param scheduler         executor that will refresh permissions
     */
    public SemaphoreBasedRateLimiter(String name, RateLimiterConfig rateLimiterConfig,
        @Nullable ScheduledExecutorService scheduler) {
        this(name, rateLimiterConfig, scheduler, emptyMap());
    }

    /**
     * Creates a RateLimiter.
     *
     * @param name              the name of the RateLimiter
     * @param rateLimiterConfig The RateLimiter configuration.
     * @param scheduler         executor that will refresh permissions
     * @param tags              tags to assign to the RateLimiter
     */
    public SemaphoreBasedRateLimiter(String name, RateLimiterConfig rateLimiterConfig,
                                     @Nullable ScheduledExecutorService scheduler, Map<String, String> tags) {
        this.name = requireNonNull(name, NAME_MUST_NOT_BE_NULL);
        this.rateLimiterConfig = new AtomicReference<>(
            requireNonNull(rateLimiterConfig, CONFIG_MUST_NOT_BE_NULL));

        this.scheduler = Optional.ofNullable(scheduler).orElseGet(this::configureScheduler);
        this.tags = tags;
        this.semaphore = new Semaphore(this.rateLimiterConfig.get().getLimitForPeriod(), true);
        this.metrics = this.new SemaphoreBasedRateLimiterMetrics();

        this.eventProcessor = new RateLimiterEventProcessor();

        this.scheduledFuture = scheduleLimitRefresh();
    }

    private ScheduledExecutorService configureScheduler() {
        ThreadFactory threadFactory = target -> {
            Thread thread = new Thread(target, "SchedulerForSemaphoreBasedRateLimiterImpl-" + name);
            thread.setDaemon(true);
            return thread;
        };
        return newSingleThreadScheduledExecutor(threadFactory);
    }

    private ScheduledFuture<?> scheduleLimitRefresh() {
        Instant startedTime = this.rateLimiterConfig.get().getStartedTime();
        long refreshPeriodNano = this.rateLimiterConfig.get().getLimitRefreshPeriod().toNanos();

        if (startedTime == null) {
            return scheduler.scheduleAtFixedRate(
                    this::refreshLimit,
                    refreshPeriodNano,
                    refreshPeriodNano,
                    TimeUnit.NANOSECONDS
            );
        }

        long elapsedNano = nanoTime() - startedTime.getNano();
        long initialDelayNano = refreshPeriodNano - (elapsedNano % refreshPeriodNano);

        return scheduler.scheduleAtFixedRate(
            this::refreshLimit,
            initialDelayNano,
            refreshPeriodNano,
            TimeUnit.NANOSECONDS
        );
    }

    void refreshLimit() {
        int permissionsToRelease =
            this.rateLimiterConfig.get().getLimitForPeriod() - semaphore.availablePermits();
        semaphore.release(permissionsToRelease);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void changeTimeoutDuration(Duration timeoutDuration) {
        RateLimiterConfig newConfig = RateLimiterConfig.from(rateLimiterConfig.get())
            .timeoutDuration(timeoutDuration)
            .build();
        rateLimiterConfig.set(newConfig);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void changeLimitForPeriod(int limitForPeriod) {
        RateLimiterConfig newConfig = RateLimiterConfig.from(rateLimiterConfig.get())
            .limitForPeriod(limitForPeriod)
            .build();
        rateLimiterConfig.set(newConfig);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean acquirePermission(int permits) {
        try {
            boolean success = semaphore
                .tryAcquire(permits, rateLimiterConfig.get().getTimeoutDuration().toNanos(),
                    TimeUnit.NANOSECONDS);
            publishRateLimiterAcquisitionEvent(success, permits);
            return success;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            publishRateLimiterAcquisitionEvent(false, permits);
            return false;
        }
    }

    /**
     * Reserving permissions is not supported in the semaphore based implementation. Semaphores are
     * totally blocking by it's nature. So this non-blocking API isn't supported. Use {@link
     * #acquirePermission()}
     *
     * @throws UnsupportedOperationException always for this implementation
     */
    @Override
    public long reservePermission() {
        throw new UnsupportedOperationException(
            "Reserving permissions is not supported in the semaphore based implementation");
    }

    /**
     * @throws UnsupportedOperationException always for this implementation
     * @see #reservePermission()
     */
    @Override
    public long reservePermission(int permits) {
        throw new UnsupportedOperationException(
            "Reserving permissions is not supported in the semaphore based implementation");
    }

    @Override
    public void drainPermissions() {
        int permits = semaphore.drainPermits();
        if (eventProcessor.hasConsumers()) {
            eventProcessor.consumeEvent(new RateLimiterOnDrainedEvent(name, permits));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Metrics getMetrics() {
        return this.metrics;
    }

    @Override
    public EventPublisher getEventPublisher() {
        return eventProcessor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RateLimiterConfig getRateLimiterConfig() {
        return this.rateLimiterConfig.get();
    }

    @Override
    public String toString() {
        return "SemaphoreBasedRateLimiter{"
            + "name='" + name + '\''
            + ", rateLimiterConfig=" + rateLimiterConfig
            + '}';
    }

    @Override
    public Map<String, String> getTags() {
        return tags;
    }

    private void publishRateLimiterAcquisitionEvent(boolean permissionAcquired, int permits) {
        if (!eventProcessor.hasConsumers()) {
            return;
        }
        if (permissionAcquired) {
            eventProcessor.consumeEvent(new RateLimiterOnSuccessEvent(name, permits));
            return;
        }
        eventProcessor.consumeEvent(new RateLimiterOnFailureEvent(name, permits));
    }

    /**
     *  Close the scheduled task that refresh permissions if you don't use the {@link  SemaphoreBasedRateLimiter} anymore.
     *  Otherwise, the {@link SemaphoreBasedRateLimiter} instance will not be garbage collected even if you hold the reference,
     *  meaning if you create millions of instance, there could be a memory leak.
     *  (https://github.com/resilience4j/resilience4j/issues/1683)
     */
    public void shutdown()  {
        if (!this.scheduledFuture.isCancelled()) {
            this.scheduledFuture.cancel(true);
        }
    }

    /**
     * {@inheritDoc}
     */
    private final class SemaphoreBasedRateLimiterMetrics implements Metrics {

        private SemaphoreBasedRateLimiterMetrics() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getAvailablePermissions() {
            return semaphore.availablePermits();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getNumberOfWaitingThreads() {
            return semaphore.getQueueLength();
        }
    }
}
