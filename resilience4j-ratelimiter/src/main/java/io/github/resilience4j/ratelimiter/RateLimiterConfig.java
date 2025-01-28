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
package io.github.resilience4j.ratelimiter;

import io.github.resilience4j.core.functions.Either;
import io.github.resilience4j.core.lang.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class RateLimiterConfig implements Serializable {

    private static final long serialVersionUID = -1621614587284115957L;

    private static final String TIMEOUT_DURATION_MUST_NOT_BE_NULL = "TimeoutDuration must not be null";
    private static final String TIMEOUT_DURATION_TOO_LARGE = "TimeoutDuration too large";
    private static final String LIMIT_REFRESH_PERIOD_MUST_NOT_BE_NULL = "LimitRefreshPeriod must not be null";
    private static final String LIMIT_REFRESH_PERIOD_TOO_LARGE = "LimitRefreshPeriod too large";
    private static final String STARTED_TIME_MUST_NOT_BE_NULL = "StartedTime must not be null";
    private static final Duration ACCEPTABLE_REFRESH_PERIOD = Duration.ofNanos(1L);
    private static final boolean DEFAULT_WRITABLE_STACK_TRACE_ENABLED = true;

    private final Duration timeoutDuration;
    private final Duration limitRefreshPeriod;
    private final int limitForPeriod;
    @Nullable
    private final Instant startedTime;
    private final transient Predicate<Either<? extends Throwable, ?>> drainPermissionsOnResult;
    private final boolean writableStackTraceEnabled;

    private RateLimiterConfig(Duration timeoutDuration,
                              Duration limitRefreshPeriod,
                              int limitForPeriod,
                              @Nullable Instant startedTime,
                              Predicate<Either<? extends Throwable, ?>> drainPermissionsOnResult,
                              boolean writableStackTraceEnabled) {
        this.timeoutDuration = timeoutDuration;
        this.limitRefreshPeriod = limitRefreshPeriod;
        this.limitForPeriod = limitForPeriod;
        this.startedTime = startedTime;
        this.drainPermissionsOnResult = drainPermissionsOnResult;
        this.writableStackTraceEnabled = writableStackTraceEnabled;
    }

    /**
     * Returns a builder to create a custom RateLimiterConfig.
     *
     * @return a {@link RateLimiterConfig.Builder}
     */
    public static Builder custom() {
        return new Builder();
    }

    /**
     * Returns a builder to create a custom RateLimiterConfig using specified config as prototype
     *
     * @param prototype A {@link RateLimiterConfig} prototype.
     * @return a {@link RateLimiterConfig.Builder}
     */
    public static Builder from(RateLimiterConfig prototype) {
        return new Builder(prototype);
    }

    /**
     * Creates a default RateLimiter configuration.
     *
     * @return a default RateLimiter configuration.
     */
    public static RateLimiterConfig ofDefaults() {
        return new Builder().build();
    }

    private static Duration checkTimeoutDuration(final Duration timeoutDuration) {
        requireNonNull(timeoutDuration, TIMEOUT_DURATION_MUST_NOT_BE_NULL);
        if (timeoutDuration.isNegative()) {
            throw new IllegalArgumentException("TimeoutDuration must not be negative");
        }
        return validateDurationWithinRange(timeoutDuration, TIMEOUT_DURATION_TOO_LARGE);
    }

    private static Duration validateDurationWithinRange(Duration duration, String message) {
        try {
            return Duration.ofNanos(duration.toNanos()); // make sure there is no long overflow
        } catch (Exception e) {
            throw new RuntimeException(message, e);
        }
    }

    private static Duration checkLimitRefreshPeriod(Duration limitRefreshPeriod) {
        validateDurationWithinRange(
            requireNonNull(limitRefreshPeriod, LIMIT_REFRESH_PERIOD_MUST_NOT_BE_NULL), LIMIT_REFRESH_PERIOD_TOO_LARGE);
        boolean refreshPeriodIsTooShort =
            limitRefreshPeriod.compareTo(ACCEPTABLE_REFRESH_PERIOD) < 0;
        if (refreshPeriodIsTooShort) {
            throw new IllegalArgumentException("LimitRefreshPeriod is too short");
        }
        return limitRefreshPeriod;
    }

    private static int checkLimitForPeriod(final int limitForPeriod) {
        if (limitForPeriod < 1) {
            throw new IllegalArgumentException("LimitForPeriod should be greater than 0");
        }
        return limitForPeriod;
    }

    private static Instant checkStartedTime(final Instant startedTime) {
        requireNonNull(startedTime, STARTED_TIME_MUST_NOT_BE_NULL);
        if (startedTime.isAfter(Instant.now())) {
            throw new IllegalArgumentException("StartedTime should be earlier or equal than now");
        }
        return startedTime;
    }

    public Duration getTimeoutDuration() {
        return timeoutDuration;
    }

    public Duration getLimitRefreshPeriod() {
        return limitRefreshPeriod;
    }

    public int getLimitForPeriod() {
        return limitForPeriod;
    }

    @Nullable
    public Instant getStartedTime() {
        return startedTime;
    }

    public Predicate<Either<? extends Throwable, ?>> getDrainPermissionsOnResult() {
        return drainPermissionsOnResult;
    }

    public boolean isWritableStackTraceEnabled() {
        return writableStackTraceEnabled;
    }

    @Override
    public String toString() {
        return "RateLimiterConfig{" +
            "timeoutDuration=" + timeoutDuration +
            ", limitRefreshPeriod=" + limitRefreshPeriod +
            ", limitForPeriod=" + limitForPeriod +
            ", writableStackTraceEnabled=" + writableStackTraceEnabled +
            '}';
    }

    public static class Builder {

        private Duration timeoutDuration = Duration.ofSeconds(5);
        private Duration limitRefreshPeriod = Duration.ofNanos(500);
        private int limitForPeriod = 50;
        @Nullable
        private Instant startedTime;
        private Predicate<Either<? extends Throwable, ?>> drainPermissionsOnResult = any -> false;
        private boolean writableStackTraceEnabled = DEFAULT_WRITABLE_STACK_TRACE_ENABLED;

        public Builder() {
        }

        public Builder(RateLimiterConfig prototype) {
            this.timeoutDuration = prototype.timeoutDuration;
            this.limitRefreshPeriod = prototype.limitRefreshPeriod;
            this.limitForPeriod = prototype.limitForPeriod;
            this.startedTime = prototype.startedTime;
            this.drainPermissionsOnResult = prototype.drainPermissionsOnResult;
            this.writableStackTraceEnabled = prototype.writableStackTraceEnabled;
        }

        /**
         * Builds a RateLimiterConfig
         *
         * @return the RateLimiterConfig
         */
        public RateLimiterConfig build() {
            return new RateLimiterConfig(timeoutDuration, limitRefreshPeriod, limitForPeriod, startedTime,
                drainPermissionsOnResult, writableStackTraceEnabled);
        }

        /**
         * Enables writable stack traces. When set to false, {@link Exception#getStackTrace()}
         * returns a zero length array. This may be used to reduce log spam when the circuit breaker
         * is open as the cause of the exceptions is already known (the circuit breaker is
         * short-circuiting calls).
         *
         * @param writableStackTraceEnabled flag to control if stack trace is writable
         * @return the BulkheadConfig.Builder
         */
        public Builder writableStackTraceEnabled(boolean writableStackTraceEnabled) {
            this.writableStackTraceEnabled = writableStackTraceEnabled;
            return this;
        }

        /**
         * Allows you to check the result of a call decorated by this rate limiter and make a decision
         * should we drain all the permissions left it the current period. Useful in situations when
         * despite using a RateLimiter the underlining called service will say that you passed the maximum number of calls for a given period.
         *
         * @param drainPermissionsOnResult your function should return true when the permissions drain
         *                                 should happen
         * @return the RateLimiterConfig.Builder
         * @see RateLimiter#drainPermissions()
         */
        public Builder drainPermissionsOnResult(
            Predicate<Either<? extends Throwable, ?>> drainPermissionsOnResult) {
            this.drainPermissionsOnResult = drainPermissionsOnResult;
            return this;
        }

        /**
         * Configures the default wait for permission duration. Default value is 5 seconds.
         *
         * @param timeoutDuration the default wait for permission duration
         * @return the RateLimiterConfig.Builder
         */
        public Builder timeoutDuration(final Duration timeoutDuration) {
            this.timeoutDuration = checkTimeoutDuration(timeoutDuration);
            return this;
        }

        /**
         * Configures the period of limit refresh. After each period rate limiter sets its
         * permissions count to {@link RateLimiterConfig#limitForPeriod} value. Default value is 500
         * nanoseconds.
         *
         * @param limitRefreshPeriod the period of limit refresh
         * @return the RateLimiterConfig.Builder
         */
        public Builder limitRefreshPeriod(final Duration limitRefreshPeriod) {
            this.limitRefreshPeriod = checkLimitRefreshPeriod(limitRefreshPeriod);
            return this;
        }

        /**
         * Configures the permissions limit for refresh period. Count of permissions available
         * during one rate limiter period specified by {@link RateLimiterConfig#limitRefreshPeriod}
         * value. Default value is 50.
         *
         * @param limitForPeriod the permissions limit for refresh period
         * @return the RateLimiterConfig.Builder
         */
        public Builder limitForPeriod(final int limitForPeriod) {
            this.limitForPeriod = checkLimitForPeriod(limitForPeriod);
            return this;
        }

        /**
         * Configures the started time of first period that should be earlier or equal than now.
         * This can be particularly useful when synchronize the period of rate limiter on both server and client.
         *
         * @param startedTime the started time of the first period that should be earlier or equal than now
         * @return the RateLimiterConfig.Builder
         */
        public Builder startedTime(final Instant startedTime) {
            this.startedTime = checkStartedTime(startedTime);
            return this;
        }

    }
}
