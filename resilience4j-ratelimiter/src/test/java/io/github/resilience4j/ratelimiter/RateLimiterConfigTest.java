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
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableCauseMatcher;
import org.junit.rules.ExpectedException;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.Predicate;

import static org.assertj.core.api.BDDAssertions.then;
import static org.hamcrest.CoreMatchers.isA;


public class RateLimiterConfigTest {

    private static final int LIMIT = 50;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REFRESH_PERIOD = Duration.ofNanos(500);
    private static final Instant STARTED_TIME = Instant.now().truncatedTo(ChronoUnit.MICROS);
    private static final Predicate<Either<? extends Throwable, ?>> DRAIN_CONDITION_CHECKER = result -> false;
    private static final String TIMEOUT_DURATION_MUST_NOT_BE_NULL = "TimeoutDuration must not be null";
    private static final String TIMEOUT_DURATION_MUST_NOT_BE_NEGATIVE = "TimeoutDuration must not be negative";
    private static final String REFRESH_PERIOD_MUST_NOT_BE_NULL = "RefreshPeriod must not be null";
    private static final String STARTED_TIME_MUST_NOT_BE_NULL = "StartedTime must not be null";

    @Rule
    public ExpectedException exception = ExpectedException.none();


    @Test
    public void builderPositive() throws Exception {
        RateLimiterConfig config = RateLimiterConfig.custom()
            .timeoutDuration(TIMEOUT)
            .limitRefreshPeriod(REFRESH_PERIOD)
            .limitForPeriod(LIMIT)
            .startedTime(STARTED_TIME)
            .drainPermissionsOnResult(DRAIN_CONDITION_CHECKER)
            .build();

        then(config.getLimitForPeriod()).isEqualTo(LIMIT);
        then(config.getLimitRefreshPeriod()).isEqualTo(REFRESH_PERIOD);
        then(config.getTimeoutDuration()).isEqualTo(TIMEOUT);
        then(config.getStartedTime()).isEqualTo(STARTED_TIME);
        then(config.getDrainPermissionsOnResult()).isEqualTo(DRAIN_CONDITION_CHECKER);
    }

    @Test
    public void builderTimeoutIsNull() throws Exception {
        exception.expect(NullPointerException.class);
        exception.expectMessage(TIMEOUT_DURATION_MUST_NOT_BE_NULL);
        RateLimiterConfig.custom()
            .timeoutDuration(null);
    }

    @Test
    public void builderTimeoutIsNegative() throws Exception {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(TIMEOUT_DURATION_MUST_NOT_BE_NEGATIVE);
        RateLimiterConfig.custom()
            .timeoutDuration(Duration.ofNanos(-1));
    }

    @Test
    public void builderRefreshPeriodIsNull() throws Exception {
        exception.expect(NullPointerException.class);
        exception.expectMessage(REFRESH_PERIOD_MUST_NOT_BE_NULL);
        RateLimiterConfig.custom()
            .limitRefreshPeriod(null);
    }

    @Test
    public void builderRefreshPeriodTooShort() throws Exception {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("RefreshPeriod is too short");
        RateLimiterConfig.custom()
            .timeoutDuration(TIMEOUT)
            .limitRefreshPeriod(Duration.ZERO)
            .limitForPeriod(LIMIT)
            .build();
    }
    @Test
    public void builderRefreshPeriodNegative() throws Exception {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("RefreshPeriod is too short");
        RateLimiterConfig.custom()
            .timeoutDuration(TIMEOUT)
            .limitRefreshPeriod(Duration.ofNanos(-1))
            .limitForPeriod(LIMIT)
            .build();
    }

    @Test
    public void builderLimitIsLessThanOne() throws Exception {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("LimitForPeriod should be greater than 0");
        RateLimiterConfig.custom()
            .limitForPeriod(0);
    }
    @Test
    public void buildTimeoutDurationIsNotWithinLimits() {
        exception.expect(ThrowableCauseMatcher.hasCause(isA(ArithmeticException.class)));
        exception.expectMessage("TimeoutDuration too large");
        RateLimiterConfig.custom()
            .timeoutDuration(Duration.ofSeconds(Long.MAX_VALUE));
    }

    @Test
    public void buildLimitRefreshPeriodIsNotWithinLimits() {
        exception.expect(ThrowableCauseMatcher.hasCause(isA(ArithmeticException.class)));
        exception.expectMessage("LimitRefreshPeriod too large");
        RateLimiterConfig.custom()
            .limitRefreshPeriod(Duration.ofSeconds(Long.MAX_VALUE));
    }

    @Test
    public void builderStartedTimeIsNull() {
        exception.expect(NullPointerException.class);
        exception.expectMessage(STARTED_TIME_MUST_NOT_BE_NULL);
        RateLimiterConfig.custom()
                .startedTime(null);
    }

    @Test
    public void builderStartedTimeAfterNow() throws Exception {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("StartedTime should be earlier or equal than now");
        RateLimiterConfig.custom()
                .startedTime(Instant.now().plusSeconds(600L));
    }
}
