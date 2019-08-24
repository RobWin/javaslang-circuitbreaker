/*
 *
 *  Copyright 2019 authors
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
package io.github.resilience4j.metrics;

import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import io.vavr.collection.Array;

import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;

import static com.codahale.metrics.MetricRegistry.name;
import static io.github.resilience4j.timelimiter.utils.MetricNames.DEFAULT_PREFIX;
import static io.github.resilience4j.timelimiter.utils.MetricNames.ERROR_CALLS;
import static io.github.resilience4j.timelimiter.utils.MetricNames.SUCCESSFUL_CALLS;
import static io.github.resilience4j.timelimiter.utils.MetricNames.TIMED_OUT_CALLS;
import static java.util.Objects.requireNonNull;

/**
 * An adapter which exports {@link TimeLimiter.Metrics} as Dropwizard Metrics Gauges.
 */
public class TimeLimiterMetrics implements MetricSet {

    private static final String PREFIX_NULL = "Prefix must not be null";
    private static final String ITERABLE_NULL = "TimeLimiters iterable must not be null";

    private final MetricRegistry metricRegistry;

    private TimeLimiterMetrics(Iterable<TimeLimiter> timeLimiters) {
        this(DEFAULT_PREFIX, timeLimiters, new MetricRegistry());
    }

    private TimeLimiterMetrics(String prefix, Iterable<TimeLimiter> timeLimiters, MetricRegistry metricRegistry) {
        requireNonNull(prefix, PREFIX_NULL);
        requireNonNull(timeLimiters, ITERABLE_NULL);
        requireNonNull(metricRegistry);
        this.metricRegistry = metricRegistry;
        timeLimiters.forEach(timeLimiter -> {
                    String name = timeLimiter.getName();
                    metricRegistry.register(name(prefix, name, SUCCESSFUL_CALLS),
                            (Gauge<Long>) timeLimiter.getMetrics()::getNumberOfSuccessfulCalls);
                    metricRegistry.register(name(prefix, name, ERROR_CALLS),
                            (Gauge<Long>) timeLimiter.getMetrics()::getNumberOfErrorCalls);
                    metricRegistry.register(name(prefix, name, TIMED_OUT_CALLS),
                            (Gauge<Long>) timeLimiter.getMetrics()::getNumberOfTimedOutCalls);
                }
        );
    }

    /**
     * Creates a new instance {@link TimeLimiterMetrics} with specified metrics names prefix and
     * a {@link TimeLimiterRegistry} as a source.
     *
     * @param prefix              the prefix of metrics names
     * @param timeLimiterRegistry the registry of time limiters
     * @param metricRegistry      the metric registry
     */
    public static TimeLimiterMetrics ofTimeLimiterRegistry(String prefix, TimeLimiterRegistry timeLimiterRegistry, MetricRegistry metricRegistry) {
        return new TimeLimiterMetrics(prefix, timeLimiterRegistry.getAllTimeLimiters(), metricRegistry);
    }

    /**
     * Creates a new instance {@link TimeLimiterMetrics} with specified metrics names prefix and
     * a {@link TimeLimiterRegistry} as a source.
     *
     * @param prefix              the prefix of metrics names
     * @param timeLimiterRegistry the registry of time limiters
     */
    public static TimeLimiterMetrics ofTimeLimiterRegistry(String prefix, TimeLimiterRegistry timeLimiterRegistry) {
        return new TimeLimiterMetrics(prefix, timeLimiterRegistry.getAllTimeLimiters(), new MetricRegistry());
    }

    /**
     * Creates a new instance {@link TimeLimiterMetrics} with
     * a {@link TimeLimiterRegistry} as a source.
     *
     * @param timeLimiterRegistry the registry of time limiters
     * @param metricRegistry      the metric registry
     */
    public static TimeLimiterMetrics ofTimeLimiterRegistry(TimeLimiterRegistry timeLimiterRegistry, MetricRegistry metricRegistry) {
        return new TimeLimiterMetrics(DEFAULT_PREFIX, timeLimiterRegistry.getAllTimeLimiters(), metricRegistry);
    }

    /**
     * Creates a new instance {@link TimeLimiterMetrics} with
     * a {@link TimeLimiterRegistry} as a source.
     *
     * @param timeLimiterRegistry the registry of time limiters
     */
    public static TimeLimiterMetrics ofTimeLimiterRegistry(TimeLimiterRegistry timeLimiterRegistry) {
        return new TimeLimiterMetrics(timeLimiterRegistry.getAllTimeLimiters());
    }

    /**
     * Creates a new instance {@link TimeLimiterMetrics} with
     * an {@link Iterable} of time limiters as a source.
     *
     * @param timeLimiters the time limiters
     */
    public static TimeLimiterMetrics ofIterable(Iterable<TimeLimiter> timeLimiters) {
        return new TimeLimiterMetrics(timeLimiters);
    }

    /**
     * Creates a new instance {@link TimeLimiterMetrics} with
     * an {@link Iterable} of time limiters as a source.
     *
     * @param timeLimiters the time limiters
     */
    public static TimeLimiterMetrics ofIterable(String prefix, Iterable<TimeLimiter> timeLimiters) {
        return new TimeLimiterMetrics(prefix, timeLimiters, new MetricRegistry());
    }


    /**
     * Creates a new instance of {@link TimeLimiterMetrics} with a time limiter as a source.
     *
     * @param timeLimiter the time limiter
     */
    public static TimeLimiterMetrics ofTimeLimiter(TimeLimiter timeLimiter) {
        return new TimeLimiterMetrics(Array.of(timeLimiter));
    }

    @Override
    public Map<String, Metric> getMetrics() {
        return metricRegistry.getMetrics();
    }
}
