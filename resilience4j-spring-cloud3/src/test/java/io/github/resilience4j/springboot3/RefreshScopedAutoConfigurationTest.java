package io.github.resilience4j.springboot3;

import io.github.resilience4j.springboot3.bulkhead.autoconfigure.BulkheadAutoConfiguration;
import io.github.resilience4j.springboot3.bulkhead.autoconfigure.RefreshScopedBulkheadAutoConfiguration;
import io.github.resilience4j.springboot3.circuitbreaker.autoconfigure.CircuitBreakerAutoConfiguration;
import io.github.resilience4j.springboot3.circuitbreaker.autoconfigure.RefreshScopedCircuitBreakerAutoConfiguration;
import io.github.resilience4j.springboot3.ratelimiter.autoconfigure.RateLimiterAutoConfiguration;
import io.github.resilience4j.springboot3.ratelimiter.autoconfigure.RefreshScopedRateLimiterAutoConfiguration;
import io.github.resilience4j.springboot3.retry.autoconfigure.RefreshScopedRetryAutoConfiguration;
import io.github.resilience4j.springboot3.retry.autoconfigure.RetryAutoConfiguration;
import io.github.resilience4j.springboot3.timelimiter.autoconfigure.RefreshScopedTimeLimiterAutoConfiguration;
import io.github.resilience4j.springboot3.timelimiter.autoconfigure.TimeLimiterAutoConfiguration;

import org.junit.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.core.type.MethodMetadata;

import static org.junit.Assert.assertTrue;

public class RefreshScopedAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner();

    private static void testRefreshScoped(AssertableApplicationContext context, String beanName) {
        BeanDefinition beanDefinition = context.getBeanFactory().getBeanDefinition(beanName);
        MethodMetadata beanMethod = (MethodMetadata) beanDefinition.getSource();

        assertTrue(beanMethod.isAnnotated(RefreshScope.class.getName()));
    }

    @Test
    public void refreshScopedBulkheadRegistry() {
        contextRunner
            .withConfiguration(AutoConfigurations.of(
                RefreshScopedBulkheadAutoConfiguration.class, BulkheadAutoConfiguration.class))
            .run(context -> {
                testRefreshScoped(context, "bulkheadRegistry");
                testRefreshScoped(context, "threadPoolBulkheadRegistry");
            });
    }

    @Test
    public void refreshScopedCircuitBreakerRegistry() {
        contextRunner
            .withConfiguration(AutoConfigurations.of(
                RefreshScopedCircuitBreakerAutoConfiguration.class,
                CircuitBreakerAutoConfiguration.class))
            .run(context -> testRefreshScoped(context, "circuitBreakerRegistry"));
    }

    @Test
    public void refreshScopedRateLimiterRegistry() {
        contextRunner
            .withConfiguration(AutoConfigurations.of(
                RefreshScopedRateLimiterAutoConfiguration.class,
                RateLimiterAutoConfiguration.class))
            .run(context -> testRefreshScoped(context, "rateLimiterRegistry"));
    }

    @Test
    public void refreshScopedRetryRegistry() {
        contextRunner
            .withConfiguration(AutoConfigurations.of(
                RefreshScopedRetryAutoConfiguration.class, RetryAutoConfiguration.class))
            .run(context -> testRefreshScoped(context, "retryRegistry"));
    }

    @Test
    public void refreshScopedTimeLimiterRegistry() {
        contextRunner
            .withConfiguration(AutoConfigurations.of(
                RefreshScopedTimeLimiterAutoConfiguration.class, TimeLimiterAutoConfiguration.class))
            .run(context -> testRefreshScoped(context, "timeLimiterRegistry"));
    }
}
