package io.github.resilience4j.springboot3.springboot3.timelimiter.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;

import io.github.resilience4j.common.CompositeCustomizer;
import io.github.resilience4j.consumer.DefaultEventConsumerRegistry;
import io.github.resilience4j.core.registry.CompositeRegistryEventConsumer;
import io.github.resilience4j.spring6.timelimiter.configure.TimeLimiterConfigurationProperties;
import io.github.resilience4j.springboot3.timelimiter.autoconfigure.RefreshScopedTimeLimiterAutoConfiguration;

public class RefreshScopedTimeLimiterConfigurationTest {

    @Test
    public void testRefreshScopedTimeLimiterConfig() {
        Arrays.stream(RefreshScopedTimeLimiterAutoConfiguration.class.getMethods())
            .filter(method -> method.isAnnotationPresent(Bean.class))
            .forEach(method -> assertThat(method.isAnnotationPresent(RefreshScope.class)).isTrue());
    }

    @Test
    public void testTimeLimiterCloudCommonConfig() {
        TimeLimiterConfig timeLimiterConfig = new TimeLimiterConfig();

        assertThat(timeLimiterConfig.timeLimiterRegistry(
            new TimeLimiterConfigurationProperties(), new DefaultEventConsumerRegistry<>(),
            new CompositeRegistryEventConsumer<>(Collections.emptyList()),
            new CompositeCustomizer<>(Collections.emptyList()))).isNotNull();
    }


    static class TimeLimiterConfig extends RefreshScopedTimeLimiterAutoConfiguration {

    }
}