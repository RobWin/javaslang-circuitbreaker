package io.github.resilience4j.springboot3.ratelimiter.autoconfigure;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.cloud.context.scope.refresh.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.github.resilience4j.common.CompositeCustomizer;
import io.github.resilience4j.common.ratelimiter.configuration.RateLimiterConfigCustomizer;
import io.github.resilience4j.consumer.EventConsumerRegistry;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.ratelimiter.event.RateLimiterEvent;
import io.github.resilience4j.spring6.ratelimiter.configure.RateLimiterConfiguration;
import io.github.resilience4j.spring6.ratelimiter.configure.RateLimiterConfigurationProperties;

@Configuration
@ConditionalOnClass({RateLimiter.class, RefreshScope.class})
@AutoConfigureAfter(RefreshAutoConfiguration.class)
@AutoConfigureBefore(RateLimiterAutoConfiguration.class)
public class RefreshScopedRateLimiterAutoConfiguration {
    private final RateLimiterConfiguration rateLimiterConfiguration;

    public RefreshScopedRateLimiterAutoConfiguration() {
        this.rateLimiterConfiguration = new RateLimiterConfiguration();
    }

    /**
     * @param rateLimiterProperties             ratelimiter spring configuration properties
     * @param rateLimiterEventsConsumerRegistry the ratelimiter event consumer registry
     * @return the RefreshScoped RateLimiterRegistry
     */
    @Bean
    @org.springframework.cloud.context.config.annotation.RefreshScope
    @ConditionalOnMissingBean
    public RateLimiterRegistry rateLimiterRegistry(
        RateLimiterConfigurationProperties rateLimiterProperties,
        EventConsumerRegistry<RateLimiterEvent> rateLimiterEventsConsumerRegistry,
        RegistryEventConsumer<RateLimiter> rateLimiterRegistryEventConsumer,
        @Qualifier("compositeRateLimiterCustomizer") CompositeCustomizer<RateLimiterConfigCustomizer> compositeRateLimiterCustomizer) {
        return rateLimiterConfiguration.rateLimiterRegistry(
            rateLimiterProperties, rateLimiterEventsConsumerRegistry,
            rateLimiterRegistryEventConsumer, compositeRateLimiterCustomizer);
    }
}
