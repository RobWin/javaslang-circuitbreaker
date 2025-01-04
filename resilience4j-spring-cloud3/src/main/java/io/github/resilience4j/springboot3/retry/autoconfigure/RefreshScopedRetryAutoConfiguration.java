package io.github.resilience4j.springboot3.retry.autoconfigure;

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
import io.github.resilience4j.common.retry.configuration.RetryConfigCustomizer;
import io.github.resilience4j.consumer.EventConsumerRegistry;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.retry.event.RetryEvent;
import io.github.resilience4j.spring6.retry.configure.RetryConfiguration;
import io.github.resilience4j.spring6.retry.configure.RetryConfigurationProperties;

@Configuration
@ConditionalOnClass({Retry.class, RefreshScope.class})
@AutoConfigureAfter(RefreshAutoConfiguration.class)
@AutoConfigureBefore(RetryAutoConfiguration.class)
public class RefreshScopedRetryAutoConfiguration {

    private final RetryConfiguration retryConfiguration;

    protected RefreshScopedRetryAutoConfiguration() {
        this.retryConfiguration = new RetryConfiguration();
    }

    /**
     * @param retryConfigurationProperties retry spring configuration properties
     * @param retryEventConsumerRegistry   the retry event consumer registry
     * @return the RefreshScoped RetryRegistry
     */
    @Bean
    @org.springframework.cloud.context.config.annotation.RefreshScope
    @ConditionalOnMissingBean
    public RetryRegistry retryRegistry(RetryConfigurationProperties retryConfigurationProperties,
                                       EventConsumerRegistry<RetryEvent> retryEventConsumerRegistry,
                                       RegistryEventConsumer<Retry> retryRegistryEventConsumer,
                                       @Qualifier("compositeRetryCustomizer") CompositeCustomizer<RetryConfigCustomizer> compositeRetryCustomizer) {
        return retryConfiguration
            .retryRegistry(retryConfigurationProperties, retryEventConsumerRegistry,
                retryRegistryEventConsumer, compositeRetryCustomizer);
    }

}
