package io.github.resilience4j.springboot3.circuitbreaker.autoconfigure;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.cloud.context.scope.refresh.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerEvent;
import io.github.resilience4j.common.CompositeCustomizer;
import io.github.resilience4j.common.circuitbreaker.configuration.CircuitBreakerConfigCustomizer;
import io.github.resilience4j.consumer.EventConsumerRegistry;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.github.resilience4j.spring6.circuitbreaker.configure.CircuitBreakerConfiguration;
import io.github.resilience4j.spring6.circuitbreaker.configure.CircuitBreakerConfigurationProperties;

@Configuration
@ConditionalOnClass({CircuitBreaker.class, RefreshScope.class})
@AutoConfigureAfter(RefreshAutoConfiguration.class)
@AutoConfigureBefore(CircuitBreakerAutoConfiguration.class)
public class RefreshScopedCircuitBreakerAutoConfiguration {


    private final CircuitBreakerConfiguration circuitBreakerConfiguration;
    private final CircuitBreakerConfigurationProperties circuitBreakerProperties;

    public RefreshScopedCircuitBreakerAutoConfiguration(
        CircuitBreakerConfigurationProperties circuitBreakerProperties) {
        this.circuitBreakerProperties = circuitBreakerProperties;
        this.circuitBreakerConfiguration = new CircuitBreakerConfiguration(
            circuitBreakerProperties);
    }

    /**
     * @param eventConsumerRegistry the circuit breaker event consumer registry
     * @return the RefreshScoped CircuitBreakerRegistry
     */
    @Bean
    @org.springframework.cloud.context.config.annotation.RefreshScope
    @ConditionalOnMissingBean
    public CircuitBreakerRegistry circuitBreakerRegistry(
        EventConsumerRegistry<CircuitBreakerEvent> eventConsumerRegistry,
        RegistryEventConsumer<CircuitBreaker> circuitBreakerRegistryEventConsumer,
        @Qualifier("compositeCircuitBreakerCustomizer") CompositeCustomizer<CircuitBreakerConfigCustomizer> compositeCircuitBreakerCustomizer) {
        return circuitBreakerConfiguration
            .circuitBreakerRegistry(eventConsumerRegistry, circuitBreakerRegistryEventConsumer,
                compositeCircuitBreakerCustomizer);
    }

}
