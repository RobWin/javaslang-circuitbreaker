package io.github.resilience4j.ratelimiter;

import io.github.resilience4j.common.CompositeCustomizer;
import io.github.resilience4j.common.ratelimiter.configuration.RateLimiterConfigCustomizer;
import io.github.resilience4j.common.ratelimiter.configuration.RateLimiterConfigurationProperties;
import io.github.resilience4j.consumer.DefaultEventConsumerRegistry;
import io.github.resilience4j.consumer.EventConsumerRegistry;
import io.github.resilience4j.core.registry.CompositeRegistryEventConsumer;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.github.resilience4j.ratelimiter.event.RateLimiterEvent;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Primary;

import javax.annotation.Nullable;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Factory
public class RateLimiterRegistryFactory {

    @Bean
    @Named("compositeRateLimiterCustomizer")
    public CompositeCustomizer<RateLimiterConfigCustomizer> compositeRateLimiterCustomizer(
        @Nullable List<RateLimiterConfigCustomizer> configCustomizers) {
        return new CompositeCustomizer<>(configCustomizers);
    }


    @Factory
    public RateLimiterRegistry rateLimiterRegistry(RateLimiterProperties rateLimiterProperties,
                                                   EventConsumerRegistry<RateLimiterEvent> rateLimiterEventsConsumerRegistry,
                                                   RegistryEventConsumer<RateLimiter> rateLimiterRegistryEventConsumer,
                                                   @Named("compositeRateLimiterCustomizer") CompositeCustomizer<RateLimiterConfigCustomizer> compositeRateLimiterCustomizer) {
        RateLimiterRegistry rateLimiterRegistry = createRateLimiterRegistry(rateLimiterProperties, rateLimiterRegistryEventConsumer, compositeRateLimiterCustomizer);
        registerEventConsumer(rateLimiterRegistry, rateLimiterEventsConsumerRegistry,
            rateLimiterProperties);
        rateLimiterProperties.getInstances().forEach(
            (name, properites) ->
                rateLimiterRegistry
                    .rateLimiter(name, rateLimiterProperties
                        .createRateLimiterConfig(properites, compositeRateLimiterCustomizer, name))
        );
        return rateLimiterRegistry;
    }

    @Bean
    public EventConsumerRegistry<RateLimiterEvent> rateLimiterEventEventConsumerRegistry() {
        return new DefaultEventConsumerRegistry<>();
    }

    @Bean
    @Primary
    public RegistryEventConsumer<RateLimiter> rateLimiterRegistryEventConsumer(
        Optional<List<RegistryEventConsumer<RateLimiter>>> optionalRegistryEventConsumers
    ) {
        return new CompositeRegistryEventConsumer<>(
            optionalRegistryEventConsumers.orElseGet(ArrayList::new)
        );
    }

    /**
     * Registers the post creation consumer function that registers the consumer events to the rate
     * limiters.
     *
     * @param rateLimiterRegistry   The rate limiter registry.
     * @param eventConsumerRegistry The event consumer registry.
     */
    private void registerEventConsumer(RateLimiterRegistry rateLimiterRegistry,
                                       EventConsumerRegistry<RateLimiterEvent> eventConsumerRegistry,
                                       RateLimiterConfigurationProperties rateLimiterConfigurationProperties) {
        rateLimiterRegistry.getEventPublisher().onEntryAdded(
            event -> registerEventConsumer(eventConsumerRegistry, event.getAddedEntry(),
                rateLimiterConfigurationProperties));
    }

    private void registerEventConsumer(
        EventConsumerRegistry<RateLimiterEvent> eventConsumerRegistry, RateLimiter rateLimiter,
        RateLimiterConfigurationProperties rateLimiterConfigurationProperties) {
        final io.github.resilience4j.common.ratelimiter.configuration.RateLimiterConfigurationProperties.InstanceProperties limiterProperties = rateLimiterConfigurationProperties
            .getInstances().get(rateLimiter.getName());
        if (limiterProperties != null && limiterProperties.getSubscribeForEvents() != null
            && limiterProperties.getSubscribeForEvents()) {
            rateLimiter.getEventPublisher().onEvent(eventConsumerRegistry
                .createEventConsumer(rateLimiter.getName(),
                    limiterProperties.getEventConsumerBufferSize() != null
                        && limiterProperties.getEventConsumerBufferSize() != 0 ? limiterProperties
                        .getEventConsumerBufferSize() : 100));
        }
    }


    /**
     * Initializes a rate limiter registry.
     *
     * @param rateLimiterConfigurationProperties The rate limiter configuration properties.
     * @param compositeRateLimiterCustomizer     the composite rate limiter customizer delegate
     * @return a RateLimiterRegistry
     */
    private RateLimiterRegistry createRateLimiterRegistry(
        RateLimiterConfigurationProperties rateLimiterConfigurationProperties,
        RegistryEventConsumer<RateLimiter> rateLimiterRegistryEventConsumer,
        CompositeCustomizer<RateLimiterConfigCustomizer> compositeRateLimiterCustomizer) {
        Map<String, RateLimiterConfig> configs = rateLimiterConfigurationProperties.getConfigs()
            .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                entry -> rateLimiterConfigurationProperties
                    .createRateLimiterConfig(entry.getValue(), compositeRateLimiterCustomizer,
                        entry.getKey())));

        return RateLimiterRegistry.of(configs, rateLimiterRegistryEventConsumer,
            io.vavr.collection.HashMap.ofAll(rateLimiterConfigurationProperties.getTags()));
    }

}
