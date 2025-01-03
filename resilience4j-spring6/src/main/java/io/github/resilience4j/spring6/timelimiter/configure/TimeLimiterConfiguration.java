/*
 * Copyright 2020 Ingyu Hwang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.resilience4j.spring6.timelimiter.configure;

import io.github.resilience4j.common.CompositeCustomizer;
import io.github.resilience4j.common.timelimiter.configuration.TimeLimiterConfigCustomizer;
import io.github.resilience4j.common.timelimiter.configuration.CommonTimeLimiterConfigurationProperties.InstanceProperties;
import io.github.resilience4j.consumer.DefaultEventConsumerRegistry;
import io.github.resilience4j.consumer.EventConsumerRegistry;
import io.github.resilience4j.core.ContextAwareScheduledThreadPoolExecutor;
import io.github.resilience4j.core.registry.CompositeRegistryEventConsumer;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.github.resilience4j.spring6.fallback.FallbackExecutor;
import io.github.resilience4j.spring6.fallback.configure.FallbackConfiguration;
import io.github.resilience4j.spring6.spelresolver.SpelResolver;
import io.github.resilience4j.spring6.spelresolver.configure.SpelResolverConfiguration;
import io.github.resilience4j.spring6.utils.RxJava3OnClasspathCondition;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import io.github.resilience4j.timelimiter.event.TimeLimiterEvent;
import io.github.resilience4j.spring6.utils.AspectJOnClasspathCondition;
import io.github.resilience4j.spring6.utils.ReactorOnClasspathCondition;
import io.github.resilience4j.spring6.utils.RxJava2OnClasspathCondition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link Configuration} for resilience4j-timelimiter.
 */
@Configuration
@Import({FallbackConfiguration.class, SpelResolverConfiguration.class})
public class TimeLimiterConfiguration {

    @Bean
    public TimeLimiterConfigurationProperties timeLimiterConfigurationProperties() {
        return new TimeLimiterConfigurationProperties();
    }

    @Bean
    @Qualifier("compositeTimeLimiterCustomizer")
    public CompositeCustomizer<TimeLimiterConfigCustomizer> compositeTimeLimiterCustomizer(
        @Autowired(required = false) List<TimeLimiterConfigCustomizer> customizers) {
        return new CompositeCustomizer<>(customizers);
    }

    @Bean
    public TimeLimiterRegistry timeLimiterRegistry(
        TimeLimiterConfigurationProperties timeLimiterConfigurationProperties,
        EventConsumerRegistry<TimeLimiterEvent> timeLimiterEventConsumerRegistry,
        RegistryEventConsumer<TimeLimiter> timeLimiterRegistryEventConsumer,
        @Qualifier("compositeTimeLimiterCustomizer") CompositeCustomizer<TimeLimiterConfigCustomizer> compositeTimeLimiterCustomizer) {
        TimeLimiterRegistry timeLimiterRegistry =
                createTimeLimiterRegistry(timeLimiterConfigurationProperties, timeLimiterRegistryEventConsumer,
                    compositeTimeLimiterCustomizer);
        registerEventConsumer(timeLimiterRegistry, timeLimiterEventConsumerRegistry, timeLimiterConfigurationProperties);

        initTimeLimiterRegistry(timeLimiterRegistry, timeLimiterConfigurationProperties, compositeTimeLimiterCustomizer);
        return timeLimiterRegistry;
    }

    @Bean
    @Primary
    public RegistryEventConsumer<TimeLimiter> timeLimiterRegistryEventConsumer(
            Optional<List<RegistryEventConsumer<TimeLimiter>>> optionalRegistryEventConsumers) {
        return new CompositeRegistryEventConsumer<>(optionalRegistryEventConsumers.orElseGet(ArrayList::new));
    }

    @Bean
    @Conditional(AspectJOnClasspathCondition.class)
    public TimeLimiterAspect timeLimiterAspect(
        TimeLimiterConfigurationProperties timeLimiterConfigurationProperties,
        TimeLimiterRegistry timeLimiterRegistry,
        @Autowired(required = false) List<TimeLimiterAspectExt> timeLimiterAspectExtList,
        FallbackExecutor fallbackExecutor,
        SpelResolver spelResolver,
        @Autowired(required = false) ContextAwareScheduledThreadPoolExecutor contextAwareScheduledThreadPoolExecutor
    ) {
        return new TimeLimiterAspect(timeLimiterRegistry, timeLimiterConfigurationProperties, timeLimiterAspectExtList, fallbackExecutor, spelResolver, contextAwareScheduledThreadPoolExecutor);
    }

    @Bean
    @Conditional({RxJava2OnClasspathCondition.class, AspectJOnClasspathCondition.class})
    public RxJava2TimeLimiterAspectExt rxJava2TimeLimiterAspectExt() {
        return new RxJava2TimeLimiterAspectExt();
    }

    @Bean
    @Conditional({RxJava3OnClasspathCondition.class, AspectJOnClasspathCondition.class})
    public RxJava3TimeLimiterAspectExt rxJava3TimeLimiterAspectExt() {
        return new RxJava3TimeLimiterAspectExt();
    }

    @Bean
    @Conditional({ReactorOnClasspathCondition.class, AspectJOnClasspathCondition.class})
    public ReactorTimeLimiterAspectExt reactorTimeLimiterAspectExt() {
        return new ReactorTimeLimiterAspectExt();
    }

    /**
     * The EventConsumerRegistry is used to manage EventConsumer instances.
     * The EventConsumerRegistry is used by the TimeLimiter events monitor to show the latest TimeLimiter events
     * for each TimeLimiter instance.
     *
     * @return a default EventConsumerRegistry {@link DefaultEventConsumerRegistry}
     */
    @Bean
    public EventConsumerRegistry<TimeLimiterEvent> timeLimiterEventsConsumerRegistry() {
        return new DefaultEventConsumerRegistry<>();
    }
    /**
     * Initializes a timeLimiter registry.
     *
     * @param timeLimiterConfigurationProperties The timeLimiter configuration properties.
     * @return a timeLimiterRegistry
     */
    private static TimeLimiterRegistry createTimeLimiterRegistry(
        TimeLimiterConfigurationProperties timeLimiterConfigurationProperties,
        RegistryEventConsumer<TimeLimiter> timeLimiterRegistryEventConsumer,
        CompositeCustomizer<TimeLimiterConfigCustomizer> compositeTimeLimiterCustomizer) {

        Map<String, TimeLimiterConfig> configs = timeLimiterConfigurationProperties.getConfigs()
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> timeLimiterConfigurationProperties.createTimeLimiterConfig(
                            entry.getKey(), entry.getValue(), compositeTimeLimiterCustomizer)));

        return TimeLimiterRegistry.of(configs, timeLimiterRegistryEventConsumer, Map.copyOf(timeLimiterConfigurationProperties.getTags()));
    }

    /**
     * Initializes the TimeLimiter registry.
     *
     * @param timeLimiterRegistry The time limiter registry.
     * @param compositeTimeLimiterCustomizer The Composite time limiter customizer
     */
    void initTimeLimiterRegistry(
        TimeLimiterRegistry timeLimiterRegistry,
        TimeLimiterConfigurationProperties timeLimiterConfigurationProperties,
        CompositeCustomizer<TimeLimiterConfigCustomizer> compositeTimeLimiterCustomizer) {

        timeLimiterConfigurationProperties.getInstances().forEach(
            (name, properties) -> timeLimiterRegistry.timeLimiter(name,
                timeLimiterConfigurationProperties
                    .createTimeLimiterConfig(name, properties, compositeTimeLimiterCustomizer))
        );
    }
    /**
     * Registers the post creation consumer function that registers the consumer events to the timeLimiters.
     *
     * @param timeLimiterRegistry   The timeLimiter registry.
     * @param eventConsumerRegistry The event consumer registry.
     * @param properties timeLimiter configuration properties
     */
    private static void registerEventConsumer(TimeLimiterRegistry timeLimiterRegistry,
                                              EventConsumerRegistry<TimeLimiterEvent> eventConsumerRegistry,
                                              TimeLimiterConfigurationProperties properties) {
        timeLimiterRegistry.getEventPublisher()
            .onEntryAdded(event -> registerEventConsumer(eventConsumerRegistry, event.getAddedEntry(), properties))
            .onEntryReplaced(event -> registerEventConsumer(eventConsumerRegistry, event.getNewEntry(), properties))
            .onEntryRemoved(event -> unregisterEventConsumer(eventConsumerRegistry, event.getRemovedEntry()));
    }

    private static void unregisterEventConsumer(EventConsumerRegistry<TimeLimiterEvent> eventConsumerRegistry, TimeLimiter timeLimiter) {
        eventConsumerRegistry.removeEventConsumer(timeLimiter.getName());
    }

    private static void registerEventConsumer(EventConsumerRegistry<TimeLimiterEvent> eventConsumerRegistry, TimeLimiter timeLimiter,
                                              TimeLimiterConfigurationProperties timeLimiterConfigurationProperties) {
        int eventConsumerBufferSize = Optional.ofNullable(timeLimiterConfigurationProperties.getInstanceProperties(timeLimiter.getName()))
                .map(InstanceProperties::getEventConsumerBufferSize)
                .orElse(100);
        timeLimiter.getEventPublisher().onEvent(
            eventConsumerRegistry.createEventConsumer(timeLimiter.getName(), eventConsumerBufferSize));
    }

}
