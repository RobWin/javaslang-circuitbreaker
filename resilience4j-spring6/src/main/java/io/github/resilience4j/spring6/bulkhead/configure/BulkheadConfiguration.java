/*
 * Copyright 2019 lespinsideg, Mahmoud Romeh
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
package io.github.resilience4j.spring6.bulkhead.configure;

import io.github.resilience4j.core.ContextAwareScheduledThreadPoolExecutor;
import io.github.resilience4j.bulkhead.*;
import io.github.resilience4j.spring6.bulkhead.configure.threadpool.ThreadPoolBulkheadConfiguration;
import io.github.resilience4j.bulkhead.event.BulkheadEvent;
import io.github.resilience4j.common.CompositeCustomizer;
import io.github.resilience4j.common.bulkhead.configuration.BulkheadConfigCustomizer;
import io.github.resilience4j.common.bulkhead.configuration.CommonBulkheadConfigurationProperties.InstanceProperties;
import io.github.resilience4j.consumer.DefaultEventConsumerRegistry;
import io.github.resilience4j.consumer.EventConsumerRegistry;
import io.github.resilience4j.core.registry.CompositeRegistryEventConsumer;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.github.resilience4j.spring6.fallback.FallbackExecutor;
import io.github.resilience4j.spring6.fallback.configure.FallbackConfiguration;
import io.github.resilience4j.spring6.spelresolver.SpelResolver;
import io.github.resilience4j.spring6.spelresolver.configure.SpelResolverConfiguration;
import io.github.resilience4j.spring6.timelimiter.configure.TimeLimiterConfiguration;
import io.github.resilience4j.spring6.utils.AspectJOnClasspathCondition;
import io.github.resilience4j.spring6.utils.ReactorOnClasspathCondition;
import io.github.resilience4j.spring6.utils.RxJava2OnClasspathCondition;
import io.github.resilience4j.spring6.utils.RxJava3OnClasspathCondition;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link Configuration Configuration} for resilience4j-bulkhead.
 */
@Configuration
@Import({ThreadPoolBulkheadConfiguration.class, FallbackConfiguration.class, SpelResolverConfiguration.class, TimeLimiterConfiguration.class})
public class BulkheadConfiguration {

    @Bean
    @Qualifier("compositeBulkheadCustomizer")
    public CompositeCustomizer<BulkheadConfigCustomizer> compositeBulkheadCustomizer(
        @Autowired(required = false) List<BulkheadConfigCustomizer> customizers) {
        return new CompositeCustomizer<>(customizers);
    }

    /**
     * @param bulkheadConfigurationProperties bulk head spring configuration properties
     * @param bulkheadEventConsumerRegistry   the bulk head event consumer registry
     * @return the BulkheadRegistry with all needed setup in place
     */
    @Bean
    public BulkheadRegistry bulkheadRegistry(
        BulkheadConfigurationProperties bulkheadConfigurationProperties,
        EventConsumerRegistry<BulkheadEvent> bulkheadEventConsumerRegistry,
        RegistryEventConsumer<Bulkhead> bulkheadRegistryEventConsumer,
        @Qualifier("compositeBulkheadCustomizer") CompositeCustomizer<BulkheadConfigCustomizer> compositeBulkheadCustomizer) {
        BulkheadRegistry bulkheadRegistry = createBulkheadRegistry(bulkheadConfigurationProperties,
            bulkheadRegistryEventConsumer, compositeBulkheadCustomizer);
        registerEventConsumer(bulkheadRegistry, bulkheadEventConsumerRegistry,
            bulkheadConfigurationProperties);
        bulkheadConfigurationProperties.getInstances().forEach((name, properties) ->
            bulkheadRegistry
                .bulkhead(name, bulkheadConfigurationProperties
                    .createBulkheadConfig(properties, compositeBulkheadCustomizer,
                        name)));
        return bulkheadRegistry;
    }

    @Bean
    @Primary
    public RegistryEventConsumer<Bulkhead> bulkheadRegistryEventConsumer(
        Optional<List<RegistryEventConsumer<Bulkhead>>> optionalRegistryEventConsumers) {
        return new CompositeRegistryEventConsumer<>(
            optionalRegistryEventConsumers.orElseGet(ArrayList::new));
    }

    /**
     * Initializes a bulkhead registry.
     *
     * @param bulkheadConfigurationProperties The bulkhead configuration properties.
     * @param compositeBulkheadCustomizer
     * @return a BulkheadRegistry
     */
    private BulkheadRegistry createBulkheadRegistry(
        BulkheadConfigurationProperties bulkheadConfigurationProperties,
        RegistryEventConsumer<Bulkhead> bulkheadRegistryEventConsumer,
        CompositeCustomizer<BulkheadConfigCustomizer> compositeBulkheadCustomizer) {
        Map<String, BulkheadConfig> configs = bulkheadConfigurationProperties.getConfigs()
            .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                entry -> bulkheadConfigurationProperties.createBulkheadConfig(entry.getValue(),
                    compositeBulkheadCustomizer, entry.getKey())));
        return BulkheadRegistry.of(configs, bulkheadRegistryEventConsumer, Map.copyOf(bulkheadConfigurationProperties.getTags()));
    }

    /**
     * Registers the post creation consumer function that registers the consumer events to the
     * bulkheads.
     *
     * @param bulkheadRegistry      The BulkHead registry.
     * @param eventConsumerRegistry The event consumer registry.
     */
    private void registerEventConsumer(BulkheadRegistry bulkheadRegistry,
        EventConsumerRegistry<BulkheadEvent> eventConsumerRegistry,
        BulkheadConfigurationProperties properties) {
        bulkheadRegistry.getEventPublisher()
            .onEntryAdded(event -> registerEventConsumer(eventConsumerRegistry, event.getAddedEntry(), properties))
            .onEntryReplaced(event -> registerEventConsumer(eventConsumerRegistry, event.getNewEntry(), properties))
            .onEntryRemoved(event -> unregisterEventConsumer(eventConsumerRegistry, event.getRemovedEntry()));
    }

    private void unregisterEventConsumer(EventConsumerRegistry<BulkheadEvent> eventConsumerRegistry, Bulkhead bulkHead) {
        eventConsumerRegistry.removeEventConsumer(bulkHead.getName());
    }

    private void registerEventConsumer(EventConsumerRegistry<BulkheadEvent> eventConsumerRegistry,
        Bulkhead bulkHead, BulkheadConfigurationProperties bulkheadConfigurationProperties) {
        int eventConsumerBufferSize = Optional
            .ofNullable(bulkheadConfigurationProperties.getBackendProperties(bulkHead.getName()))
            .map(InstanceProperties::getEventConsumerBufferSize)
            .orElse(100);
        bulkHead.getEventPublisher().onEvent(
            eventConsumerRegistry.createEventConsumer(bulkHead.getName(), eventConsumerBufferSize));
    }

    @Bean
    @Conditional(value = {AspectJOnClasspathCondition.class})
    public BulkheadAspect bulkheadAspect(
        BulkheadConfigurationProperties bulkheadConfigurationProperties,
        ThreadPoolBulkheadRegistry threadPoolBulkheadRegistry,
        BulkheadRegistry bulkheadRegistry,
        @Autowired(required = false) List<BulkheadAspectExt> bulkHeadAspectExtList,
        FallbackExecutor fallbackExecutor,
        SpelResolver spelResolver
    ) {
        return new BulkheadAspect(bulkheadConfigurationProperties, threadPoolBulkheadRegistry,
            bulkheadRegistry, bulkHeadAspectExtList, fallbackExecutor, spelResolver);
    }

    @Bean
    @Conditional(value = {RxJava2OnClasspathCondition.class, AspectJOnClasspathCondition.class})
    public RxJava2BulkheadAspectExt rxJava2BulkHeadAspectExt() {
        return new RxJava2BulkheadAspectExt();
    }

    @Bean
    @Conditional(value = {RxJava3OnClasspathCondition.class, AspectJOnClasspathCondition.class})
    public RxJava3BulkheadAspectExt rxJava3BulkHeadAspectExt() {
        return new RxJava3BulkheadAspectExt();
    }

    @Bean
    @Conditional(value = {ReactorOnClasspathCondition.class, AspectJOnClasspathCondition.class})
    public ReactorBulkheadAspectExt reactorBulkHeadAspectExt() {
        return new ReactorBulkheadAspectExt();
    }

    @Bean
    @Conditional(value = AspectJOnClasspathCondition.class)
    public PlainObjectBulkheadAspectExt plainObjectBulkHeadAspectExt(
            ThreadPoolBulkheadRegistry threadPoolBulkheadRegistry,
            TimeLimiterRegistry timeLimiterRegistry,
            @Autowired(required = false) ContextAwareScheduledThreadPoolExecutor contextAwareScheduledThreadPoolExecutor) {
        return new PlainObjectBulkheadAspectExt(threadPoolBulkheadRegistry, timeLimiterRegistry, contextAwareScheduledThreadPoolExecutor);
    }

    /**
     * The EventConsumerRegistry is used to manage EventConsumer instances. The
     * EventConsumerRegistry is used by the BulkheadHealthIndicator to show the latest Bulkhead
     * events for each Bulkhead instance.
     *
     * @return a default EventConsumerRegistry {@link DefaultEventConsumerRegistry}
     */
    @Bean
    public EventConsumerRegistry<BulkheadEvent> bulkheadEventConsumerRegistry() {
        return new DefaultEventConsumerRegistry<>();
    }
}
