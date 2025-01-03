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

package io.github.resilience4j.timelimiter.autoconfigure;

import io.github.resilience4j.common.CompositeCustomizer;
import io.github.resilience4j.common.timelimiter.configuration.TimeLimiterConfigCustomizer;
import io.github.resilience4j.consumer.EventConsumerRegistry;
import io.github.resilience4j.core.ContextAwareScheduledThreadPoolExecutor;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.github.resilience4j.fallback.FallbackExecutor;
import io.github.resilience4j.fallback.autoconfigure.FallbackConfigurationOnMissingBean;
import io.github.resilience4j.spelresolver.SpelResolver;
import io.github.resilience4j.spelresolver.autoconfigure.SpelResolverConfigurationOnMissingBean;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import io.github.resilience4j.timelimiter.configure.*;
import io.github.resilience4j.timelimiter.event.TimeLimiterEvent;
import io.github.resilience4j.utils.AspectJOnClasspathCondition;
import io.github.resilience4j.utils.ReactorOnClasspathCondition;
import io.github.resilience4j.utils.RxJava2OnClasspathCondition;
import io.github.resilience4j.utils.RxJava3OnClasspathCondition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.*;

import java.util.List;
import java.util.Optional;

@Configuration
@Import({FallbackConfigurationOnMissingBean.class, SpelResolverConfigurationOnMissingBean.class})
public abstract class AbstractTimeLimiterConfigurationOnMissingBean {
    protected final TimeLimiterConfiguration timeLimiterConfiguration;

    protected AbstractTimeLimiterConfigurationOnMissingBean() {
        this.timeLimiterConfiguration = new TimeLimiterConfiguration();
    }

    @Bean
    @ConditionalOnMissingBean(name = "compositeTimeLimiterCustomizer")
    @Qualifier("compositeTimeLimiterCustomizer")
    public CompositeCustomizer<TimeLimiterConfigCustomizer> compositeTimeLimiterCustomizer(
        @Autowired(required = false) List<TimeLimiterConfigCustomizer> customizers) {
        return new CompositeCustomizer<>(customizers);
    }

    @Bean
    @ConditionalOnMissingBean
    public TimeLimiterProperties timeLimiterProperties() {
        return new TimeLimiterProperties();
    }

    @Bean
    @ConditionalOnMissingBean
    public TimeLimiterRegistry timeLimiterRegistry(
        TimeLimiterConfigurationProperties timeLimiterProperties,
        EventConsumerRegistry<TimeLimiterEvent> timeLimiterEventsConsumerRegistry,
        RegistryEventConsumer<TimeLimiter> timeLimiterRegistryEventConsumer,
        @Qualifier("compositeTimeLimiterCustomizer") CompositeCustomizer<TimeLimiterConfigCustomizer> compositeTimeLimiterCustomizer) {
        return timeLimiterConfiguration.timeLimiterRegistry(
            timeLimiterProperties, timeLimiterEventsConsumerRegistry,
            timeLimiterRegistryEventConsumer, compositeTimeLimiterCustomizer);
    }

    @Bean
    @Primary
    public RegistryEventConsumer<TimeLimiter> timeLimiterRegistryEventConsumer(
        Optional<List<RegistryEventConsumer<TimeLimiter>>> optionalRegistryEventConsumers) {
        return timeLimiterConfiguration.timeLimiterRegistryEventConsumer(optionalRegistryEventConsumers);
    }

    @Bean
    @Conditional(AspectJOnClasspathCondition.class)
    @ConditionalOnMissingBean
    public TimeLimiterAspect timeLimiterAspect(
        TimeLimiterConfigurationProperties timeLimiterProperties,
        TimeLimiterRegistry timeLimiterRegistry,
        @Autowired(required = false) List<TimeLimiterAspectExt> timeLimiterAspectExtList,
        FallbackExecutor fallbackExecutor,
        SpelResolver spelResolver,
        @Autowired(required = false) ContextAwareScheduledThreadPoolExecutor contextAwareScheduledThreadPoolExecutor
    ) {
        return timeLimiterConfiguration.timeLimiterAspect(
            timeLimiterProperties, timeLimiterRegistry, timeLimiterAspectExtList, fallbackExecutor, spelResolver, contextAwareScheduledThreadPoolExecutor);
    }

    @Bean
    @Conditional({RxJava2OnClasspathCondition.class, AspectJOnClasspathCondition.class})
    @ConditionalOnMissingBean
    public RxJava2TimeLimiterAspectExt rxJava2TimeLimiterAspectExt() {
        return timeLimiterConfiguration.rxJava2TimeLimiterAspectExt();
    }

    @Bean
    @Conditional({RxJava3OnClasspathCondition.class, AspectJOnClasspathCondition.class})
    @ConditionalOnMissingBean
    public RxJava3TimeLimiterAspectExt rxJava3TimeLimiterAspectExt() {
        return timeLimiterConfiguration.rxJava3TimeLimiterAspectExt();
    }

    @Bean
    @Conditional({ReactorOnClasspathCondition.class, AspectJOnClasspathCondition.class})
    @ConditionalOnMissingBean
    public ReactorTimeLimiterAspectExt reactorTimeLimiterAspectExt() {
        return timeLimiterConfiguration.reactorTimeLimiterAspectExt();
    }

}
