/*
 * Copyright 2019 lespinsideg
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
package io.github.resilience4j;

import io.github.resilience4j.bulkhead.configure.BulkheadConfigurationProperties;
import io.github.resilience4j.circuitbreaker.configure.CircuitBreakerConfigurationProperties;
import io.github.resilience4j.common.bulkhead.configuration.CommonThreadPoolBulkheadConfigurationProperties;
import io.github.resilience4j.ratelimiter.configure.RateLimiterConfigurationProperties;
import io.github.resilience4j.retry.configure.RetryConfigurationProperties;
import io.github.resilience4j.timelimiter.configure.TimeLimiterConfigurationProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
@Configuration
public class TestApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }

    @Bean
    public BulkheadConfigurationProperties bulkheadConfigurationProperties() {
        return new BulkheadConfigurationProperties();
    }

    @Bean
    public CommonThreadPoolBulkheadConfigurationProperties threadPoolBulkheadConfigurationProperties() {
        return new CommonThreadPoolBulkheadConfigurationProperties();
    }

    @Bean
    public CircuitBreakerConfigurationProperties circuitBreakerConfigurationProperties() {
        return new CircuitBreakerConfigurationProperties();
    }

    @Bean
    public RateLimiterConfigurationProperties rateLimiterConfigurationProperties() {
        return new RateLimiterConfigurationProperties();
    }

    @Bean
    public RetryConfigurationProperties retryConfigurationProperties() {
        return new RetryConfigurationProperties();
    }
}
