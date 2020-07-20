/*
 * Copyright 2019 Andrew From
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

package io.github.resilience4j.ratpack.circuitbreaker.monitoring.endpoint.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerEvent;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class CircuitBreakerStreamEventsDTO {

    private CircuitBreakerEvent circuitBreakerRecentEvent;
    private CircuitBreaker.Metrics metrics;
    private CircuitBreaker.State currentState;

    public CircuitBreakerStreamEventsDTO(CircuitBreakerEvent circuitBreakerEvent,
                                         CircuitBreaker.State state,
                                         CircuitBreaker.Metrics metrics) {
        this.circuitBreakerRecentEvent = circuitBreakerEvent;
        this.metrics = metrics;
        this.currentState = state;
    }

    public CircuitBreakerEvent getCircuitBreakerRecentEvent() {
        return circuitBreakerRecentEvent;
    }

    public void setCircuitBreakerRecentEvent(CircuitBreakerEvent circuitBreakerRecentEvent) {
        this.circuitBreakerRecentEvent = circuitBreakerRecentEvent;
    }

    public CircuitBreaker.Metrics getMetrics() {
        return metrics;
    }

    public void setMetrics(CircuitBreaker.Metrics metrics) {
        this.metrics = metrics;
    }

    public CircuitBreaker.State getCurrentState() {
        return currentState;
    }

    public void setCurrentState(CircuitBreaker.State currentState) {
        this.currentState = currentState;
    }
}
