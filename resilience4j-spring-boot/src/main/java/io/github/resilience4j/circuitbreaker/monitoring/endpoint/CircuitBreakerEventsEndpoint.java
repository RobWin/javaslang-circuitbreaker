/*
 * Copyright 2017 Robert Winkler
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
package io.github.resilience4j.circuitbreaker.monitoring.endpoint;


import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerEvent;
import io.github.resilience4j.consumer.EventConsumer;
import io.github.resilience4j.consumer.EventConsumerRegistry;
import org.springframework.boot.actuate.endpoint.mvc.EndpointMvcAdapter;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Comparator;


@RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
public class CircuitBreakerEventsEndpoint extends EndpointMvcAdapter {

    private final EventConsumerRegistry<CircuitBreakerEvent> eventConsumerRegistry;
    private final CircuitBreakerRegistry circuitBreakerRegistry;


    public CircuitBreakerEventsEndpoint(CircuitBreakerEndpoint circuitBreakerEndpoint,
                                        EventConsumerRegistry<CircuitBreakerEvent> eventConsumerRegistry, CircuitBreakerRegistry circuitBreakerRegistry) {
        super(circuitBreakerEndpoint);
        this.eventConsumerRegistry = eventConsumerRegistry;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
    }

    @RequestMapping(value = "events", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public CircuitBreakerEventsEndpointResponse getAllCircuitBreakerEvents() {
        return new CircuitBreakerEventsEndpointResponse(eventConsumerRegistry.getAllEventConsumer()
                .flatMap(EventConsumer::getBufferedEvents)
                .sorted(Comparator.comparing(CircuitBreakerEvent::getCreationTime))
                .map(CircuitBreakerEventDTOFactory::createCircuitBreakerEventDTO).toJavaList());
    }

    @RequestMapping(value = "events/{circuitBreakerName}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public CircuitBreakerEventsEndpointResponse getEventsFilteredByCircuitBreakerName(@PathVariable("circuitBreakerName") String circuitBreakerName) {
        return new CircuitBreakerEventsEndpointResponse(eventConsumerRegistry.getEventConsumer(circuitBreakerName).getBufferedEvents()
                .filter(event -> event.getCircuitBreakerName().equals(circuitBreakerName))
                .map(CircuitBreakerEventDTOFactory::createCircuitBreakerEventDTO).toJavaList());
    }

    @RequestMapping(value = "events/{circuitBreakerName}", produces = "text/event-stream")
    public SseEmitter getEventsStreamFilteredByCircuitBreakerName(@PathVariable("circuitBreakerName") String circuitBreakerName) {
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.getAllCircuitBreakers()
                .find(cb -> cb.getName().equals(circuitBreakerName))
                .getOrElseThrow(() ->
                        new IllegalArgumentException(String.format("circuit breaker with name %s not found", circuitBreakerName)))
                ;
        CircuitBreakerEventSseEmitter eventEmitter = new CircuitBreakerEventSseEmitter(circuitBreaker);
        return eventEmitter.getSseEmitter();
    }

    @RequestMapping(value = "events/{circuitBreakerName}/{eventType}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public CircuitBreakerEventsEndpointResponse getEventsFilteredByCircuitBreakerNameAndEventType(@PathVariable("circuitBreakerName") String circuitBreakerName,
                                                @PathVariable("eventType") String eventType) {
        return new CircuitBreakerEventsEndpointResponse(eventConsumerRegistry.getEventConsumer(circuitBreakerName).getBufferedEvents()
                .filter(event -> event.getCircuitBreakerName().equals(circuitBreakerName))
                .filter(event -> event.getEventType() == CircuitBreakerEvent.Type.valueOf(eventType.toUpperCase()))
                .map(CircuitBreakerEventDTOFactory::createCircuitBreakerEventDTO).toJavaList());
    }
}
