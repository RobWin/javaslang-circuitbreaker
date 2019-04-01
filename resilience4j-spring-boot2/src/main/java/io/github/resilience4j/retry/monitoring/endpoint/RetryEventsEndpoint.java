/*
 * Copyright 2019 Mahmoud Romeh
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
package io.github.resilience4j.retry.monitoring.endpoint;


import java.util.Comparator;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;

import io.github.resilience4j.consumer.CircularEventConsumer;
import io.github.resilience4j.consumer.EventConsumerRegistry;
import io.github.resilience4j.retry.event.RetryEvent;
import io.vavr.collection.List;


/**
 * rest api endpoint to retrieve retry events
 */
@Endpoint(id = "retryevents")
public class RetryEventsEndpoint {

	private final EventConsumerRegistry<RetryEvent> syncRetryEventConsumerRegistry;

	public RetryEventsEndpoint(EventConsumerRegistry<RetryEvent> eventConsumerRegistry) {
		this.syncRetryEventConsumerRegistry = eventConsumerRegistry;
	}

	/**
	 * @return all retry generated events
	 */
	@ReadOperation
	public RetryEventsEndpointResponse getAllRetryEvenets() {
		return new RetryEventsEndpointResponse(syncRetryEventConsumerRegistry.getAllEventConsumer()
				.flatMap(CircularEventConsumer::getBufferedEvents)
				.sorted(Comparator.comparing(RetryEvent::getCreationTime))
				.map(RetryEventDTOFactory::createRetryEventDTO).toJavaList());
	}

	/**
	 * @param name backend name
	 * @return the retry events generated for this backend
	 */
	@ReadOperation
	public RetryEventsEndpointResponse getEventsFilteredByRetryrName(@Selector String name) {
		return new RetryEventsEndpointResponse(getRetryEventCircularEventConsumer(name)
				.filter(event -> event.getName().equals(name))
				.map(RetryEventDTOFactory::createRetryEventDTO).toJavaList());
	}

	/**
	 * @param name      backend name
	 * @param eventType retry event type
	 * @return the matching generated retry events
	 */
	@ReadOperation
	public RetryEventsEndpointResponse getEventsFilteredByRetryNameAndEventType(@Selector String name, @Selector String eventType) {
		return new RetryEventsEndpointResponse(getRetryEventCircularEventConsumer(name)
				.filter(event -> event.getName().equals(name))
				.filter(event -> event.getEventType() == RetryEvent.Type.valueOf(eventType.toUpperCase()))
				.map(RetryEventDTOFactory::createRetryEventDTO).toJavaList());
	}

	private List<RetryEvent> getRetryEventCircularEventConsumer(String name) {
		final CircularEventConsumer<RetryEvent> syncEvents = syncRetryEventConsumerRegistry.getEventConsumer(name);
		if (syncEvents != null) {
			return syncEvents.getBufferedEvents();
		} else {
			return List.empty();
		}
	}
}
