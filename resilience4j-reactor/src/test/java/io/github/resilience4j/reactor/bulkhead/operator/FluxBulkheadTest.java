/*
 * Copyright 2018 Julien Hoarau
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
package io.github.resilience4j.reactor.bulkhead.operator;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

public class FluxBulkheadTest {


    private Bulkhead bulkhead;

    @Before
    public void setUp() {
        bulkhead = mock(Bulkhead.class, RETURNS_DEEP_STUBS);
        given(bulkhead.getName()).willReturn(UUID.randomUUID().toString());
    }

    @Test
    public void shouldEmitEvent() {
        given(bulkhead.tryAcquirePermission()).willReturn(true);

        StepVerifier.create(
            Flux.just("Event 1", "Event 2")
                .transformDeferred(BulkheadOperator.of(bulkhead)))
            .expectNext("Event 1")
            .expectNext("Event 2")
            .verifyComplete();

        verify(bulkhead, times(1)).onComplete();
    }

    @Test
    public void shouldPropagateError() {
        given(bulkhead.tryAcquirePermission()).willReturn(true);

        StepVerifier.create(
            Flux.error(new IOException("BAM!"))
                .transformDeferred(BulkheadOperator.of(bulkhead)))
            .expectSubscription()
            .expectError(IOException.class)
            .verify(Duration.ofSeconds(1));

        verify(bulkhead, times(1)).onComplete();
    }

    @Test
    public void shouldEmitErrorWithBulkheadFullException() {
        given(bulkhead.tryAcquirePermission()).willReturn(false);
        bulkhead.tryAcquirePermission();

        StepVerifier.create(
            Flux.just("Event")
                .transformDeferred(BulkheadOperator.of(bulkhead)))
            .expectSubscription()
            .expectError(BulkheadFullException.class)
            .verify(Duration.ofSeconds(1));

        verify(bulkhead, never()).onComplete();
    }

    @Test
    public void shouldEmitBulkheadFullExceptionEvenWhenErrorDuringSubscribe() {
        given(bulkhead.tryAcquirePermission()).willReturn(false);

        StepVerifier.create(
            Flux.error(new IOException("BAM!"))
                .transformDeferred(BulkheadOperator.of(bulkhead)))
            .expectSubscription()
            .expectError(BulkheadFullException.class)
            .verify(Duration.ofSeconds(1));

        verify(bulkhead, never()).onComplete();
    }

    @Test
    public void shouldEmitBulkheadFullExceptionEvenWhenErrorNotOnSubscribe() {
        given(bulkhead.tryAcquirePermission()).willReturn(false);

        StepVerifier.create(
            Flux.error(new IOException("BAM!"), true)
                .transformDeferred(BulkheadOperator.of(bulkhead)))
            .expectSubscription()
            .expectError(BulkheadFullException.class)
            .verify(Duration.ofSeconds(1));

        verify(bulkhead, never()).onComplete();
    }

    @Test
    public void shouldInvokeOnCompleteOnCancelAfterSubscription() {
        given(bulkhead.tryAcquirePermission()).willReturn(true);

        StepVerifier.create(
            Flux.just("Event")
                .delayElements(Duration.ofHours(1))
                .transformDeferred(BulkheadOperator.of(bulkhead)))
            .expectSubscription()
            .thenCancel()
            .verify();

        verify(bulkhead, never()).releasePermission();
        verify(bulkhead, times(1)).onComplete();
    }

    @Test
    public void shouldInvokeOnCompleteOnCancelWhenEventWasEmitted() {
        given(bulkhead.tryAcquirePermission()).willReturn(true);

        StepVerifier.create(
            Flux.just("Event1", "Event2", "Event3")
                .transformDeferred(BulkheadOperator.of(bulkhead)))
            .expectSubscription()
            .thenRequest(1)
            .thenCancel()
            .verify();

        verify(bulkhead, never()).releasePermission();
        verify(bulkhead, times(1)).onComplete();
    }

    @Test
    public void shouldOnceEmitCompleteWhenErrorInCompleteEvent() {
        given(bulkhead.tryAcquirePermission()).willReturn(true);
        doThrow(new RuntimeException("BAM!")).when(bulkhead).onComplete();

        StepVerifier.create(
            Flux.just("Event1", "Event2", "Event3")
                .transformDeferred(BulkheadOperator.of(bulkhead)))
            .expectNext("Event1")
            .expectNext("Event2")
            .expectNext("Event3")
            .expectError(RuntimeException.class)
            .verify(Duration.ofSeconds(1));

        verify(bulkhead, times(1)).onComplete();
    }
}
