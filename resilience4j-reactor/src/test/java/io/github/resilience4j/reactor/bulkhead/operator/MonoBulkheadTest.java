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
import org.testng.annotations.BeforeMethod;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

public class MonoBulkheadTest {

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
            Mono.just("Event")
                .transformDeferred(BulkheadOperator.of(bulkhead)))
            .expectNext("Event")
            .verifyComplete();

        verify(bulkhead, times(1)).onComplete();
    }

    @Test
    public void shouldPropagateError() {
        given(bulkhead.tryAcquirePermission()).willReturn(true);

        StepVerifier.create(
            Mono.error(new IOException("BAM!"))
                .transformDeferred(BulkheadOperator.of(bulkhead)))
            .expectSubscription()
            .expectError(IOException.class)
            .verify(Duration.ofSeconds(1));

        verify(bulkhead, times(1)).onComplete();
    }

    @Test
    public void shouldEmitErrorWithBulkheadFullException() {
        given(bulkhead.tryAcquirePermission()).willReturn(false);

        StepVerifier.create(
            Mono.just("Event")
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
            Mono.error(new IOException("BAM!"))
                .transformDeferred(BulkheadOperator.of(bulkhead)))
            .expectSubscription()
            .expectError(BulkheadFullException.class)
            .verify(Duration.ofSeconds(1));
    }

    @Test
    public void shouldReleaseBulkheadSemaphoreOnCancel() {
        given(bulkhead.tryAcquirePermission()).willReturn(true);

        StepVerifier.create(
            Mono.just("Event")
                .delayElement(Duration.ofHours(1))
                .transformDeferred(BulkheadOperator.of(bulkhead)))
            .expectSubscription()
            .thenCancel()
            .verify();

        verify(bulkhead, times(1)).releasePermission();
    }

    @Test
    public void shouldOnceEmitCompleteWhenErrorInCompleteEvent() {
        given(bulkhead.tryAcquirePermission()).willReturn(true);
        doThrow(new RuntimeException("BAM!")).when(bulkhead).onComplete();

        StepVerifier.create(
            Mono.just("Event")
                .transformDeferred(BulkheadOperator.of(bulkhead)))
            .expectSubscription()
            .expectError(RuntimeException.class)
            .verify(Duration.ofSeconds(1));

        verify(bulkhead, times(1)).onComplete();
    }
}
