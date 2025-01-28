/*
 *
 *  Copyright 2016 Robert Winkler and Bohdan Storozhuk
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */
package io.github.resilience4j.ratelimiter;

import io.github.resilience4j.core.functions.CheckedFunction;
import io.github.resilience4j.core.functions.CheckedRunnable;
import io.github.resilience4j.core.functions.CheckedSupplier;
import io.vavr.control.Try;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.jayway.awaitility.Awaitility.await;
import static io.vavr.API.*;
import static io.vavr.Predicates.instanceOf;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.*;


@SuppressWarnings("unchecked")
public class RateLimiterTest {

    private static final int LIMIT = 50;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REFRESH_PERIOD = Duration.ofNanos(500);

    private RateLimiterConfig config;
    private RateLimiter limit;

    @Before
    public void init() {
        config = RateLimiterConfig.custom()
            .timeoutDuration(TIMEOUT)
            .limitRefreshPeriod(REFRESH_PERIOD)
            .limitForPeriod(LIMIT)
            .build();
        limit = mock(RateLimiter.class);
        given(limit.getRateLimiterConfig()).willReturn(config);
    }

    @Test
    public void decorateCheckedSupplier() throws Throwable {
        CheckedSupplier supplier = mock(CheckedSupplier.class);
        CheckedSupplier decorated = RateLimiter.decorateCheckedSupplier(limit, supplier);
        given(limit.acquirePermission(1)).willReturn(false);
        Try decoratedSupplierResult = Try.of(() -> decorated.get());
        assertThat(decoratedSupplierResult.isFailure()).isTrue();
        assertThat(decoratedSupplierResult.getCause()).isInstanceOf(RequestNotPermitted.class);
        then(supplier).should(never()).get();
        given(limit.acquirePermission(1)).willReturn(true);

        Try secondSupplierResult = Try.of(() -> decorated.get());

        assertThat(secondSupplierResult.isSuccess()).isTrue();
        then(supplier).should().get();
    }

    @Test
    public void decorateCheckedRunnable() throws Throwable {
        CheckedRunnable runnable = mock(CheckedRunnable.class);
        CheckedRunnable decorated = RateLimiter.decorateCheckedRunnable(limit, runnable);
        given(limit.acquirePermission(1)).willReturn(false);
        Try decoratedRunnableResult = Try.run(() -> decorated.run());
        assertThat(decoratedRunnableResult.isFailure()).isTrue();
        assertThat(decoratedRunnableResult.getCause()).isInstanceOf(RequestNotPermitted.class);
        then(runnable).should(never()).run();
        given(limit.acquirePermission(1)).willReturn(true);

        Try secondRunnableResult = Try.run(() -> decorated.run());

        assertThat(secondRunnableResult.isSuccess()).isTrue();
        then(runnable).should().run();
    }

    @Test
    public void decorateCheckedFunction() throws Throwable {
        CheckedFunction<Integer, String> function = mock(CheckedFunction.class);
        CheckedFunction<Integer, String> decorated = RateLimiter
            .decorateCheckedFunction(limit, function);
        given(limit.acquirePermission(1)).willReturn(false);
        Try<String> decoratedFunctionResult = Try.success(1).mapTry(value -> decorated.apply(value));
        assertThat(decoratedFunctionResult.isFailure()).isTrue();
        assertThat(decoratedFunctionResult.getCause()).isInstanceOf(RequestNotPermitted.class);
        then(function).should(never()).apply(any());
        given(limit.acquirePermission(1)).willReturn(true);

        Try secondFunctionResult = Try.success(1).mapTry(value -> decorated.apply(value));

        assertThat(secondFunctionResult.isSuccess()).isTrue();
        then(function).should().apply(1);
    }

    @Test
    public void decorateSupplier() {
        Supplier supplier = mock(Supplier.class);
        Supplier decorated = RateLimiter.decorateSupplier(limit, supplier);
        given(limit.acquirePermission(1)).willReturn(false);
        Try decoratedSupplierResult = Try.success(decorated).map(Supplier::get);
        assertThat(decoratedSupplierResult.isFailure()).isTrue();
        assertThat(decoratedSupplierResult.getCause()).isInstanceOf(RequestNotPermitted.class);
        then(supplier).should(never()).get();
        given(limit.acquirePermission(1)).willReturn(true);

        Try secondSupplierResult = Try.success(decorated).map(Supplier::get);

        assertThat(secondSupplierResult.isSuccess()).isTrue();
        then(supplier).should().get();
    }

    @Test
    public void decorateConsumer() {
        Consumer<Integer> consumer = mock(Consumer.class);
        Consumer<Integer> decorated = RateLimiter.decorateConsumer(limit, consumer);
        given(limit.acquirePermission(1)).willReturn(false);
        Try<Integer> decoratedConsumerResult = Try.success(1).andThen(decorated);
        assertThat(decoratedConsumerResult.isFailure()).isTrue();
        assertThat(decoratedConsumerResult.getCause()).isInstanceOf(RequestNotPermitted.class);
        then(consumer).should(never()).accept(any());
        given(limit.acquirePermission(1)).willReturn(true);

        Try secondConsumerResult = Try.success(1).andThen(decorated);

        assertThat(secondConsumerResult.isSuccess()).isTrue();
        then(consumer).should().accept(1);
    }

    @Test
    public void decorateRunnable() {
        Runnable runnable = mock(Runnable.class);
        Runnable decorated = RateLimiter.decorateRunnable(limit, runnable);
        given(limit.acquirePermission(1)).willReturn(false);
        Try decoratedRunnableResult = Try.success(decorated).andThen(Runnable::run);
        assertThat(decoratedRunnableResult.isFailure()).isTrue();
        assertThat(decoratedRunnableResult.getCause()).isInstanceOf(RequestNotPermitted.class);
        then(runnable).should(never()).run();
        given(limit.acquirePermission(1)).willReturn(true);

        Try secondRunnableResult = Try.success(decorated).andThen(Runnable::run);

        assertThat(secondRunnableResult.isSuccess()).isTrue();
        then(runnable).should().run();
    }

    @Test
    public void decorateFunction() {
        Function<Integer, String> function = mock(Function.class);
        Function<Integer, String> decorated = RateLimiter.decorateFunction(limit, function);
        given(limit.acquirePermission(1)).willReturn(false);
        Try<String> decoratedFunctionResult = Try.success(1).map(decorated);
        assertThat(decoratedFunctionResult.isFailure()).isTrue();
        assertThat(decoratedFunctionResult.getCause()).isInstanceOf(RequestNotPermitted.class);
        then(function).should(never()).apply(any());
        given(limit.acquirePermission(1)).willReturn(true);

        Try secondFunctionResult = Try.success(1).map(decorated);

        assertThat(secondFunctionResult.isSuccess()).isTrue();
        then(function).should().apply(1);
    }

    @Test
    public void decorateCompletionStage() {
        Supplier supplier = mock(Supplier.class);
        given(supplier.get()).willReturn("Resource");
        Supplier<CompletionStage<String>> completionStage = () -> supplyAsync(supplier);
        Supplier<CompletionStage<String>> decorated = RateLimiter
            .decorateCompletionStage(limit, completionStage);
        given(limit.acquirePermission(1)).willReturn(false);
        AtomicReference<Throwable> error = new AtomicReference<>(null);
        CompletableFuture<String> notPermittedFuture = decorated.get()
            .whenComplete((v, e) -> error.set(e))
            .toCompletableFuture();

        Try<String> errorResult = Try.of(notPermittedFuture::get);

        assertTrue(errorResult.isFailure());
        assertThat(errorResult.getCause()).isInstanceOf(ExecutionException.class);
        assertThat(notPermittedFuture.isCompletedExceptionally()).isTrue();
        assertThat(error.get()).isExactlyInstanceOf(RequestNotPermitted.class);
        then(supplier).should(never()).get();

        given(limit.acquirePermission(1)).willReturn(true);
        AtomicReference<Throwable> shouldBeEmpty = new AtomicReference<>(null);
        CompletableFuture<String> success = decorated.get()
            .whenComplete((v, e) -> error.set(e))
            .toCompletableFuture();

        Try<String> successResult = Try.of(success::get);

        assertThat(successResult.isSuccess()).isTrue();
        assertThat(success.isCompletedExceptionally()).isFalse();
        assertThat(shouldBeEmpty.get()).isNull();
        then(supplier).should().get();
    }

    @Test(expected = RequestNotPermitted.class)
    public void decorateFutureFailure()
            throws InterruptedException, ExecutionException, TimeoutException {

        Supplier<String> supplier = mock(Supplier.class);
        given(supplier.get()).willReturn("Resource");
        Supplier<Future<String>> decoratedFuture =
            RateLimiter.decorateFuture(limit, () -> supplyAsync(supplier));
        given(limit.acquirePermission(1)).willReturn(false);

        decoratedFuture.get().get(2, TimeUnit.SECONDS);
    }

    @Test
    public void decorateFutureSuccess()
            throws ExecutionException, InterruptedException, TimeoutException {
        Supplier<String> supplier = mock(Supplier.class);
        given(supplier.get()).willReturn("Resource");
        Supplier<Future<String>> decoratedFuture =
            RateLimiter.decorateFuture(limit, () -> supplyAsync(supplier));
        given(limit.acquirePermission(1)).willReturn(true);

        String result = decoratedFuture.get().get(2, TimeUnit.SECONDS);

        then(supplier).should().get();
        assertThat(result).isEqualTo("Resource");
    }

    @Test
    public void waitForPermissionWithOne() {
        given(limit.acquirePermission(1)).willReturn(true);

        RateLimiter.waitForPermission(limit);

        then(limit).should().acquirePermission(1);
    }

    @Test(expected = RequestNotPermitted.class)
    public void waitForPermissionWithoutOne() {
        given(limit.acquirePermission(1)).willReturn(false);

        RateLimiter.waitForPermission(limit);

        then(limit).should().acquirePermission(1);
    }

    @Test
    public void waitForPermissionWithInterruption() {
        when(limit.acquirePermission(1))
            .then(invocation -> {
                LockSupport.parkNanos(5_000_000_000L);
                return null;
            });
        AtomicBoolean wasInterrupted = new AtomicBoolean(true);
        Thread thread = new Thread(() -> {
            wasInterrupted.set(false);
            Throwable cause = Try.run(() -> RateLimiter.waitForPermission(limit))
                .getCause();
            Boolean interrupted = Match(cause).of(
                Case($(instanceOf(IllegalStateException.class)), true)
            );
            interrupted = interrupted && Thread.currentThread().isInterrupted();
            wasInterrupted.set(interrupted);
        });
        thread.setDaemon(true);
        thread.start();

        await()
            .atMost(5, TimeUnit.SECONDS)
            .untilFalse(wasInterrupted);
        thread.interrupt();
        await()
            .atMost(5, TimeUnit.SECONDS)
            .untilTrue(wasInterrupted);
    }

    @Test
    public void construction() {
        RateLimiter rateLimiter = RateLimiter.of("test", () -> config);
        assertThat(rateLimiter).isNotNull();
    }

    @Test
    public void testRequestNotPermittedRateLimiterName() {
        given(limit.getName()).willReturn("testLimiterName");

        RequestNotPermitted exception = RequestNotPermitted.createRequestNotPermitted(limit);

        assertThat(exception.getCausingRateLimiterName()).isEqualTo("testLimiterName");
    }
}