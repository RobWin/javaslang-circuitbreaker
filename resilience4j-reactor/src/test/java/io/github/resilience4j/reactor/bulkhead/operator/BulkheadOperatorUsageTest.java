package io.github.resilience4j.reactor.bulkhead.operator;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.resilience4j.bulkhead.BulkheadConfig.custom;
import static org.assertj.core.api.Assertions.assertThat;

public class BulkheadOperatorUsageTest {

    @Test
    public void testBulkheadOperatorUsage() throws Exception {
        int fluxElements = 10000;
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger atomicInteger = new AtomicInteger();
        BulkheadConfig bulkheadConfig = custom().maxConcurrentCalls(30).maxWaitDuration(Duration.ofSeconds(30)).build();
        Bulkhead bulkhead = Bulkhead.of("testUsage", bulkheadConfig);

        Flux.range(0, fluxElements)
            .delayElements(Duration.ofNanos(10))
            .flatMap(i -> Mono.just(i).delayElement(Duration.ofMillis(2)).transformDeferred(BulkheadOperator.of(bulkhead)))
            .doOnNext(i -> atomicInteger.addAndGet(1))
            .doOnTerminate(latch::countDown)
            .subscribe();

        latch.await();
        assertThat(bulkhead.getMetrics().getAvailableConcurrentCalls()).isEqualTo(30);
        assertThat(atomicInteger.get()).isEqualTo(fluxElements);
    }

    @Test
    public void testReactiveSemaphoreUsageError() throws Exception {
        int fluxElements = 10000;
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger atomicInteger = new AtomicInteger();
        BulkheadConfig bulkheadConfig = custom().maxConcurrentCalls(30).maxWaitDuration(Duration.ofSeconds(30)).build();
        Bulkhead bulkhead = Bulkhead.of("testUsageError", bulkheadConfig);

        Flux.range(0, fluxElements)
            .delayElements(Duration.ofNanos(10))
            .flatMap(i -> Mono.just(i).delayElement(Duration.ofMillis(2))
                .then(Mono.error(new RuntimeException("BOOM!"))).transformDeferred(BulkheadOperator.of(bulkhead))
                .onErrorReturn(i))
            .doOnNext(i -> atomicInteger.addAndGet(1))
            .doOnTerminate(latch::countDown)
            .subscribe();

        latch.await();
        assertThat(bulkhead.getMetrics().getAvailableConcurrentCalls()).isEqualTo(30);
        assertThat(atomicInteger.get()).isEqualTo(fluxElements);
    }
}
