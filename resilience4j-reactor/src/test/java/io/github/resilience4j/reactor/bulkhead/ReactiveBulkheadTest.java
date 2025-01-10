package io.github.resilience4j.reactor.bulkhead;


import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import org.junit.Test;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.resilience4j.bulkhead.BulkheadConfig.custom;
import static org.assertj.core.api.Assertions.*;

public class ReactiveBulkheadTest {

    @Test
    public void shouldReturnPermitWhenBulkheadHasRoom() {
        BulkheadConfig bulkheadConfig = custom().maxConcurrentCalls(1).maxWaitDuration(Duration.ofMillis(500)).build();
        Bulkhead bulkhead = Bulkhead.of("test", bulkheadConfig);
        ReactiveBulkhead reactiveBulkhead = new ReactiveBulkhead(bulkhead);

        assertThatCode(() -> reactiveBulkhead.acquirePermission().block())
            .doesNotThrowAnyException();
    }

    @Test
    public void shouldReturnPermitWhenBulkheadHasRoomAndNoWaitTime() {
        BulkheadConfig bulkheadConfig = custom().maxConcurrentCalls(1).maxWaitDuration(Duration.ofMillis(0)).build();
        Bulkhead bulkhead = Bulkhead.of("test", bulkheadConfig);
        ReactiveBulkhead reactiveBulkhead = new ReactiveBulkhead(bulkhead);

        assertThatCode(() -> reactiveBulkhead.acquirePermission().block())
            .doesNotThrowAnyException();
    }

    @Test
    public void shouldThrowWhenBulkheadFull() {
        BulkheadConfig bulkheadConfig = custom().maxConcurrentCalls(1).maxWaitDuration(Duration.ofMillis(500)).build();
        Bulkhead bulkhead = Bulkhead.of("test", bulkheadConfig);
        ReactiveBulkhead reactiveBulkhead = new ReactiveBulkhead(bulkhead);
        reactiveBulkhead.acquirePermission().block();

        AtomicReference<Long> subscribeTimestamp = new AtomicReference<>();
        AtomicReference<Long> acquireTimeoutTimestamp = new AtomicReference<>();

        assertThatExceptionOfType(BulkheadFullException.class)
            .isThrownBy(() -> reactiveBulkhead.acquirePermission()
                .doOnSubscribe(s -> subscribeTimestamp.set(System.currentTimeMillis()))
                .doOnError(BulkheadFullException.class,
                    rstae -> acquireTimeoutTimestamp.set(System.currentTimeMillis()))
                .block());

        assertThat(acquireTimeoutTimestamp.get() - subscribeTimestamp.get())
            .isGreaterThanOrEqualTo(500);
    }

    @Test
    public void shouldThrowWhenBulkheadFullAndNoWaitTime() {
        BulkheadConfig bulkheadConfig = custom().maxConcurrentCalls(1).maxWaitDuration(Duration.ofMillis(0)).build();
        Bulkhead bulkhead = Bulkhead.of("test", bulkheadConfig);
        ReactiveBulkhead reactiveBulkhead = new ReactiveBulkhead(bulkhead);
        reactiveBulkhead.acquirePermission().block();

        assertThatExceptionOfType(BulkheadFullException.class)
            .isThrownBy(() -> reactiveBulkhead.acquirePermission().block());
    }

    @Test
    public void shouldReturnPermitIfAnotherIsReleasedInTheMeantime() {
        BulkheadConfig bulkheadConfig = custom().maxConcurrentCalls(1).maxWaitDuration(Duration.ofMillis(500)).build();
        Bulkhead bulkhead = Bulkhead.of("test", bulkheadConfig);
        ReactiveBulkhead reactiveBulkhead = new ReactiveBulkhead(bulkhead);

        ReactiveBulkhead.Permission permission = reactiveBulkhead.acquirePermission().block();

        Schedulers.parallel().schedule(() -> reactiveBulkhead.releasePermission(permission).block(), 100, TimeUnit.MILLISECONDS);

        assertThatCode(() -> reactiveBulkhead.acquirePermission().block())
            .doesNotThrowAnyException();
    }

    @Test
    public void shouldNotReleaseAnyExtraPermitWhenCancelled() {
        BulkheadConfig bulkheadConfig = custom().maxConcurrentCalls(1).maxWaitDuration(Duration.ofMillis(500)).build();
        Bulkhead bulkhead = Bulkhead.of("test", bulkheadConfig);
        ReactiveBulkhead reactiveBulkhead = new ReactiveBulkhead(bulkhead);

        reactiveBulkhead.acquirePermission().block();

        assertThatCode(() -> reactiveBulkhead.acquirePermission().timeout(Duration.ofMillis(200)).block())
            .hasCauseInstanceOf(TimeoutException.class);

        assertThat(bulkhead.getMetrics().getAvailableConcurrentCalls()).isEqualTo(0);
    }
}
