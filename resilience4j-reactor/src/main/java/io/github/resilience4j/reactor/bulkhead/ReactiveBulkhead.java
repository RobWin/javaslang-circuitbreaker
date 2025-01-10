package io.github.resilience4j.reactor.bulkhead;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class ReactiveBulkhead {

    private final Bulkhead bulkhead;
    private final ConcurrentLinkedDeque<QueuedAcquirer> queue;

    public ReactiveBulkhead(Bulkhead bulkhead) {
        this.bulkhead = bulkhead;
        this.queue = new ConcurrentLinkedDeque<>();
    }

    public Mono<Permission> acquirePermission() {
        Duration maxWaitDuration = bulkhead.getBulkheadConfig().getMaxWaitDuration();

        if (maxWaitDuration == null || maxWaitDuration.isZero()) {
            return bulkhead.tryAcquirePermission() ? Mono.just(new NonQueuedAcquirer(bulkhead)) :
                Mono.error(BulkheadFullException.createBulkheadFullException(bulkhead));
        }

        return Mono.create(sink -> {
            QueuedAcquirer queuedAcquirer = new QueuedAcquirer(sink, bulkhead, maxWaitDuration);

            sink.onCancel(() -> {
                queuedAcquirer.cancel();
                offerNewPermitIfAvailable();
            });

            queue.offerLast(queuedAcquirer);
            offerNewPermitIfAvailable();
        });
    }


    public Mono<Void> releasePermission(Permission permission) {
        return releasePermission(permission, true);
    }

    public Mono<Void> releasePermission(Permission permission, boolean wasUsed) {
        return Mono.fromRunnable(() -> {
            permission.release(wasUsed);
            offerNewPermitIfAvailable();
        });
    }

    private void offerNewPermitIfAvailable() {
        // all permissions taken but the acquirer is already queued, will get completed
        // when more release calls will come in
        if (!bulkhead.tryAcquirePermissionNoWait()) {
            return;
        }

        // managed to get a permit, use it to complete a queued acquirer
        while (true) {
            QueuedAcquirer acquirer = queue.pollFirst();
            // lost the dequeuing battle, release the permissions
            if (acquirer == null) {
                bulkhead.releasePermission();
                break;
            }

            // won the completion battle, job done
            if (acquirer.tryComplete()) {
                break;
            }
        }
    }


    public interface Permission {
        void release(boolean wasUsed);
    }

    private static class NonQueuedAcquirer implements Permission {

        private final Bulkhead bulkhead;

        private NonQueuedAcquirer(Bulkhead bulkhead) {
            this.bulkhead = bulkhead;
        }

        @Override
        public void release(boolean wasUsed) {
            if (wasUsed) {
                bulkhead.onComplete();
            } else {
                bulkhead.releasePermission();
            }
        }
    }

    private static class QueuedAcquirer implements Permission {

        public static final AtomicIntegerFieldUpdater<QueuedAcquirer> STATE =
            AtomicIntegerFieldUpdater.newUpdater(QueuedAcquirer.class, "state");
        private static final int STATE_PENDING = 0;
        private static final int STATE_COMPLETED = 1;
        private static final int STATE_CANCELED = 2;
        private static final int STATE_RELEASED = 3;

        private volatile int state;
        private final MonoSink<Permission> sink;
        private final Bulkhead bulkhead;
        private final Disposable timeoutTask;

        public QueuedAcquirer(MonoSink<Permission> sink, Bulkhead semaphore, Duration maxWaitDuration) {
            this.sink = sink;
            this.bulkhead = semaphore;
            this.timeoutTask = Schedulers.parallel().schedule(this::timeout, maxWaitDuration.toMillis(), TimeUnit.MILLISECONDS);
            state = STATE_PENDING;
        }

        public boolean tryComplete() {
            if (STATE.compareAndSet(this, STATE_PENDING, STATE_COMPLETED)) {
                if (!timeoutTask.isDisposed()) {
                    timeoutTask.dispose();
                }
                sink.success(this);
                return true;
            }

            return false;
        }

        public void timeout() {
            if (STATE.compareAndSet(this, STATE_PENDING, STATE_CANCELED)) {
                sink.error(BulkheadFullException.createBulkheadFullException(bulkhead));
            }
        }

        public void cancel() {
            release(false);
            STATE.compareAndSet(this, STATE_PENDING, STATE_CANCELED);
        }

        @Override
        public void release(boolean wasUsed) {
            if (STATE.compareAndSet(this, STATE_COMPLETED, STATE_RELEASED)) {
                if (wasUsed) {
                    bulkhead.onComplete();
                } else {
                    bulkhead.releasePermission();
                }
            }
        }
    }
}
