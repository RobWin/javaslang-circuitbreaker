/*
 * Copyright 2019 Julien Hoarau, Robert Winkler
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
import io.github.resilience4j.core.registry.InMemoryRegistryStore;
import io.github.resilience4j.reactor.IllegalPublisherException;
import io.github.resilience4j.reactor.bulkhead.ReactiveBulkhead;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

/**
 * A Bulkhead operator which checks if a subscriber/observer can acquire a permission to subscribe
 * to an upstream Publisher. Otherwise emits a {@link BulkheadFullException}, if the Bulkhead is
 * full.
 *
 * @param <T> the value type
 */
public class BulkheadOperator<T> implements UnaryOperator<Publisher<T>> {

    private final ReactiveBulkhead reactiveBulkhead;

    private static final InMemoryRegistryStore<ReactiveBulkhead> reactiveBulkheadStore
        = new InMemoryRegistryStore<>();


    private BulkheadOperator(ReactiveBulkhead reactiveBulkhead) {
        this.reactiveBulkhead = reactiveBulkhead;
    }

    /**
     * Creates a BulkheadOperator.
     *
     * @param <T>      the value type of the upstream and downstream
     * @param bulkhead the Bulkhead
     * @return a BulkheadOperator
     */
    public static <T> BulkheadOperator<T> of(Bulkhead bulkhead) {
        return new BulkheadOperator<>(reactiveBulkheadStore.computeIfAbsent(bulkhead.getName(), name -> new ReactiveBulkhead(bulkhead)));
    }

    @Override
    public Publisher<T> apply(Publisher<T> publisher) {
        if (publisher instanceof Mono) {
            return Mono.usingWhen(
                reactiveBulkhead.acquirePermission(),
                permission -> (Mono<? extends T>)publisher,
                reactiveBulkhead::releasePermission,
                (permission, t) -> reactiveBulkhead.releasePermission(permission),
                permission -> reactiveBulkhead.releasePermission(permission, false)
            );
        }

        if (publisher instanceof Flux) {
            AtomicBoolean atomicBoolean = new AtomicBoolean(false);
            return Flux.usingWhen(
                reactiveBulkhead.acquirePermission(),
                permission -> ((Flux<? extends  T>)publisher)
                    .doOnSubscribe(s -> atomicBoolean.getAndSet(true)),
                reactiveBulkhead::releasePermission,
                (permission, t) -> reactiveBulkhead.releasePermission(permission),
                permission -> reactiveBulkhead.releasePermission(permission, atomicBoolean.get())
            );
        }

        throw new IllegalPublisherException(publisher);
    }
}
