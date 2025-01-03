package io.github.resilience4j.bulkhead.configure;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.bulkhead.ThreadPoolBulkhead;
import io.github.resilience4j.bulkhead.ThreadPoolBulkheadRegistry;
import io.github.resilience4j.core.ContextAwareScheduledThreadPoolExecutor;
import io.github.resilience4j.core.lang.Nullable;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.aspectj.lang.ProceedingJoinPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.*;

/**
 * The {@code PlainObjectBulkheadAspectExt} class provides an aspect for managing method executions
 * that return plain objects while enforcing bulkhead constraints.
 */
public class PlainObjectBulkheadAspectExt implements BulkheadAspectExt {

    private static final Logger logger = LoggerFactory.getLogger(PlainObjectBulkheadAspectExt.class);

    private final ThreadPoolBulkheadRegistry threadPoolBulkheadRegistry;
    private final TimeLimiterRegistry timeLimiterRegistry;
    private final ScheduledExecutorService executorService;

    public PlainObjectBulkheadAspectExt(
            ThreadPoolBulkheadRegistry threadPoolBulkheadRegistry,
            TimeLimiterRegistry timeLimiterRegistry,
            @Nullable ContextAwareScheduledThreadPoolExecutor contextAwareScheduledThreadPoolExecutor
    ) {
        this.threadPoolBulkheadRegistry = threadPoolBulkheadRegistry;
        this.timeLimiterRegistry = timeLimiterRegistry;
        this.executorService = contextAwareScheduledThreadPoolExecutor != null
                ? contextAwareScheduledThreadPoolExecutor
                : Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    }

    /**
     * Determines if the aspect can handle the specified return type of the method.
     *
     * @param returnType the AOP method return type class
     * @return {@code true} if the method has a plain object return type; {@code false} otherwise.
     */
    @Override
    @SuppressWarnings("rawtypes")
    public boolean canHandleReturnType(Class returnType) {
        return !returnType.equals(CompletableFuture.class)
                && !returnType.equals(Mono.class)
                && !returnType.equals(Flux.class);
    }

    /**
     * Handle the execution of a method wrapped in a bulkhead context.
     *
     * @param proceedingJoinPoint Spring AOP proceedingJoinPoint
     * @param bulkhead the configured bulkhead
     * @param methodName the method name
     * @return the result object from the method execution
     * @throws Throwable if an exception occurs during the method execution
     */
    @Override
    public Object handle(ProceedingJoinPoint proceedingJoinPoint, Bulkhead bulkhead, String methodName) throws Throwable {
        TimeLimiter timeLimiter = timeLimiterRegistry.timeLimiter(bulkhead.getName());
        if (bulkhead instanceof ThreadPoolBulkhead) {
            ThreadPoolBulkhead threadPoolBulkhead = threadPoolBulkheadRegistry.bulkhead(bulkhead.getName());
            return handleThreadPoolBulkhead(proceedingJoinPoint, threadPoolBulkhead, timeLimiter, methodName);
        }
        return bulkhead.executeCheckedSupplier(proceedingJoinPoint::proceed);
    }

    private Object handleThreadPoolBulkhead(ProceedingJoinPoint proceedingJoinPoint, ThreadPoolBulkhead threadPoolBulkhead, TimeLimiter timeLimiter, String methodName) throws Throwable {
        try {
            CompletableFuture<Object> completableFuture = threadPoolBulkhead.decorateCallable (() -> {
                try {
                    return proceedingJoinPoint.proceed();
                } catch (Throwable throwable) {
                    return completeFutureExceptionally(throwable);
                }
            }).get().toCompletableFuture();

            return timeLimiter.executeCompletionStage(executorService, () -> completableFuture)
                    .toCompletableFuture()
                    .join();
        } catch (CompletionException ex) {
            logger.error("Completion exception occurred while executing '{}': {}", methodName, ex.getMessage(), ex);
            throw ex.getCause();
        } catch (BulkheadFullException ex) {
            logger.error("BulkheadFullException occurred while executing '{}': {}", methodName, ex.getMessage(), ex);
            throw ex;
        }
    }

    private CompletableFuture<Object> completeFutureExceptionally(Throwable throwable) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }
}
