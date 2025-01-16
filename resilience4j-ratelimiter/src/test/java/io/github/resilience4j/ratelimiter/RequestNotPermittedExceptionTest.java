package io.github.resilience4j.ratelimiter;

import org.junit.Test;

import static io.github.resilience4j.ratelimiter.RequestNotPermitted.createRequestNotPermitted;
import static org.assertj.core.api.Assertions.assertThat;

public class RequestNotPermittedExceptionTest {

    @Test
    public void shouldReturnCorrectMessageWhenStateIsOpen() {
        RateLimiter rateLimiter = RateLimiter.ofDefaults("testName");
        final RequestNotPermitted requestNotPermitted = createRequestNotPermitted(rateLimiter);
        assertThat(requestNotPermitted.getMessage())
            .isEqualTo("RateLimiter 'testName' does not permit further calls");
        assertThat(requestNotPermitted.getCausingRateLimiterName()).isEqualTo(rateLimiter.getName());
    }

}
