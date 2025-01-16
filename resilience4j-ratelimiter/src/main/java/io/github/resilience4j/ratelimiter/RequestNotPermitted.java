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

/**
 * Exception that indicates that current thread was not able to acquire permission from {@link
 * RateLimiter}.
 */
public class RequestNotPermitted extends RuntimeException {

    private final transient String causingRateLimiter;

    private RequestNotPermitted(RateLimiter rateLimiter, String message, boolean writableStackTrace) {
        super(message, null, false, writableStackTrace);
        this.causingRateLimiter = rateLimiter.getName();
    }

    /**
     * Static method to construct a {@link RequestNotPermitted} with a RateLimiter.
     *
     * @param rateLimiter the RateLimiter.
     */
    public static RequestNotPermitted createRequestNotPermitted(RateLimiter rateLimiter) {
        boolean writableStackTraceEnabled = rateLimiter.getRateLimiterConfig()
                .isWritableStackTraceEnabled();

        String message = String
                .format("RateLimiter '%s' does not permit further calls", rateLimiter.getName());

        return new RequestNotPermitted(rateLimiter, message, writableStackTraceEnabled);
    }

    /**
     * Returns the name of {@link RateLimiter} that caused this exception.
     *
     * @return the name of  {@link RateLimiter} that caused this exception.
     */
    public String getCausingRateLimiterName() {
        return this.causingRateLimiter;
    }
}
