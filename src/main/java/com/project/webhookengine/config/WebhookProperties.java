package com.project.webhookengine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "webhook")
public record WebhookProperties(
        Api api,
        Kafka kafka,
        Retry retry,
        OutboundRateLimit outboundRateLimit,
        Dispatch dispatch
) {
    public record Api(String dispatchPath) {
    }

    public record Kafka(int dispatchConcurrency, Topics topics) {
    }

    public record Topics(
            String dispatch,
            String retry10s,
            String retry1m,
            String retry5m,
            String dlq
    ) {
    }

    public record Retry(int maxDeliveryAttempts, int maxThrottleRetries) {
    }

    public record OutboundRateLimit(int bucketCapacity, int refillPerSecond) {
    }

    public record Dispatch(
            int connectTimeoutSeconds,
            int httpTimeoutSeconds,
            int listenerTimeoutSeconds
    ) {
    }
}
