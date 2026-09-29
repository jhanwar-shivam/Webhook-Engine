package com.project.webhookengine.retry;

public record RetryDecision(
        String targetTopic,
        long delaySeconds,
        boolean isExhausted
) {
}
