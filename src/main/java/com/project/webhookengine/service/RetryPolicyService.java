package com.project.webhookengine.service;

import com.project.webhookengine.retry.RetryDecision;
import org.springframework.stereotype.Component;

@Component
public class RetryPolicyService {
    public static final String TOPIC_RETRY_10S = "webhooks.retry.10s";
    public static final String TOPIC_RETRY_1M = "webhooks.retry.1m";
    public static final String TOPIC_RETRY_5M = "webhooks.retry.5m";
    public static final String TOPIC_DLQ = "webhooks.dlq";

    public static final int MAX_ATTEMPTS = 4;

    public RetryDecision determineNextStep(int currentAttemptCount) {
        if (currentAttemptCount >= MAX_ATTEMPTS) {
            return new RetryDecision(TOPIC_DLQ, 0, true);
        }

        return switch (currentAttemptCount) {
            case 1 -> new RetryDecision(TOPIC_RETRY_10S, 10, false);
            case 2 -> new RetryDecision(TOPIC_RETRY_1M, 60, false);
            case 3 -> new RetryDecision(TOPIC_RETRY_5M, 300, false);
            default -> new RetryDecision(TOPIC_DLQ, 0, true);
        };
    }
}