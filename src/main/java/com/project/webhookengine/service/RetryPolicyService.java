package com.project.webhookengine.service;

import com.project.webhookengine.config.WebhookProperties;
import com.project.webhookengine.retry.RetryDecision;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class RetryPolicyService {

    private final WebhookProperties webhookProperties;

    public RetryDecision determineNextStep(int currentAttemptCount) {
        WebhookProperties.Topics topics = webhookProperties.kafka().topics();
        int maxAttempts = webhookProperties.retry().maxDeliveryAttempts();

        if (currentAttemptCount >= maxAttempts) {
            return new RetryDecision(topics.dlq(), 0, true);
        }

        return switch (currentAttemptCount) {
            case 1 -> new RetryDecision(topics.retry10s(), 10, false);
            case 2 -> new RetryDecision(topics.retry1m(), 60, false);
            case 3 -> new RetryDecision(topics.retry5m(), 300, false);
            default -> new RetryDecision(topics.dlq(), 0, true);
        };
    }

    public String retryTopic10Seconds() {
        return webhookProperties.kafka().topics().retry10s();
    }

    public String deadLetterTopic() {
        return webhookProperties.kafka().topics().dlq();
    }
}
