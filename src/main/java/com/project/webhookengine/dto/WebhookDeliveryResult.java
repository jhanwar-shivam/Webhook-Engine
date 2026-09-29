package com.project.webhookengine.dto;

import org.jspecify.annotations.Nullable;

import java.time.Instant;

public record WebhookDeliveryResult(
        int statusCode,
        String responseSummary,
        boolean isSuccessful,
        @Nullable Instant deliveredAt
) {
}
