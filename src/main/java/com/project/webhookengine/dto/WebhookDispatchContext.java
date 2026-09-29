package com.project.webhookengine.dto;

import java.util.UUID;

public record WebhookDispatchContext(
        UUID dispatchTaskId,
        String targetUrl,
        String secretKey,
        String payload,
        String eventId,
        String eventType
) {
}
