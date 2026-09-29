package com.project.webhookengine.worker;

import com.project.webhookengine.config.WebhookProperties;
import com.project.webhookengine.dto.WebhookDeliveryResult;
import com.project.webhookengine.dto.WebhookDispatchContext;
import com.project.webhookengine.utils.SignatureUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@Component
public class WebhookHttpClient {

    private final SignatureUtil signatureUtil;
    private final HttpClient httpClient;
    private final WebhookProperties webhookProperties;

    public WebhookHttpClient(SignatureUtil signatureUtil, WebhookProperties webhookProperties) {
        this.signatureUtil = signatureUtil;
        this.webhookProperties = webhookProperties;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(webhookProperties.dispatch().connectTimeoutSeconds()))
                .build();
    }

    public WebhookDeliveryResult send(WebhookDispatchContext context) {
        try {
            String signature = signatureUtil.generateSignature(context.payload(), context.secretKey());
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(context.targetUrl()))
                    .timeout(Duration.ofSeconds(webhookProperties.dispatch().httpTimeoutSeconds()))
                    .header("Content-Type", "application/json")
                    .header("X-Webhook-Signature", signature)
                    .header("X-Webhook-Event-Id", context.eventId())
                    .header("X-Webhook-Event-Type", context.eventType())
                    .POST(HttpRequest.BodyPublishers.ofString(context.payload()))
                    .build();

            log.info("Dispatching task {} to URL: {}", context.dispatchTaskId(), context.targetUrl());

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int statusCode = response.statusCode();
            boolean isSuccess = statusCode >= 200 && statusCode < 300;

            String responseSummary = isSuccess ? "Delivered Successfully" : truncate(response.body());
            Instant deliveredAt = isSuccess ? Instant.now() : null;

            return new WebhookDeliveryResult(statusCode, responseSummary, isSuccess, deliveredAt);
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("Network error delivering task {}: {}", context.dispatchTaskId(), e.getMessage());
            return new WebhookDeliveryResult(0, e.getClass().getSimpleName() + ": " + e.getMessage(), false, null);
        } catch (Exception e) {
            log.error("Unexpected error preparing task {}: {}", context.dispatchTaskId(), e.getMessage(), e);
            return new WebhookDeliveryResult(0, "Internal Error: " + e.getMessage(), false, null);
        }
    }

    private String truncate(String body) {
        if (body == null) {
            return "No response body";
        }
        return body.length() > 1000 ? body.substring(0, 1000) + "...[truncated]" : body;
    }
}
