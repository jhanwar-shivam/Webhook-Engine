package com.project.webhookengine.worker;

import com.project.webhookengine.model.DispatchStatus;
import com.project.webhookengine.model.DispatchTask;
import com.project.webhookengine.model.WebhookEvent;
import com.project.webhookengine.model.WebhookSubscription;
import com.project.webhookengine.repository.DispatchTaskRepository;
import com.project.webhookengine.utils.RateLimitUtil;
import com.project.webhookengine.utils.SignatureUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@RequiredArgsConstructor
@Component
public class WebhookDispatcherWorker {

    private final DispatchTaskRepository dispatchTaskRepository;
    private final RateLimitUtil rateLimitUtil;
    private final SignatureUtil signatureUtil;

    private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private static final Integer CAPACITY = 600;
    private static final Integer REFILL = 10;

    @KafkaListener(topics = "webhooks.dispatch", groupId = "webhook-engine-group")
    @Transactional(readOnly = true)
    public void consumeDispatchTask(String dispatchTaskIdString) {
        UUID dispatchTaskId = UUID.fromString(dispatchTaskIdString);

        DispatchTask dispatchTask = getDispatchTask(dispatchTaskId);
        if (dispatchTask == null) {
            return;
        }

        WebhookSubscription subscription = dispatchTask.getWebhookSubscription();
        WebhookEvent event = dispatchTask.getWebhookEvent();

        String targetUrl = subscription.getTargetUrl();
        String secretKey = subscription.getSecretKey();
        String payload = event.getPayload();
        String eventId = event.getWebhookEventId().toString();
        String eventType = event.getEventType();

        try {
            String domain = extractDomain(targetUrl);
            if (rateLimitUtil.checkOutboundRateLimit(domain, CAPACITY, REFILL)) {
                executeAsyncDispatch(dispatchTaskId, targetUrl, secretKey, payload, eventId, eventType);
            } else {
                log.warn("Rate limit exceeded for domain: {}. Task {} delayed.", domain, dispatchTaskId);
                // Phase 6 will handle delayed retry queueing here
            }
        } catch (URISyntaxException e) {
            log.error("Invalid Target URL for task {}. URL: {}", dispatchTaskId, targetUrl);
        }
    }

    private void executeAsyncDispatch(UUID dispatchTaskId,
                                      String targetUrl,
                                      String secretKey,
                                      String payload,
                                      String eventId,
                                      String eventType) {
        virtualThreadExecutor.submit(() -> {
            int statusCode = 0;
            String responseSummary;
            DispatchStatus status = DispatchStatus.RETRYING;
            Instant deliveredAt = null;

            log.info("Running on Virtual Thread: Preparing to dispatch task {} to URL: {}", dispatchTaskId, targetUrl);

            try {
                String signature = signatureUtil.generateSignature(payload, secretKey);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(targetUrl))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/json")
                        .header("X-Webhook-Signature", signature)
                        .header("X-Webhook-Event-Id", eventId)
                        .header("X-Webhook-Event-Type", eventType)
                        .POST(HttpRequest.BodyPublishers.ofString(payload))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                statusCode = response.statusCode();

                if (statusCode >= 200 && statusCode < 300) {
                    status = DispatchStatus.DELIVERED;
                    deliveredAt = Instant.now();
                    responseSummary = "Delivered successfully";
                } else {
                    String body = response.body();
                    responseSummary = (body != null && body.length() > 1000)
                            ? body.substring(0, 1000) + "... [truncated]"
                            : body;
                }
            } catch (IOException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                log.error("Network error delivering task {}: {}", dispatchTaskId, e.getMessage());
                responseSummary = e.getClass().getSimpleName() + ": " + e.getMessage();
            } catch (Exception e) {
                log.error("Unexpected error executing dispatch for task {}: {}", dispatchTaskId, e.getMessage(), e);
                responseSummary = "Internal Error: " + e.getMessage();
            }

            updateTaskOutcome(dispatchTaskId, status, statusCode, responseSummary, deliveredAt);
        });
    }

    private void updateTaskOutcome(UUID dispatchTaskId,
                                   DispatchStatus status,
                                   int statusCode,
                                   String responseSummary,
                                   @Nullable Instant deliveredAt) {
        Optional<DispatchTask> taskOpt = dispatchTaskRepository.findById(dispatchTaskId);
        if (taskOpt.isEmpty()) {
            log.error("Unable to update task outcome: task {} not found", dispatchTaskId);
            return;
        }

        DispatchTask task = taskOpt.get();
        task.setAttemptCount(task.getAttemptCount() + 1);
        task.setLastResponseCode(statusCode);
        task.setLastResponseMessage(responseSummary);
        task.setDispatchStatus(status);
        if (deliveredAt != null) {
            task.setDeliveredAt(deliveredAt);
        }

        dispatchTaskRepository.save(task);
    }

    private String extractDomain(String targetUrl) throws URISyntaxException {
        if (targetUrl == null || targetUrl.isBlank()) {
            throw new IllegalArgumentException("Target URL cannot be null or empty");
        }

        URI uri = new URI(targetUrl);
        String domain = uri.getHost();

        if (domain == null) {
            throw new URISyntaxException(targetUrl, "Could not extract domain");
        }

        if (domain.startsWith("www.")) {
            domain = domain.substring(4);
        }

        return domain;
    }

    private @Nullable DispatchTask getDispatchTask(UUID dispatchTaskId) {
        Optional<DispatchTask> dispatchTaskOpt = dispatchTaskRepository.findById(dispatchTaskId);
        if (dispatchTaskOpt.isEmpty()) {
            log.error("Dispatch task not found for dispatchTaskId {}", dispatchTaskId);
            return null;
        }

        return dispatchTaskOpt.get();
    }
}