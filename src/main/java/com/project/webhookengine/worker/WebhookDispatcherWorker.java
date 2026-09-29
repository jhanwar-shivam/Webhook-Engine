package com.project.webhookengine.worker;

import com.project.webhookengine.dto.WebhookDeliveryResult;
import com.project.webhookengine.dto.WebhookDispatchContext;
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

    private final DispatchTaskService dispatchTaskService;
    private final WebhookHttpClient webhookHttpClient;
    private final RateLimitUtil rateLimitUtil;


    private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    private static final Integer CAPACITY = 600;
    private static final Integer REFILL = 10;

    @KafkaListener(topics = "webhooks.dispatch", groupId = "webhook-engine-group")
    @Transactional(readOnly = true)
    public void consumeDispatchTask(String dispatchTaskIdString) {
        UUID dispatchTaskId = UUID.fromString(dispatchTaskIdString);

        Optional<DispatchTask> dispatchTaskOpt = dispatchTaskService.findById(dispatchTaskId);
        if (dispatchTaskOpt.isEmpty()) {
            log.error("dispatchTask not found for dispatchTaskId: {}", dispatchTaskId);
            return;
        }
        DispatchTask task = dispatchTaskOpt.get();
        WebhookDispatchContext context = getDispatchContext(task);

        try {
            String domain = extractDomain(context.targetUrl());
            if (rateLimitUtil.checkOutboundRateLimit(domain, CAPACITY, REFILL)) {
                executeAsyncDispatch(context);
            } else {
                log.warn("Rate limit exceeded for domain: {}. Task {} delayed.", domain, dispatchTaskId);
                // Phase 6 will handle delayed retry queueing here
            }
        } catch (URISyntaxException e) {
            log.error("Invalid Target URL for task {}. URL: {}", dispatchTaskId, context.targetUrl());
        }
    }

    private WebhookDispatchContext getDispatchContext(DispatchTask task) {
        WebhookSubscription subscription = task.getWebhookSubscription();
        WebhookEvent event = task.getWebhookEvent();

        return new WebhookDispatchContext(
                task.getDispatchTaskId(),
                subscription.getTargetUrl(),
                subscription.getSecretKey(),
                event.getPayload(),
                event.getWebhookEventId().toString(),
                event.getEventType()
        );
    }

    private void executeAsyncDispatch(WebhookDispatchContext context) {
        virtualThreadExecutor.submit(() -> {
            WebhookDeliveryResult result = webhookHttpClient.send(context);
            dispatchTaskService.updateDeliveryOutcome(context.dispatchTaskId(),result);
        });
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
}