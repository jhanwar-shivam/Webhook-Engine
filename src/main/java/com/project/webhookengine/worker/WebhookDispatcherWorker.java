package com.project.webhookengine.worker;

import com.project.webhookengine.config.WebhookProperties;
import com.project.webhookengine.dto.WebhookDeliveryResult;
import com.project.webhookengine.dto.WebhookDispatchContext;
import com.project.webhookengine.model.DispatchTask;
import com.project.webhookengine.model.WebhookEvent;
import com.project.webhookengine.model.WebhookSubscription;
import com.project.webhookengine.utils.RateLimitUtil;
import com.project.webhookengine.utils.UrlDomainExtractor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.net.URISyntaxException;
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
    private final UrlDomainExtractor urlDomainExtractor;
    private final WebhookProperties webhookProperties;

    private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    @KafkaListener(
            topics = "${webhook.kafka.topics.dispatch}",
            groupId = "webhook-engine-group",
            concurrency = "${webhook.kafka.dispatch-concurrency}"
    )
    public void consumeDispatchTask(String dispatchTaskIdString) {
        UUID dispatchTaskId = UUID.fromString(dispatchTaskIdString);

        if (!dispatchTaskService.tryClaim(dispatchTaskId)) {
            log.debug("Skipping task {} — not claimable (duplicate or already in progress).", dispatchTaskId);
            return;
        }

        Optional<DispatchTask> dispatchTaskOpt = dispatchTaskService.findDetailedById(dispatchTaskId);
        if (dispatchTaskOpt.isEmpty()) {
            log.error("Dispatch task not found after claim for dispatchTaskId: {}", dispatchTaskId);
            return;
        }

        WebhookDispatchContext context = toDispatchContext(dispatchTaskOpt.get());

        try {
            String domain = urlDomainExtractor.extractDomain(context.targetUrl());
            WebhookProperties.OutboundRateLimit outbound = webhookProperties.outboundRateLimit();
            if (rateLimitUtil.checkOutboundRateLimit(domain, outbound.bucketCapacity(), outbound.refillPerSecond())) {
                executeDispatch(context);
            } else {
                log.warn("Rate limit exceeded for domain: {}. Task {} delayed.", domain, dispatchTaskId);
                dispatchTaskService.handleRateLimitExceeded(dispatchTaskId);
            }
        } catch (URISyntaxException | IllegalArgumentException e) {
            log.error("Invalid target URL for task {}. URL: {}", dispatchTaskId, context.targetUrl());
            dispatchTaskService.updateDeliveryOutcome(
                    dispatchTaskId,
                    new WebhookDeliveryResult(0, "Invalid target URL: " + e.getMessage(), false, null)
            );
        }
    }

    private WebhookDispatchContext toDispatchContext(DispatchTask task) {
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

    private void executeDispatch(WebhookDispatchContext context) {
        virtualThreadExecutor.submit(() -> {
            try {
                WebhookDeliveryResult result = webhookHttpClient.send(context);
                dispatchTaskService.updateDeliveryOutcome(context.dispatchTaskId(), result);
            } catch (Exception e) {
                log.error("Dispatch failed unexpectedly for task {}", context.dispatchTaskId(), e);
                dispatchTaskService.updateDeliveryOutcome(
                        context.dispatchTaskId(),
                        new WebhookDeliveryResult(0, "Dispatch execution error: " + e.getMessage(), false, null)
                );
            }
        });
    }
}
