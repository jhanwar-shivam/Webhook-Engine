package com.project.webhookengine.worker;

import com.project.webhookengine.config.WebhookProperties;
import com.project.webhookengine.messaging.DispatchMessagePublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;

@Slf4j
@Component
@RequiredArgsConstructor
public class RetryDelayConsumer {

    private final DispatchMessagePublisher dispatchMessagePublisher;
    private final WebhookProperties webhookProperties;
    private final ScheduledExecutorService delayScheduler = Executors.newScheduledThreadPool(2);

    @KafkaListener(
            topics = "${webhook.kafka.topics.retry-10s}",
            groupId = "webhook-retry-10s-group",
            concurrency = "2"
    )
    public void handleRetry10s(
            @Payload String dispatchTaskId,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String partitionKey) {
        processDelayedRetry(dispatchTaskId, partitionKey, Duration.ofSeconds(10));
    }

    @KafkaListener(
            topics = "${webhook.kafka.topics.retry-1m}",
            groupId = "webhook-retry-1m-group",
            concurrency = "2"
    )
    public void handleRetry1m(
            @Payload String dispatchTaskId,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String partitionKey) {
        processDelayedRetry(dispatchTaskId, partitionKey, Duration.ofMinutes(1));
    }

    @KafkaListener(
            topics = "${webhook.kafka.topics.retry-5m}",
            groupId = "webhook-retry-5m-group",
            concurrency = "2"
    )
    public void handleRetry5m(
            @Payload String dispatchTaskId,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String partitionKey) {
        processDelayedRetry(dispatchTaskId, partitionKey, Duration.ofMinutes(5));
    }

    private void processDelayedRetry(String dispatchTaskId, String partitionKey, Duration delay) {
        log.info("Scheduling task {} for re-dispatch in {}s", dispatchTaskId, delay.toSeconds());

        String dispatchTopic = webhookProperties.kafka().topics().dispatch();
        delayScheduler.schedule(() -> {
            try {
                log.info("Delay elapsed. Re-enqueuing task {} to {}", dispatchTaskId, dispatchTopic);
                dispatchMessagePublisher.publishImmediately(dispatchTopic, partitionKey, dispatchTaskId);
            } catch (Exception e) {
                log.error("Failed to re-enqueue task {} to {}", dispatchTaskId, dispatchTopic, e);
            }
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    @KafkaListener(
            topics = "${webhook.kafka.topics.dlq}",
            groupId = "webhook-dlq-group"
    )
    public void handleDeadLetter(@Payload String dispatchTaskId) {
        log.error("CRITICAL: Webhook task {} exhausted all retry attempts and entered DLQ.", dispatchTaskId);
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down RetryDelayConsumer scheduler...");
        delayScheduler.shutdown();
        try {
            if (!delayScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Scheduler did not terminate in time, forcing shutdown.");
                delayScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            delayScheduler.shutdownNow();
        }
        log.info("RetryDelayConsumer scheduler shut down cleanly.");
    }
}
