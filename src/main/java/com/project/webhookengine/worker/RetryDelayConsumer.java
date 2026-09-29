package com.project.webhookengine.worker;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class RetryDelayConsumer {

    private static final String MAIN_DISPATCH_TOPIC = "webhooks.dispatch";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ScheduledExecutorService delayScheduler = Executors.newScheduledThreadPool(2);

    @KafkaListener(
            topics = "webhooks.retry.10s",
            groupId = "webhook-retry-10s-group",
            concurrency = "2"
    )
    public void handleRetry10s(
            @Payload String dispatchTaskId,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String partitionKey) {
        processDelayedRetry(dispatchTaskId, partitionKey, Duration.ofSeconds(10));
    }

    @KafkaListener(
            topics = "webhooks.retry.1m",
            groupId = "webhook-retry-1m-group",
            concurrency = "2"
    )
    public void handleRetry1m(
            @Payload String dispatchTaskId,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String partitionKey) {
        processDelayedRetry(dispatchTaskId, partitionKey, Duration.ofMinutes(1));
    }

    @KafkaListener(
            topics = "webhooks.retry.5m",
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

        delayScheduler.schedule(() -> {
            try {
                log.info("Delay elapsed. Re-enqueuing task {} to {}", dispatchTaskId, MAIN_DISPATCH_TOPIC);
                kafkaTemplate.send(MAIN_DISPATCH_TOPIC, partitionKey, dispatchTaskId);
            } catch (Exception e) {
                log.error("Failed to re-enqueue task {} to {}", dispatchTaskId, MAIN_DISPATCH_TOPIC, e);
            }
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    @KafkaListener(
            topics = "webhooks.dlq",
            groupId = "webhook-dlq-group"
    )
    public void handleDeadLetter(@Payload String dispatchTaskId) {
        log.error("CRITICAL: Webhook task {} exhausted all retry attempts and entered DLQ.", dispatchTaskId);
    }
}