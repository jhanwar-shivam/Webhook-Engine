package com.project.webhookengine.worker;

import com.project.webhookengine.config.WebhookProperties;
import com.project.webhookengine.dto.WebhookDeliveryResult;
import com.project.webhookengine.messaging.DispatchMessagePublisher;
import com.project.webhookengine.model.DispatchStatus;
import com.project.webhookengine.model.DispatchTask;
import com.project.webhookengine.repository.DispatchTaskRepository;
import com.project.webhookengine.retry.RetryDecision;
import com.project.webhookengine.service.RetryPolicyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DispatchTaskService {

    private static final List<DispatchStatus> CLAIMABLE_STATUSES = List.of(
            DispatchStatus.PENDING,
            DispatchStatus.RETRYING
    );

    private final DispatchTaskRepository dispatchTaskRepository;
    private final RetryPolicyService retryPolicyService;
    private final DispatchMessagePublisher dispatchMessagePublisher;
    private final WebhookProperties webhookProperties;

    @Transactional
    public boolean tryClaim(UUID dispatchTaskId) {
        int updated = dispatchTaskRepository.claimForProcessing(
                dispatchTaskId,
                DispatchStatus.IN_PROGRESS,
                CLAIMABLE_STATUSES
        );
        return updated > 0;
    }

    @Transactional(readOnly = true)
    public Optional<DispatchTask> findDetailedById(UUID dispatchTaskId) {
        return dispatchTaskRepository.findDetailedById(dispatchTaskId);
    }

    @Transactional
    public void updateDeliveryOutcome(UUID dispatchTaskId, WebhookDeliveryResult result) {
        DispatchTask dispatchTask = dispatchTaskRepository.findDetailedById(dispatchTaskId).orElse(null);
        if (dispatchTask == null) {
            log.error("Dispatch task not found for dispatchTaskId={}", dispatchTaskId);
            return;
        }

        dispatchTask.setThrottleCount(0);
        dispatchTask.setAttemptCount(dispatchTask.getAttemptCount() + 1);
        dispatchTask.setLastResponseCode(result.statusCode());
        dispatchTask.setLastResponseMessage(result.responseSummary());

        String partitionKey = partitionKey(dispatchTask);

        if (result.isSuccessful()) {
            dispatchTask.setDispatchStatus(DispatchStatus.DELIVERED);
            dispatchTask.setDeliveredAt(result.deliveredAt());
            dispatchTask.setNextRetryAt(null);
            dispatchTaskRepository.save(dispatchTask);
            log.info("Task {} delivered successfully on attempt {}", dispatchTaskId, dispatchTask.getAttemptCount());
            return;
        }

        RetryDecision decision = retryPolicyService.determineNextStep(dispatchTask.getAttemptCount());
        if (decision.isExhausted()) {
            dispatchTask.setDispatchStatus(DispatchStatus.DEAD_LETTERED);
            dispatchTask.setNextRetryAt(null);
        } else {
            dispatchTask.setDispatchStatus(DispatchStatus.RETRYING);
            dispatchTask.setNextRetryAt(Instant.now().plusSeconds(decision.delaySeconds()));
        }
        dispatchTaskRepository.save(dispatchTask);
        dispatchMessagePublisher.publishAfterCommit(decision.targetTopic(), partitionKey, dispatchTaskId.toString());

        log.warn("Task {} failed (attempt {}). Routed to topic: {}",
                dispatchTaskId, dispatchTask.getAttemptCount(), decision.targetTopic());
    }

    @Transactional
    public void handleRateLimitExceeded(UUID dispatchTaskId) {
        DispatchTask task = dispatchTaskRepository.findDetailedById(dispatchTaskId).orElse(null);
        if (task == null) {
            return;
        }

        int throttleCount = task.getThrottleCount() + 1;
        task.setThrottleCount(throttleCount);
        task.setLastResponseMessage("Throttled: Outbound domain rate limit exceeded");

        String partitionKey = partitionKey(task);
        int maxThrottleRetries = webhookProperties.retry().maxThrottleRetries();

        if (throttleCount > maxThrottleRetries) {
            task.setDispatchStatus(DispatchStatus.DEAD_LETTERED);
            task.setNextRetryAt(null);
            dispatchTaskRepository.save(task);
            dispatchMessagePublisher.publishAfterCommit(
                    retryPolicyService.deadLetterTopic(),
                    partitionKey,
                    dispatchTaskId.toString()
            );
            log.error("Task {} exceeded throttle retries ({}). Sent to DLQ.", dispatchTaskId, maxThrottleRetries);
            return;
        }

        task.setDispatchStatus(DispatchStatus.RETRYING);
        task.setNextRetryAt(Instant.now().plusSeconds(10));
        dispatchTaskRepository.save(task);
        dispatchMessagePublisher.publishAfterCommit(
                retryPolicyService.retryTopic10Seconds(),
                partitionKey,
                dispatchTaskId.toString()
        );
    }

    private String partitionKey(DispatchTask dispatchTask) {
        return dispatchTask.getWebhookSubscription().getTenant().getTenantId().toString();
    }
}
