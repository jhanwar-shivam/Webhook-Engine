package com.project.webhookengine.worker;

import com.project.webhookengine.dto.WebhookDeliveryResult;
import com.project.webhookengine.model.DispatchStatus;
import com.project.webhookengine.model.DispatchTask;
import com.project.webhookengine.repository.DispatchTaskRepository;
import com.project.webhookengine.retry.RetryDecision;
import com.project.webhookengine.service.RetryPolicyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DispatchTaskService {
    private final DispatchTaskRepository dispatchTaskRepository;
    private final RetryPolicyService retryPolicyService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Transactional(readOnly = true)
    public Optional<DispatchTask> findById(UUID dispatchTaskId) {
        return dispatchTaskRepository.findById(dispatchTaskId);
    }

    @Transactional
    public void updateDeliveryOutcome(UUID dispatchTaskId, WebhookDeliveryResult result) {
        Optional<DispatchTask> dispatchTaskOpt = dispatchTaskRepository.findById(dispatchTaskId);
        if (dispatchTaskOpt.isEmpty()) {
            log.error("Dispatch task not found for dispatchTaskId={}", dispatchTaskId);
            return;
        }

        DispatchTask dispatchTask = dispatchTaskOpt.get();
        dispatchTask.setAttemptCount(dispatchTask.getAttemptCount() + 1);
        dispatchTask.setLastResponseCode(result.statusCode());
        dispatchTask.setLastResponseMessage(result.responseSummary());

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
        }
        dispatchTaskRepository.save(dispatchTask);
        String partitionKey = dispatchTask.getWebhookSubscription()
                .getTenant()
                .getTenantId()
                .toString();

        kafkaTemplate.send(decision.targetTopic(), partitionKey, dispatchTaskId.toString());

        log.warn("Task {} failed (attempt {}). Routed to topic: {}",
                dispatchTaskId, dispatchTask.getAttemptCount(), decision.targetTopic());
    }

    // Inside DispatchTaskService:
    @Transactional
    public void handleRateLimitExceeded(UUID dispatchTaskId) {
        Optional<DispatchTask> taskOpt = dispatchTaskRepository.findById(dispatchTaskId);
        if (taskOpt.isEmpty()) return;

        DispatchTask task = taskOpt.get();
        task.setDispatchStatus(DispatchStatus.RETRYING);
        task.setNextRetryAt(Instant.now().plusSeconds(10));
        task.setLastResponseMessage("Throttled: Outbound domain rate limit exceeded");
        dispatchTaskRepository.save(task);

        String partitionKey = task.getWebhookSubscription().getTenant().getTenantId().toString();
        kafkaTemplate.send(RetryPolicyService.TOPIC_RETRY_10S, partitionKey, dispatchTaskId.toString());
    }
}
