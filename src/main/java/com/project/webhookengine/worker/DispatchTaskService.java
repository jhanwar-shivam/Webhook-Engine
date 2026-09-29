package com.project.webhookengine.worker;

import com.project.webhookengine.dto.WebhookDeliveryResult;
import com.project.webhookengine.model.DispatchStatus;
import com.project.webhookengine.model.DispatchTask;
import com.project.webhookengine.repository.DispatchTaskRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DispatchTaskService {
    private final DispatchTaskRepository dispatchTaskRepository;

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
        } else {
            dispatchTask.setDispatchStatus(DispatchStatus.RETRYING);
            //
        }
        dispatchTaskRepository.save(dispatchTask);
    }
}
