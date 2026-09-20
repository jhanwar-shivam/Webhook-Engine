package com.project.webhookengine.service;

import com.project.webhookengine.model.*;
import com.project.webhookengine.repository.DispatchTaskRepository;
import com.project.webhookengine.repository.TenantRepository;
import com.project.webhookengine.dto.DispatchRequestDTO;
import com.project.webhookengine.repository.WebhookEventRepository;
import com.project.webhookengine.repository.WebhookSubscriptionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class WebhookIngestionService {
    private final TenantRepository tenantRepository;
    private final WebhookEventRepository webhookEventRepository;
    private final WebhookSubscriptionRepository webhookSubscriptionRepository;
    private final DispatchTaskRepository dispatchTaskRepository;

    private final KafkaTemplate<String, String> kafkaTemplate;
    @Transactional
    public void processEvent(UUID tenantId, DispatchRequestDTO dispatchRequestDTO) {
        try {
            Tenant tenantRef = tenantRepository.getReferenceById(tenantId);
            WebhookEvent webhookEvent = saveWebhookEvent(dispatchRequestDTO, tenantRef);

            List<WebhookSubscription> subscriptionList = webhookSubscriptionRepository
                    .findByTenantAndEventTypeAndIsActiveTrue(tenantRef, dispatchRequestDTO.eventType());

            List<DispatchTask> taskToDispatch = new ArrayList<>();
            for (WebhookSubscription subscription : subscriptionList) {
                taskToDispatch.add(createDispatchTask(subscription, webhookEvent));
            }
            dispatchTaskRepository.saveAll(taskToDispatch);

            for (DispatchTask task : taskToDispatch) {
                kafkaTemplate.send("webhooks.dispatch", tenantId.toString(), task.getDispatchTaskId().toString());
            }

        } catch (DataIntegrityViolationException e) {
            System.out.println("Duplicate event received for key: " + dispatchRequestDTO.idempotencyKey() + ". Ignoring.");
        }
    }

    private DispatchTask createDispatchTask(WebhookSubscription subscription, WebhookEvent webhookEvent) {
        DispatchTask dispatchTask = new DispatchTask();
        dispatchTask.setWebhookEvent(webhookEvent);
        dispatchTask.setWebhookSubscription(subscription);
        dispatchTask.setDispatchStatus(DispatchStatus.PENDING);
        dispatchTask.setAttemptCount(0);

        return dispatchTask;
    }

    private WebhookEvent saveWebhookEvent(DispatchRequestDTO dispatchRequestDTO, Tenant tenantRef) {
        WebhookEvent webhookEvent = new WebhookEvent();
        webhookEvent.setTenant(tenantRef);
        webhookEvent.setEventType(dispatchRequestDTO.eventType());
        webhookEvent.setPayload(dispatchRequestDTO.payload().toString());
        webhookEvent.setIdempotencyKey(dispatchRequestDTO.idempotencyKey());

        webhookEventRepository.save(webhookEvent);
        return  webhookEvent;
    }
}
