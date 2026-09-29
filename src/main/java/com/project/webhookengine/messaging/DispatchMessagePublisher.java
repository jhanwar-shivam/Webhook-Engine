package com.project.webhookengine.messaging;

import com.project.webhookengine.config.WebhookProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
@RequiredArgsConstructor
public class DispatchMessagePublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final WebhookProperties webhookProperties;

    public void publishDispatchAfterCommit(String partitionKey, String dispatchTaskId) {
        publishAfterCommit(
                webhookProperties.kafka().topics().dispatch(),
                partitionKey,
                dispatchTaskId
        );
    }

    public void publishAfterCommit(String topic, String partitionKey, String dispatchTaskId) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    kafkaTemplate.send(topic, partitionKey, dispatchTaskId);
                }
            });
        } else {
            kafkaTemplate.send(topic, partitionKey, dispatchTaskId);
        }
    }

    public void publishImmediately(String topic, String partitionKey, String dispatchTaskId) {
        kafkaTemplate.send(topic, partitionKey, dispatchTaskId);
    }
}
