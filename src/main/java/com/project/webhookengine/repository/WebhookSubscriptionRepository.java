package com.project.webhookengine.repository;

import com.project.webhookengine.model.Tenant;
import com.project.webhookengine.model.WebhookSubscription;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface WebhookSubscriptionRepository extends JpaRepository<WebhookSubscription, UUID> {
    List<WebhookSubscription> findByTenantAndEventTypeAndIsActiveTrue(Tenant tenant, String eventType);
}
