package com.project.webhookengine.repository;

import com.project.webhookengine.model.WebhookEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface WebhookEventRepository extends JpaRepository<WebhookEvent, UUID> {
}
