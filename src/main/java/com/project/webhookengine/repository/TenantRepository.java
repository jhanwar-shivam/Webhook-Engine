package com.project.webhookengine.repository;

import com.project.webhookengine.model.Tenant;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface TenantRepository extends JpaRepository<Tenant, UUID> {
    Optional<Tenant> findByApiKeyHash(String apiKeyHash);
}
