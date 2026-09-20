package com.project.webhookengine.model;

import jakarta.persistence.*;
import lombok.Data;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "tenants")
@Data

public class Tenant {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false, unique = true)
    private String apiKeyHash;
    @Column(nullable = false)
    private Integer maxRequestsPerSecond;

    @Column(nullable = false, updatable = false)
    private Instant createdAt = Instant.now();
}
