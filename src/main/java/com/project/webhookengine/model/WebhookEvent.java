package com.project.webhookengine.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;


import java.time.Instant;
import java.util.UUID;

@Entity
@Getter
@Setter
@Table(name = "webhook_events")
@AllArgsConstructor
@NoArgsConstructor
public class WebhookEvent {
    @Id
    @Column(columnDefinition = "uuid", nullable = false, updatable = false)
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID webhookEventId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "tenant_id")
    private Tenant tenant;

    @Column(nullable = false)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false)
    private String payload;

    @Column(nullable = false, unique = true)
    private String idempotencyKey;

    @Column(nullable = false, updatable = false)
    private Instant createdAt = Instant.now();
}
