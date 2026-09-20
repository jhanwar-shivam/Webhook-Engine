package com.project.webhookengine.model;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name="dispatch_tasks")
public class DispatchTask {
    @Id
    @Column(columnDefinition = "uuid", nullable = false, updatable = false)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "id", nullable = false)
    private WebhookEvent webhookEvent;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "id", nullable = false)
    private WebhookSubscription webhookSubscription;
    
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private DispatchStatus dispatchStatus;

    @Column(nullable = false)
    private Integer attemptCount = 0;

    private Instant nextRetryAt;

    private Integer lastResponseCode;
    
    @Column(columnDefinition = "text")
    private String lastResponseMessage;
}
