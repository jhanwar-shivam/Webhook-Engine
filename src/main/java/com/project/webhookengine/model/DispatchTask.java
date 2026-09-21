package com.project.webhookengine.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name="dispatch_tasks")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class DispatchTask {
    @Id
    @Column(columnDefinition = "uuid", nullable = false, updatable = false)
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID dispatchTaskId;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "webhook_event_id", nullable = false)
    private WebhookEvent webhookEvent;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "webhook_subscription_id", nullable = false)
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

    private Instant deliveredAt;
}
