package com.project.webhookengine.model;

import jakarta.persistence.*;

import lombok.*;

import java.util.UUID;

@Getter
@Setter
@Entity
@Table(name = "webhook_subscriptions")
@AllArgsConstructor
@NoArgsConstructor
public class WebhookSubscription {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID webhookSubscriptionId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "tenant_id", nullable = false)
    private Tenant tenant;

    @Column(nullable = false)
    private String eventType;

    @Column(nullable = false)
    private String targetUrl;
    
    @Column(nullable = false)
    private String secretKey;

    @Column(nullable = false)
    private Boolean isActive = true;



}
