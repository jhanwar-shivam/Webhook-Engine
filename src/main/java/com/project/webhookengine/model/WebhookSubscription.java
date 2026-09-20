package com.project.webhookengine.model;

import jakarta.persistence.*;

import lombok.Data;

import java.util.UUID;

@Data
@Entity
@Table(name = "webhook_subscriptions")
public class WebhookSubscription {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "id", nullable = false)
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
