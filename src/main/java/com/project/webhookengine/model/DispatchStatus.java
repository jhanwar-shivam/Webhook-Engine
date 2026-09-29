package com.project.webhookengine.model;

public enum DispatchStatus {
    PENDING,
    IN_PROGRESS,
    RETRYING,
    DELIVERED,
    DEAD_LETTERED
}
