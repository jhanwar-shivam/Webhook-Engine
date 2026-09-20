package com.project.webhookengine.repository;

import com.project.webhookengine.model.DispatchTask;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface DispatchTaskRepository extends JpaRepository<DispatchTask, UUID> {
}
