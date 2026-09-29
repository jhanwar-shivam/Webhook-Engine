package com.project.webhookengine.repository;

import com.project.webhookengine.model.DispatchStatus;
import com.project.webhookengine.model.DispatchTask;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

public interface DispatchTaskRepository extends JpaRepository<DispatchTask, UUID> {

    @Query("""
            SELECT t FROM DispatchTask t
            JOIN FETCH t.webhookEvent
            JOIN FETCH t.webhookSubscription s
            JOIN FETCH s.tenant
            WHERE t.dispatchTaskId = :dispatchTaskId
            """)
    Optional<DispatchTask> findDetailedById(@Param("dispatchTaskId") UUID dispatchTaskId);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE DispatchTask t
            SET t.dispatchStatus = :inProgress
            WHERE t.dispatchTaskId = :dispatchTaskId
              AND t.dispatchStatus IN :claimableStatuses
            """)
    int claimForProcessing(
            @Param("dispatchTaskId") UUID dispatchTaskId,
            @Param("inProgress") DispatchStatus inProgress,
            @Param("claimableStatuses") Collection<DispatchStatus> claimableStatuses
    );
}
