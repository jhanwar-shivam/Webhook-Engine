package com.project.webhookengine.controller;


import com.project.webhookengine.dto.DispatchRequestDTO;
import com.project.webhookengine.service.WebhookIngestionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;


@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1")
public class WebhookIngestionController {
    private final WebhookIngestionService webhookIngestionService;

    @PostMapping("events/dispatch")
    public ResponseEntity<Void> dispatchEvent(@RequestAttribute("tenantId") UUID tenantId, @Valid @RequestBody DispatchRequestDTO dispatchRequestDTO) {
        webhookIngestionService.processEvent(tenantId, dispatchRequestDTO);
        return ResponseEntity.accepted().build();
    }
}
