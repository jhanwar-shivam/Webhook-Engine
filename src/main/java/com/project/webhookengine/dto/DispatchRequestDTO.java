package com.project.webhookengine.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import tools.jackson.databind.JsonNode;

public record DispatchRequestDTO(

        @NotBlank(message = "Event type cannot be blank")
        @Size(max = 100, message = "Event type is too long")
        String eventType,

        @NotBlank(message = "Idempotency key cannot be blank")
        @Size(max = 100, message = "Idempotency key is too long")
        String idempotencyKey,

        @NotNull(message = "Payload cannot be null")
        JsonNode payload
) {}
