package com.usig.domain.run;

import java.time.Instant;
import java.util.Objects;

public record Run(
    RunId id,
    ConversationId conversationId,
    TenantId tenantId,
    String serviceId,
    RunStatus status,
    ExecutionHandle executionHandle,
    long version,
    Instant createdAt,
    Instant updatedAt) {

  public Run {
    id = Objects.requireNonNull(id, "id must not be null");
    conversationId = Objects.requireNonNull(conversationId, "conversationId must not be null");
    tenantId = Objects.requireNonNull(tenantId, "tenantId must not be null");
    serviceId = requireText(serviceId, "serviceId");
    status = Objects.requireNonNull(status, "status must not be null");
    if (version < 0) {
      throw new IllegalArgumentException("version must not be negative");
    }
    createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
    updatedAt = Objects.requireNonNull(updatedAt, "updatedAt must not be null");
  }

  public Run transitionTo(RunStatus next, Instant now) {
    return new Run(
        id,
        conversationId,
        tenantId,
        serviceId,
        status.transitionTo(next),
        executionHandle,
        version + 1,
        createdAt,
        Objects.requireNonNull(now, "now must not be null"));
  }

  public Run withExecutionHandle(ExecutionHandle handle, Instant now) {
    return new Run(
        id,
        conversationId,
        tenantId,
        serviceId,
        status,
        Objects.requireNonNull(handle, "handle must not be null"),
        version + 1,
        createdAt,
        Objects.requireNonNull(now, "now must not be null"));
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
