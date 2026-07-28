package com.usig.infra.postgres;

import com.usig.domain.run.ConversationId;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunId;
import com.usig.domain.run.RunStatus;
import com.usig.domain.run.TenantId;
import java.time.Instant;
import java.util.Objects;

public record PostgresRunRecord(
    String runId,
    String conversationId,
    String tenantId,
    String serviceId,
    String status,
    PostgresExecutionHandleRecord executionHandle,
    long version,
    Instant createdAt,
    Instant updatedAt) {

  public PostgresRunRecord {
    runId = requireText(runId, "runId");
    conversationId = requireText(conversationId, "conversationId");
    tenantId = requireText(tenantId, "tenantId");
    serviceId = requireText(serviceId, "serviceId");
    status = requireText(status, "status");
    if (version < 0) {
      throw new IllegalArgumentException("version must not be negative");
    }
    createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
    updatedAt = Objects.requireNonNull(updatedAt, "updatedAt must not be null");
  }

  static PostgresRunRecord fromDomain(Run run) {
    return new PostgresRunRecord(
        run.id().value(),
        run.conversationId().value(),
        run.tenantId().value(),
        run.serviceId(),
        run.status().name(),
        PostgresExecutionHandleRecord.fromDomain(run.executionHandle()),
        run.version(),
        run.createdAt(),
        run.updatedAt());
  }

  Run toDomain() {
    return new Run(
        new RunId(runId),
        new ConversationId(conversationId),
        new TenantId(tenantId),
        serviceId,
        RunStatus.valueOf(status),
        executionHandle == null ? null : executionHandle.toDomain(),
        version,
        createdAt,
        updatedAt);
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
