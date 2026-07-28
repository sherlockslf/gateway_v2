package com.usig.infra.postgres;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface PostgresRunCommands {

  CompletionStage<PostgresRunRecord> insertOrFind(PostgresRunRecord record, String idempotencyKey);

  CompletionStage<Optional<PostgresRunRecord>> findByIdAndTenant(String runId, String tenantId);

  CompletionStage<Optional<PostgresRunRecord>> compareAndSetStatus(
      String runId, long expectedVersion, String nextStatus, Instant updatedAt);

  CompletionStage<Optional<PostgresRunRecord>> saveExecutionHandle(
      String runId, PostgresExecutionHandleRecord handle, Instant updatedAt);
}
