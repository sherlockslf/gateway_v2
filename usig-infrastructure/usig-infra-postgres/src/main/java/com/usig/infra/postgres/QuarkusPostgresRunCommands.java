package com.usig.infra.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public final class QuarkusPostgresRunCommands implements PostgresRunCommands {

  static final String SELECT_BY_ID_AND_TENANT =
      """
      select run_id, conversation_id, tenant_id, service_id, status,
             execution_handle::text as execution_handle,
             version, created_at, updated_at
        from runs
       where run_id = $1
         and tenant_id = $2
      """;

  static final String SELECT_BY_IDEMPOTENCY =
      """
      select run_id, conversation_id, tenant_id, service_id, status,
             execution_handle::text as execution_handle,
             version, created_at, updated_at
        from runs
       where tenant_id = $1
         and service_id = $2
         and idempotency_key = $3
      """;

  static final String INSERT_RUN =
      """
      insert into runs (
        run_id, conversation_id, tenant_id, service_id, adapter_id, status,
        execution_handle, idempotency_key, version, created_at, updated_at
      ) values (
        $1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11
      )
      returning run_id, conversation_id, tenant_id, service_id, status,
                execution_handle::text as execution_handle,
                version, created_at, updated_at
      """;

  static final String COMPARE_AND_SET_STATUS =
      """
      update runs
         set status = $3,
             version = version + 1,
             updated_at = $4
       where run_id = $1
         and version = $2
      returning run_id, conversation_id, tenant_id, service_id, status,
                execution_handle::text as execution_handle,
                version, created_at, updated_at
      """;

  static final String SAVE_EXECUTION_HANDLE =
      """
      update runs
         set execution_handle = $2::jsonb,
             adapter_id = $3,
             version = version + 1,
             updated_at = $4
       where run_id = $1
      returning run_id, conversation_id, tenant_id, service_id, status,
                execution_handle::text as execution_handle,
                version, created_at, updated_at
      """;

  private static final String UNKNOWN_ADAPTER_ID = "unknown";

  private final PgRunClient client;
  private final PostgresExecutionHandleJson executionHandleJson;

  QuarkusPostgresRunCommands(PgRunClient client, ObjectMapper objectMapper) {
    this.client = Objects.requireNonNull(client, "client must not be null");
    this.executionHandleJson =
        new PostgresExecutionHandleJson(
            Objects.requireNonNull(objectMapper, "objectMapper must not be null"));
  }

  public QuarkusPostgresRunCommands(QuarkusPgRunClient client) {
    this(client, new ObjectMapper());
  }

  @Override
  public CompletionStage<PostgresRunRecord> insertOrFind(
      PostgresRunRecord record, String idempotencyKey) {
    if (idempotencyKey == null) {
      return insert(record, null);
    }
    return client
        .query(
            SELECT_BY_IDEMPOTENCY, List.of(record.tenantId(), record.serviceId(), idempotencyKey))
        .thenCompose(
            rows ->
                rows.isEmpty()
                    ? insert(record, idempotencyKey)
                    : completed(toRecord(rows.getFirst())));
  }

  @Override
  public CompletionStage<Optional<PostgresRunRecord>> findByIdAndTenant(
      String runId, String tenantId) {
    return client
        .query(SELECT_BY_ID_AND_TENANT, List.of(runId, tenantId))
        .thenApply(rows -> rows.stream().findFirst().map(this::toRecord));
  }

  @Override
  public CompletionStage<Optional<PostgresRunRecord>> compareAndSetStatus(
      String runId, long expectedVersion, String nextStatus, Instant updatedAt) {
    return client
        .query(COMPARE_AND_SET_STATUS, List.of(runId, expectedVersion, nextStatus, updatedAt))
        .thenApply(rows -> rows.stream().findFirst().map(this::toRecord));
  }

  @Override
  public CompletionStage<Optional<PostgresRunRecord>> saveExecutionHandle(
      String runId, PostgresExecutionHandleRecord handle, Instant updatedAt) {
    return client
        .query(
            SAVE_EXECUTION_HANDLE,
            Arrays.asList(
                runId, executionHandleJson.serialize(handle), handle.adapterId(), updatedAt))
        .thenApply(rows -> rows.stream().findFirst().map(this::toRecord));
  }

  private CompletionStage<PostgresRunRecord> insert(
      PostgresRunRecord record, String idempotencyKey) {
    String handleJson = executionHandleJson.serialize(record.executionHandle());
    return client
        .query(
            INSERT_RUN,
            Arrays.asList(
                record.runId(),
                record.conversationId(),
                record.tenantId(),
                record.serviceId(),
                record.executionHandle() == null
                    ? UNKNOWN_ADAPTER_ID
                    : record.executionHandle().adapterId(),
                record.status(),
                handleJson,
                idempotencyKey,
                record.version(),
                record.createdAt(),
                record.updatedAt()))
        .thenApply(rows -> toRecord(rows.getFirst()));
  }

  private PostgresRunRecord toRecord(PgRunRow row) {
    return new PostgresRunRecord(
        row.runId(),
        row.conversationId(),
        row.tenantId(),
        row.serviceId(),
        row.status(),
        executionHandleJson.deserialize(row.executionHandleJson()),
        row.version(),
        row.createdAt(),
        row.updatedAt());
  }

  private static CompletionStage<PostgresRunRecord> completed(PostgresRunRecord record) {
    return java.util.concurrent.CompletableFuture.completedFuture(record);
  }
}
