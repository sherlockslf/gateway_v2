package com.usig.infra.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.usig.application.memory.FixedClockPort;
import com.usig.domain.run.ConversationId;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunId;
import com.usig.domain.run.RunStatus;
import com.usig.domain.run.TenantId;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class PostgresRunRepositoryTest {

  private static final Instant NOW = Instant.parse("2026-07-27T06:40:00Z");

  @Test
  void createsAndFindsRunWithinTenant() {
    FakePostgresRunCommands commands = new FakePostgresRunCommands();
    PostgresRunRepository repository = new PostgresRunRepository(commands, new FixedClockPort(NOW));
    Run run = run("run_1", "tenant_1", RunStatus.ACCEPTED, 0);

    Run created = repository.create(run, "idem_1").toCompletableFuture().join();
    Optional<Run> found =
        repository.find(new RunId("run_1"), new TenantId("tenant_1")).toCompletableFuture().join();
    Optional<Run> otherTenant =
        repository.find(new RunId("run_1"), new TenantId("tenant_2")).toCompletableFuture().join();

    assertEquals(run, created);
    assertEquals(run, found.orElseThrow());
    assertFalse(otherTenant.isPresent());
  }

  @Test
  void returnsExistingRunForSameTenantServiceAndIdempotencyKey() {
    FakePostgresRunCommands commands = new FakePostgresRunCommands();
    PostgresRunRepository repository = new PostgresRunRepository(commands, new FixedClockPort(NOW));
    Run first = run("run_1", "tenant_1", RunStatus.ACCEPTED, 0);
    Run second = run("run_2", "tenant_1", RunStatus.ACCEPTED, 0);

    Run created = repository.create(first, "idem_1").toCompletableFuture().join();
    Run duplicate = repository.create(second, "idem_1").toCompletableFuture().join();

    assertEquals(created, duplicate);
    assertEquals(1, commands.runs.size());
  }

  @Test
  void compareAndSetStatusUsesVersionAndDomainTransition() {
    FakePostgresRunCommands commands = new FakePostgresRunCommands();
    PostgresRunRepository repository = new PostgresRunRepository(commands, new FixedClockPort(NOW));
    repository
        .create(run("run_1", "tenant_1", RunStatus.ACCEPTED, 0), null)
        .toCompletableFuture()
        .join();

    Run updated =
        repository
            .compareAndSetStatus(new RunId("run_1"), 0, RunStatus.STARTING)
            .toCompletableFuture()
            .join();

    assertEquals(RunStatus.STARTING, updated.status());
    assertEquals(1, updated.version());
    assertEquals(NOW, updated.updatedAt());
  }

  @Test
  void compareAndSetStatusRejectsVersionConflict() {
    FakePostgresRunCommands commands = new FakePostgresRunCommands();
    PostgresRunRepository repository = new PostgresRunRepository(commands, new FixedClockPort(NOW));
    repository
        .create(run("run_1", "tenant_1", RunStatus.ACCEPTED, 0), null)
        .toCompletableFuture()
        .join();

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                repository
                    .compareAndSetStatus(new RunId("run_1"), 9, RunStatus.STARTING)
                    .toCompletableFuture()
                    .join());

    assertInstanceOf(IllegalStateException.class, exception.getCause());
  }

  @Test
  void savesExecutionHandle() {
    FakePostgresRunCommands commands = new FakePostgresRunCommands();
    PostgresRunRepository repository = new PostgresRunRepository(commands, new FixedClockPort(NOW));
    repository
        .create(run("run_1", "tenant_1", RunStatus.ACCEPTED, 0), null)
        .toCompletableFuture()
        .join();
    ExecutionHandle handle =
        new ExecutionHandle(
            "service_1", "adapter_1", "task_1", "thread_1", "downstream_run_1", Map.of("k", "v"));

    repository.saveExecutionHandle(new RunId("run_1"), handle).toCompletableFuture().join();
    Run found =
        repository
            .find(new RunId("run_1"), new TenantId("tenant_1"))
            .toCompletableFuture()
            .join()
            .orElseThrow();

    assertEquals(handle, found.executionHandle());
    assertEquals(1, found.version());
    assertEquals(NOW, found.updatedAt());
  }

  private static Run run(String runId, String tenantId, RunStatus status, long version) {
    return new Run(
        new RunId(runId),
        new ConversationId("conv_1"),
        new TenantId(tenantId),
        "service_1",
        status,
        null,
        version,
        NOW.minusSeconds(10),
        NOW.minusSeconds(10));
  }

  private static final class FakePostgresRunCommands implements PostgresRunCommands {

    private final Map<String, PostgresRunRecord> runs = new ConcurrentHashMap<>();
    private final Map<IdempotencyKey, String> idempotencyIndex = new ConcurrentHashMap<>();

    @Override
    public CompletionStage<PostgresRunRecord> insertOrFind(
        PostgresRunRecord record, String idempotencyKey) {
      if (idempotencyKey != null) {
        IdempotencyKey key =
            new IdempotencyKey(record.tenantId(), record.serviceId(), idempotencyKey);
        String existingRunId = idempotencyIndex.putIfAbsent(key, record.runId());
        if (existingRunId != null) {
          return CompletableFuture.completedFuture(runs.get(existingRunId));
        }
      }
      PostgresRunRecord existing = runs.putIfAbsent(record.runId(), record);
      if (existing != null) {
        return CompletableFuture.failedFuture(new IllegalStateException("runId already exists"));
      }
      return CompletableFuture.completedFuture(record);
    }

    @Override
    public CompletionStage<Optional<PostgresRunRecord>> findByIdAndTenant(
        String runId, String tenantId) {
      PostgresRunRecord record = runs.get(runId);
      if (record == null || !record.tenantId().equals(tenantId)) {
        return CompletableFuture.completedFuture(Optional.empty());
      }
      return CompletableFuture.completedFuture(Optional.of(record));
    }

    @Override
    public CompletionStage<Optional<PostgresRunRecord>> compareAndSetStatus(
        String runId, long expectedVersion, String nextStatus, Instant updatedAt) {
      PostgresRunRecord updated =
          runs.compute(
              runId,
              (ignored, current) -> {
                if (current == null || current.version() != expectedVersion) {
                  return current;
                }
                Run transitioned =
                    current.toDomain().transitionTo(RunStatus.valueOf(nextStatus), updatedAt);
                return PostgresRunRecord.fromDomain(transitioned);
              });
      if (updated == null || updated.version() != expectedVersion + 1) {
        return CompletableFuture.completedFuture(Optional.empty());
      }
      return CompletableFuture.completedFuture(Optional.of(updated));
    }

    @Override
    public CompletionStage<Optional<PostgresRunRecord>> saveExecutionHandle(
        String runId, PostgresExecutionHandleRecord handle, Instant updatedAt) {
      PostgresRunRecord updated =
          runs.compute(
              runId,
              (ignored, current) -> {
                if (current == null) {
                  return null;
                }
                Run domain = current.toDomain().withExecutionHandle(handle.toDomain(), updatedAt);
                return PostgresRunRecord.fromDomain(domain);
              });
      return CompletableFuture.completedFuture(Optional.ofNullable(updated));
    }

    private record IdempotencyKey(String tenantId, String serviceId, String idempotencyKey) {

      private IdempotencyKey {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(serviceId, "serviceId must not be null");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
      }
    }
  }
}
