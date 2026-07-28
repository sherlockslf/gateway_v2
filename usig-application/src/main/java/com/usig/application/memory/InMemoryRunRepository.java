package com.usig.application.memory;

import com.usig.application.port.ClockPort;
import com.usig.application.port.RunRepository;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunId;
import com.usig.domain.run.RunStatus;
import com.usig.domain.run.TenantId;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryRunRepository implements RunRepository {

  private final ClockPort clock;
  private final Map<RunId, Run> runs = new ConcurrentHashMap<>();
  private final Map<IdempotencyKey, RunId> idempotencyIndex = new ConcurrentHashMap<>();

  public InMemoryRunRepository(ClockPort clock) {
    this.clock = clock;
  }

  @Override
  public CompletionStage<Run> create(Run run, String idempotencyKey) {
    if (idempotencyKey == null) {
      Run existing = runs.putIfAbsent(run.id(), run);
      if (existing != null) {
        throw new IllegalStateException("runId already exists");
      }
      return CompletableFuture.completedFuture(run);
    }
    IdempotencyKey key = new IdempotencyKey(run.tenantId(), run.serviceId(), idempotencyKey);
    RunId existingRunId = idempotencyIndex.putIfAbsent(key, run.id());
    if (existingRunId != null) {
      return CompletableFuture.completedFuture(runs.get(existingRunId));
    }
    runs.put(run.id(), run);
    return CompletableFuture.completedFuture(run);
  }

  @Override
  public CompletionStage<Optional<Run>> find(RunId id, TenantId tenantId) {
    Run run = runs.get(id);
    if (run == null || !run.tenantId().equals(tenantId)) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    return CompletableFuture.completedFuture(Optional.of(run));
  }

  @Override
  public CompletionStage<Run> compareAndSetStatus(RunId id, long version, RunStatus next) {
    Run updated =
        runs.compute(
            id,
            (ignored, current) -> {
              if (current == null) {
                throw new IllegalStateException("run not found");
              }
              if (current.version() != version) {
                throw new IllegalStateException("run version conflict");
              }
              return current.transitionTo(next, clock.now());
            });
    return CompletableFuture.completedFuture(updated);
  }

  @Override
  public CompletionStage<Void> saveExecutionHandle(RunId id, ExecutionHandle handle) {
    runs.compute(
        id,
        (ignored, current) -> {
          if (current == null) {
            throw new IllegalStateException("run not found");
          }
          return current.withExecutionHandle(handle, clock.now());
        });
    return CompletableFuture.completedFuture(null);
  }

  private record IdempotencyKey(TenantId tenantId, String serviceId, String idempotencyKey) {

    private IdempotencyKey {
      tenantId = Objects.requireNonNull(tenantId, "tenantId must not be null");
      if (serviceId == null || serviceId.isBlank()) {
        throw new IllegalArgumentException("serviceId must not be blank");
      }
      if (idempotencyKey == null || idempotencyKey.isBlank()) {
        throw new IllegalArgumentException("idempotencyKey must not be blank");
      }
    }
  }
}
