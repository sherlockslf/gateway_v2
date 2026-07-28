package com.usig.infra.postgres;

import com.usig.application.port.ClockPort;
import com.usig.application.port.RunRepository;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunId;
import com.usig.domain.run.RunStatus;
import com.usig.domain.run.TenantId;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public final class PostgresRunRepository implements RunRepository {

  private final PostgresRunCommands commands;
  private final ClockPort clock;

  public PostgresRunRepository(PostgresRunCommands commands, ClockPort clock) {
    this.commands = Objects.requireNonNull(commands, "commands must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  @Override
  public CompletionStage<Run> create(Run run, String idempotencyKey) {
    return commands
        .insertOrFind(PostgresRunRecord.fromDomain(run), normalize(idempotencyKey))
        .thenApply(PostgresRunRecord::toDomain);
  }

  @Override
  public CompletionStage<Optional<Run>> find(RunId id, TenantId tenantId) {
    return commands
        .findByIdAndTenant(id.value(), tenantId.value())
        .thenApply(record -> record.map(PostgresRunRecord::toDomain));
  }

  @Override
  public CompletionStage<Run> compareAndSetStatus(RunId id, long version, RunStatus next) {
    return commands
        .compareAndSetStatus(id.value(), version, next.name(), clock.now())
        .thenApply(
            record ->
                record
                    .orElseThrow(() -> new IllegalStateException("run version conflict"))
                    .toDomain());
  }

  @Override
  public CompletionStage<Void> saveExecutionHandle(RunId id, ExecutionHandle handle) {
    return commands
        .saveExecutionHandle(
            id.value(), PostgresExecutionHandleRecord.fromDomain(handle), clock.now())
        .thenApply(
            record -> {
              if (record.isEmpty()) {
                throw new IllegalStateException("run not found");
              }
              return null;
            });
  }

  private static String normalize(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }
}
