package com.usig.application.port;

import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunId;
import com.usig.domain.run.RunStatus;
import com.usig.domain.run.TenantId;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface RunRepository {

  CompletionStage<Run> create(Run run, String idempotencyKey);

  CompletionStage<Optional<Run>> find(RunId id, TenantId tenantId);

  CompletionStage<Run> compareAndSetStatus(RunId id, long version, RunStatus next);

  CompletionStage<Void> saveExecutionHandle(RunId id, ExecutionHandle handle);
}
