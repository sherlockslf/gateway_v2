package com.usig.application.usecase;

import com.usig.application.port.RunRepository;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunId;
import com.usig.domain.run.TenantId;
import java.util.concurrent.CompletionStage;

public final class GetRunUseCase {

  private final RunRepository runRepository;

  public GetRunUseCase(RunRepository runRepository) {
    this.runRepository = runRepository;
  }

  public CompletionStage<Run> get(RunId runId, TenantId tenantId) {
    return runRepository
        .find(runId, tenantId)
        .thenApply(
            existing -> existing.orElseThrow(() -> new UseCaseException(ErrorCode.RUN_NOT_FOUND)));
  }
}
