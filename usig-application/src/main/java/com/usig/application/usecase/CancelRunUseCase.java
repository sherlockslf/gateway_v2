package com.usig.application.usecase;

import com.usig.application.port.DownstreamExecutionPort;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.application.port.RunRepository;
import com.usig.application.port.ServiceRegistry;
import com.usig.application.result.CancelRunResult;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunId;
import com.usig.domain.run.RunStatus;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceDescriptor;
import java.util.concurrent.CompletionStage;

public final class CancelRunUseCase {

  private final RunRepository runRepository;
  private final ServiceRegistry serviceRegistry;

  public CancelRunUseCase(RunRepository runRepository, ServiceRegistry serviceRegistry) {
    this.runRepository = runRepository;
    this.serviceRegistry = serviceRegistry;
  }

  public CompletionStage<CancelRunResult> cancel(RunId runId, TenantId tenantId) {
    return runRepository
        .find(runId, tenantId)
        .thenCompose(
            existing -> {
              Run run = existing.orElseThrow(() -> new UseCaseException(ErrorCode.RUN_NOT_FOUND));
              if (run.status().terminal()) {
                throw new UseCaseException(ErrorCode.RUN_ALREADY_TERMINAL);
              }
              ServiceDescriptor service =
                  serviceRegistry
                      .find(run.serviceId())
                      .orElseThrow(() -> new UseCaseException(ErrorCode.SERVICE_NOT_FOUND));
              if (!service.capabilities().cancel()) {
                return runRepository
                    .compareAndSetStatus(run.id(), run.version(), RunStatus.CANCEL_REQUESTED)
                    .thenApply(
                        updated ->
                            new CancelRunResult(
                                updated, DownstreamExecutionState.CANCEL_NOT_SUPPORTED));
              }
              DownstreamExecutionPort adapter = serviceRegistry.adapterFor(service);
              return runRepository
                  .compareAndSetStatus(run.id(), run.version(), RunStatus.CANCEL_REQUESTED)
                  .thenCompose(
                      updated ->
                          adapter
                              .cancel(
                                  updated.executionHandle(),
                                  new ExecutionContext(
                                      updated.id(), updated.tenantId(), null, null))
                              .thenApply(state -> new CancelRunResult(updated, state)));
            });
  }
}
