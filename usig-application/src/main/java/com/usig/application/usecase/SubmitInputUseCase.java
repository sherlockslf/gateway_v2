package com.usig.application.usecase;

import com.usig.application.command.SubmitInputCommand;
import com.usig.application.port.ExecutionContext;
import com.usig.application.port.RunRepository;
import com.usig.application.port.ServiceRegistry;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunStatus;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public final class SubmitInputUseCase {

  private final RunRepository runRepository;
  private final ServiceRegistry serviceRegistry;

  public SubmitInputUseCase(RunRepository runRepository, ServiceRegistry serviceRegistry) {
    this.runRepository = runRepository;
    this.serviceRegistry = serviceRegistry;
  }

  public CompletionStage<Void> submit(SubmitInputCommand command) {
    return runRepository
        .find(command.runId(), command.tenantId())
        .thenCompose(
            existing -> {
              Run run = existing.orElseThrow(() -> new UseCaseException(ErrorCode.RUN_NOT_FOUND));
              if (run.status() != RunStatus.WAITING_INPUT) {
                throw new UseCaseException(ErrorCode.INVALID_REQUEST);
              }
              ServiceDescriptor service =
                  serviceRegistry
                      .find(run.serviceId())
                      .orElseThrow(() -> new UseCaseException(ErrorCode.SERVICE_NOT_FOUND));
              if (!service.capabilities().humanInput()) {
                throw new UseCaseException(ErrorCode.SERVICE_CAPABILITY_UNSUPPORTED);
              }
              validateInputPayload(command.payload());
              return serviceRegistry
                  .adapterFor(service)
                  .submitInput(
                      command,
                      run.executionHandle(),
                      new ExecutionContext(run.id(), run.tenantId(), null, null));
            });
  }

  private static void validateInputPayload(Map<String, Object> payload) {
    if (!payload.containsKey("approvalRequestId")) {
      return;
    }

    Object approvalRequestId = payload.get("approvalRequestId");
    if (!(approvalRequestId instanceof String value) || value.isBlank()) {
      throw new UseCaseException(
          ErrorCode.INVALID_REQUEST, "approvalRequestId must be a non-blank string");
    }

    if (!(payload.get("approve") instanceof Boolean)) {
      throw new UseCaseException(ErrorCode.INVALID_REQUEST, "approve must be a boolean");
    }

    Object reason = payload.get("reason");
    if (reason != null && !(reason instanceof String)) {
      throw new UseCaseException(ErrorCode.INVALID_REQUEST, "reason must be a string");
    }
  }
}
