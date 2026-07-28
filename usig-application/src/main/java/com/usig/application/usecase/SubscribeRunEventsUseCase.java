package com.usig.application.usecase;

import com.usig.application.memory.ConcatGatewayEventPublisher;
import com.usig.application.port.DownstreamExecutionPort;
import com.usig.application.port.ExecutionContext;
import com.usig.application.port.RunEventPublisher;
import com.usig.application.port.RunRepository;
import com.usig.application.port.ServiceRegistry;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunId;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceDescriptor;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

public final class SubscribeRunEventsUseCase {

  private final RunRepository runRepository;
  private final RunEventPublisher runEventPublisher;
  private final ServiceRegistry serviceRegistry;
  private final ResumeRetryPolicy resumeRetryPolicy;

  public SubscribeRunEventsUseCase(
      RunRepository runRepository,
      RunEventPublisher runEventPublisher,
      ServiceRegistry serviceRegistry) {
    this(runRepository, runEventPublisher, serviceRegistry, ResumeRetryPolicy.defaults());
  }

  public SubscribeRunEventsUseCase(
      RunRepository runRepository,
      RunEventPublisher runEventPublisher,
      ServiceRegistry serviceRegistry,
      ResumeRetryPolicy resumeRetryPolicy) {
    this.runRepository = runRepository;
    this.runEventPublisher = runEventPublisher;
    this.serviceRegistry = serviceRegistry;
    this.resumeRetryPolicy = resumeRetryPolicy;
  }

  public CompletionStage<Flow.Publisher<GatewayEvent>> subscribe(
      RunId runId, TenantId tenantId, String lastEventId) {
    return runRepository
        .find(runId, tenantId)
        .thenCompose(
            existing -> {
              if (existing.isEmpty()) {
                throw new UseCaseException(ErrorCode.RUN_NOT_FOUND);
              }
              Run run = existing.orElseThrow();
              Flow.Publisher<GatewayEvent> replayOrLive =
                  runEventPublisher.subscribe(runId, lastEventId);
              if (lastEventId == null || lastEventId.isBlank() || run.status().terminal()) {
                return java.util.concurrent.CompletableFuture.completedFuture(replayOrLive);
              }
              ServiceDescriptor service =
                  serviceRegistry
                      .find(run.serviceId())
                      .orElseThrow(() -> new UseCaseException(ErrorCode.SERVICE_NOT_FOUND));
              if (!service.capabilities().resume()) {
                throw new UseCaseException(ErrorCode.RESUME_NOT_SUPPORTED);
              }
              if (run.executionHandle() == null) {
                throw new UseCaseException(ErrorCode.RESUME_NOT_SUPPORTED);
              }
              DownstreamExecutionPort adapter = serviceRegistry.adapterFor(service);
              int maxAttempts =
                  resumeRetryPolicy.maxAttemptsFor(service.capabilities().retrySafety());
              return resumeWithRetry(run, lastEventId, adapter, maxAttempts, 1)
                  .thenApply(
                      resumed -> {
                        Flow.Publisher<GatewayEvent> lifecycle =
                            new RunLifecycleEventPublisher(
                                run.id(), run.tenantId(), resumed, runRepository);
                        runEventPublisher.attach(run.id(), lifecycle);
                        return new ConcatGatewayEventPublisher(replayOrLive, lifecycle);
                      });
            });
  }

  private CompletionStage<Flow.Publisher<GatewayEvent>> resumeWithRetry(
      Run run, String lastEventId, DownstreamExecutionPort adapter, int maxAttempts, int attempt) {
    CompletionStage<Flow.Publisher<GatewayEvent>> resumed;
    try {
      resumed =
          adapter.resume(
              run.executionHandle(),
              lastEventId,
              new ExecutionContext(
                  run.id(),
                  run.tenantId(),
                  null,
                  java.util.Map.of("conversationId", run.conversationId().value())));
    } catch (RuntimeException exception) {
      if (attempt < maxAttempts) {
        return resumeLater(run, lastEventId, adapter, maxAttempts, attempt);
      }
      return CompletableFuture.failedFuture(exception);
    }
    return resumed
        .handle(
            (value, throwable) -> {
              if (throwable == null) {
                return CompletableFuture.completedFuture(value);
              }
              if (attempt < maxAttempts) {
                return resumeLater(run, lastEventId, adapter, maxAttempts, attempt);
              }
              return CompletableFuture.<Flow.Publisher<GatewayEvent>>failedFuture(throwable);
            })
        .thenCompose(stage -> stage);
  }

  private CompletionStage<Flow.Publisher<GatewayEvent>> resumeLater(
      Run run,
      String lastEventId,
      DownstreamExecutionPort adapter,
      int maxAttempts,
      int failedAttempt) {
    Duration delay = resumeRetryPolicy.delayBeforeRetry(failedAttempt);
    return CompletableFuture.supplyAsync(
            () -> null, CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS))
        .thenCompose(
            ignored -> resumeWithRetry(run, lastEventId, adapter, maxAttempts, failedAttempt + 1));
  }
}
