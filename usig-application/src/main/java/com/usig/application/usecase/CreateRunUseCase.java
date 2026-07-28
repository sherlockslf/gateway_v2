package com.usig.application.usecase;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.memory.NoopRunCircuitBreaker;
import com.usig.application.memory.NoopRunConcurrencyLimiter;
import com.usig.application.memory.NoopRunTelemetry;
import com.usig.application.port.AdapterExecution;
import com.usig.application.port.ClockPort;
import com.usig.application.port.DownstreamExecutionPort;
import com.usig.application.port.ExecutionContext;
import com.usig.application.port.RunCircuitBreaker;
import com.usig.application.port.RunConcurrencyLimiter;
import com.usig.application.port.RunEventPublisher;
import com.usig.application.port.RunIdGenerator;
import com.usig.application.port.RunRepository;
import com.usig.application.port.RunTelemetry;
import com.usig.application.port.ServiceRegistry;
import com.usig.application.result.CreateRunResult;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunStatus;
import com.usig.domain.service.ServiceDescriptor;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class CreateRunUseCase {

  private final RunRepository runRepository;
  private final ServiceRegistry serviceRegistry;
  private final RunEventPublisher runEventPublisher;
  private final RunIdGenerator runIdGenerator;
  private final ClockPort clock;
  private final RunTelemetry runTelemetry;
  private final RunConcurrencyLimiter concurrencyLimiter;
  private final RunCircuitBreaker circuitBreaker;
  private final InputReferenceValidator inputReferenceValidator;
  private final CreateRunRetryPolicy retryPolicy;

  public CreateRunUseCase(
      RunRepository runRepository,
      ServiceRegistry serviceRegistry,
      RunEventPublisher runEventPublisher,
      RunIdGenerator runIdGenerator,
      ClockPort clock) {
    this(
        runRepository,
        serviceRegistry,
        runEventPublisher,
        runIdGenerator,
        clock,
        new NoopRunTelemetry(),
        new NoopRunConcurrencyLimiter(),
        new NoopRunCircuitBreaker());
  }

  public CreateRunUseCase(
      RunRepository runRepository,
      ServiceRegistry serviceRegistry,
      RunEventPublisher runEventPublisher,
      RunIdGenerator runIdGenerator,
      ClockPort clock,
      RunTelemetry runTelemetry) {
    this(
        runRepository,
        serviceRegistry,
        runEventPublisher,
        runIdGenerator,
        clock,
        runTelemetry,
        new NoopRunConcurrencyLimiter(),
        new NoopRunCircuitBreaker());
  }

  public CreateRunUseCase(
      RunRepository runRepository,
      ServiceRegistry serviceRegistry,
      RunEventPublisher runEventPublisher,
      RunIdGenerator runIdGenerator,
      ClockPort clock,
      RunTelemetry runTelemetry,
      RunConcurrencyLimiter concurrencyLimiter) {
    this(
        runRepository,
        serviceRegistry,
        runEventPublisher,
        runIdGenerator,
        clock,
        runTelemetry,
        concurrencyLimiter,
        new NoopRunCircuitBreaker());
  }

  public CreateRunUseCase(
      RunRepository runRepository,
      ServiceRegistry serviceRegistry,
      RunEventPublisher runEventPublisher,
      RunIdGenerator runIdGenerator,
      ClockPort clock,
      RunTelemetry runTelemetry,
      RunConcurrencyLimiter concurrencyLimiter,
      RunCircuitBreaker circuitBreaker) {
    this(
        runRepository,
        serviceRegistry,
        runEventPublisher,
        runIdGenerator,
        clock,
        runTelemetry,
        concurrencyLimiter,
        circuitBreaker,
        new InputReferenceValidator(),
        CreateRunRetryPolicy.defaults());
  }

  public CreateRunUseCase(
      RunRepository runRepository,
      ServiceRegistry serviceRegistry,
      RunEventPublisher runEventPublisher,
      RunIdGenerator runIdGenerator,
      ClockPort clock,
      RunTelemetry runTelemetry,
      RunConcurrencyLimiter concurrencyLimiter,
      RunCircuitBreaker circuitBreaker,
      CreateRunRetryPolicy retryPolicy) {
    this(
        runRepository,
        serviceRegistry,
        runEventPublisher,
        runIdGenerator,
        clock,
        runTelemetry,
        concurrencyLimiter,
        circuitBreaker,
        new InputReferenceValidator(),
        retryPolicy);
  }

  CreateRunUseCase(
      RunRepository runRepository,
      ServiceRegistry serviceRegistry,
      RunEventPublisher runEventPublisher,
      RunIdGenerator runIdGenerator,
      ClockPort clock,
      RunTelemetry runTelemetry,
      RunConcurrencyLimiter concurrencyLimiter,
      RunCircuitBreaker circuitBreaker,
      InputReferenceValidator inputReferenceValidator) {
    this(
        runRepository,
        serviceRegistry,
        runEventPublisher,
        runIdGenerator,
        clock,
        runTelemetry,
        concurrencyLimiter,
        circuitBreaker,
        inputReferenceValidator,
        CreateRunRetryPolicy.defaults());
  }

  CreateRunUseCase(
      RunRepository runRepository,
      ServiceRegistry serviceRegistry,
      RunEventPublisher runEventPublisher,
      RunIdGenerator runIdGenerator,
      ClockPort clock,
      RunTelemetry runTelemetry,
      RunConcurrencyLimiter concurrencyLimiter,
      RunCircuitBreaker circuitBreaker,
      InputReferenceValidator inputReferenceValidator,
      CreateRunRetryPolicy retryPolicy) {
    this.runRepository = runRepository;
    this.serviceRegistry = serviceRegistry;
    this.runEventPublisher = runEventPublisher;
    this.runIdGenerator = runIdGenerator;
    this.clock = clock;
    this.runTelemetry = runTelemetry;
    this.concurrencyLimiter = concurrencyLimiter;
    this.circuitBreaker = circuitBreaker;
    this.inputReferenceValidator = inputReferenceValidator;
    this.retryPolicy = retryPolicy;
  }

  public CompletionStage<CreateRunResult> create(CreateRunCommand command) {
    ServiceDescriptor service =
        serviceRegistry
            .find(command.serviceId())
            .orElseThrow(() -> new UseCaseException(ErrorCode.SERVICE_NOT_FOUND));
    if (!service.visibleToTenant(command.tenantId().value())) {
      throw new UseCaseException(ErrorCode.FORBIDDEN);
    }
    if (!service.capabilities().streaming()) {
      throw new UseCaseException(ErrorCode.SERVICE_CAPABILITY_UNSUPPORTED);
    }
    if (inputReferenceValidator.containsImage(command)
        && !service.capabilities().multimodalInput()) {
      throw new UseCaseException(ErrorCode.SERVICE_CAPABILITY_UNSUPPORTED);
    }
    inputReferenceValidator.validate(command, service);
    Instant now = clock.now();
    Run run =
        new Run(
            runIdGenerator.nextRunId(),
            command.conversationId(),
            command.tenantId(),
            service.serviceId(),
            RunStatus.ACCEPTED,
            null,
            0,
            now,
            now);
    DownstreamExecutionPort adapter = serviceRegistry.adapterFor(service);
    return runRepository
        .create(run, command.idempotencyKey())
        .thenCompose(
            created ->
                !created.id().equals(run.id())
                    ? completedResult(created, runTelemetry)
                    : startDownstreamRun(command, created, service, adapter));
  }

  private CompletionStage<CreateRunResult> startDownstreamRun(
      CreateRunCommand command,
      Run created,
      ServiceDescriptor service,
      DownstreamExecutionPort adapter) {
    try {
      circuitBreaker.beforeCall(service);
    } catch (UseCaseException exception) {
      return failCreatedRun(created, exception);
    }

    RunConcurrencyLimiter.Permit permit;
    try {
      permit = concurrencyLimiter.acquire(created.tenantId(), service);
    } catch (UseCaseException exception) {
      return failCreatedRun(created, exception);
    }

    AtomicBoolean attached = new AtomicBoolean();
    CompletionStage<CreateRunResult> result;
    try {
      result =
          executeWithRetry(command, created, service, adapter)
              .thenCompose(
                  execution -> attachExecution(created, service, execution, permit, attached));
    } catch (RuntimeException exception) {
      permit.close();
      circuitBreaker.onFailure(service, exception);
      return CompletableFuture.failedFuture(exception);
    }
    return result.whenComplete(
        (ignored, throwable) -> {
          if (throwable != null && !attached.get()) {
            circuitBreaker.onFailure(service, throwable);
            permit.close();
          }
        });
  }

  private CompletionStage<AdapterExecution> executeWithRetry(
      CreateRunCommand command,
      Run created,
      ServiceDescriptor service,
      DownstreamExecutionPort adapter) {
    int maxAttempts = retryPolicy.maxAttemptsFor(service.capabilities().retrySafety());
    return executeAttempt(command, created, adapter, maxAttempts, 1);
  }

  private CompletionStage<AdapterExecution> executeAttempt(
      CreateRunCommand command,
      Run created,
      DownstreamExecutionPort adapter,
      int maxAttempts,
      int attempt) {
    CompletionStage<AdapterExecution> execution;
    try {
      execution =
          adapter.execute(
              command, new ExecutionContext(created.id(), created.tenantId(), null, null));
    } catch (RuntimeException exception) {
      if (attempt < maxAttempts) {
        return retryLater(command, created, adapter, maxAttempts, attempt);
      }
      return CompletableFuture.failedFuture(exception);
    }
    return execution
        .handle(
            (value, throwable) -> {
              if (throwable == null) {
                return CompletableFuture.completedFuture(value);
              }
              if (attempt < maxAttempts) {
                return retryLater(command, created, adapter, maxAttempts, attempt);
              }
              return CompletableFuture.<AdapterExecution>failedFuture(throwable);
            })
        .thenCompose(stage -> stage);
  }

  private CompletionStage<AdapterExecution> retryLater(
      CreateRunCommand command,
      Run created,
      DownstreamExecutionPort adapter,
      int maxAttempts,
      int failedAttempt) {
    Duration delay = retryPolicy.delayBeforeRetry(failedAttempt);
    runTelemetry.runCreateRetryScheduled(created, failedAttempt, delay);
    return CompletableFuture.supplyAsync(
            () -> null, CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS))
        .thenCompose(
            ignored -> executeAttempt(command, created, adapter, maxAttempts, failedAttempt + 1));
  }

  private CompletionStage<CreateRunResult> attachExecution(
      Run created,
      ServiceDescriptor service,
      AdapterExecution execution,
      RunConcurrencyLimiter.Permit permit,
      AtomicBoolean attached) {
    return runRepository
        .saveExecutionHandle(created.id(), execution.handle())
        .thenCompose(
            ignored ->
                runRepository.compareAndSetStatus(
                    created.id(), created.version() + 1, RunStatus.STARTING))
        .thenApply(
            updated -> {
              runEventPublisher.attach(
                  created.id(),
                  new PermitReleasingEventPublisher(
                      new CircuitBreakerRecordingEventPublisher(
                          new RunLifecycleEventPublisher(
                              created.id(), created.tenantId(), execution.events(), runRepository),
                          service,
                          circuitBreaker),
                      permit));
              attached.set(true);
              runTelemetry.runCreated(updated);
              return new CreateRunResult(updated, "/v1/runs/" + updated.id().value() + "/events");
            });
  }

  private CompletionStage<CreateRunResult> failCreatedRun(Run created, UseCaseException exception) {
    return runRepository
        .compareAndSetStatus(created.id(), created.version(), RunStatus.FAILED)
        .handle(
            (ignored, throwable) -> {
              throw exception;
            });
  }

  private static CompletionStage<CreateRunResult> completedResult(
      Run run, RunTelemetry runTelemetry) {
    runTelemetry.runCreationReused(run);
    return java.util.concurrent.CompletableFuture.completedFuture(
        new CreateRunResult(run, "/v1/runs/" + run.id().value() + "/events"));
  }
}
