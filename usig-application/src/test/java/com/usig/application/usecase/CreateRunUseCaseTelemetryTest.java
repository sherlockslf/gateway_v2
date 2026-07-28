package com.usig.application.usecase;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.memory.FixedClockPort;
import com.usig.application.memory.InMemoryRunEventPublisher;
import com.usig.application.memory.InMemoryRunIdGenerator;
import com.usig.application.memory.InMemoryRunRepository;
import com.usig.application.memory.InMemoryServiceRegistry;
import com.usig.application.memory.StaticGatewayEventPublisher;
import com.usig.application.port.AdapterExecution;
import com.usig.application.port.DownstreamExecutionPort;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.application.port.RunTelemetry;
import com.usig.domain.event.EventMetadata;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.ConversationId;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.run.Run;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;

class CreateRunUseCaseTelemetryTest {

  @Test
  void recordsCreatedAndIdempotencyReuseTelemetry() {
    RecordingTelemetry telemetry = new RecordingTelemetry();
    InMemoryServiceRegistry registry = new InMemoryServiceRegistry();
    registry.register(service(), new FakeDownstreamExecutionPort());
    CreateRunUseCase useCase =
        new CreateRunUseCase(
            new InMemoryRunRepository(new FixedClockPort(Instant.parse("2026-07-27T07:10:00Z"))),
            registry,
            new InMemoryRunEventPublisher(),
            new InMemoryRunIdGenerator(),
            new FixedClockPort(Instant.parse("2026-07-27T07:10:00Z")),
            telemetry);
    CreateRunCommand command =
        new CreateRunCommand(
            new ConversationId("conv_1"),
            new TenantId("tenant_1"),
            "sample-agent",
            "idem_1",
            List.of(),
            java.util.Map.of());

    useCase.create(command).toCompletableFuture().join();
    useCase.create(command).toCompletableFuture().join();

    assertEquals(List.of("created:run_1", "reused:run_1"), telemetry.events);
  }

  @Test
  void recordsCreateRetryScheduledTelemetryBeforeRetrying() {
    RecordingTelemetry telemetry = new RecordingTelemetry();
    InMemoryServiceRegistry registry = new InMemoryServiceRegistry();
    registry.register(retrySafeService(), new FailingOnceDownstreamExecutionPort());
    CreateRunUseCase useCase =
        new CreateRunUseCase(
            new InMemoryRunRepository(new FixedClockPort(Instant.parse("2026-07-27T07:10:00Z"))),
            registry,
            new InMemoryRunEventPublisher(),
            new InMemoryRunIdGenerator(),
            new FixedClockPort(Instant.parse("2026-07-27T07:10:00Z")),
            telemetry,
            new com.usig.application.memory.NoopRunConcurrencyLimiter(),
            new com.usig.application.memory.NoopRunCircuitBreaker(),
            new InputReferenceValidator(),
            CreateRunRetryPolicy.immediateForTests());
    CreateRunCommand command =
        new CreateRunCommand(
            new ConversationId("conv_1"),
            new TenantId("tenant_1"),
            "sample-agent",
            "idem_retry",
            List.of(),
            java.util.Map.of());

    useCase.create(command).toCompletableFuture().join();

    assertEquals(List.of("retry:run_1:1:0", "created:run_1"), telemetry.events);
  }

  private static ServiceDescriptor service() {
    return new ServiceDescriptor(
        "sample-agent",
        ServiceType.AGENT,
        URI.create("https://internal.example/sample-agent"),
        "fake-adapter",
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, false, false, true, false, true, true),
        1);
  }

  private static ServiceDescriptor retrySafeService() {
    return new ServiceDescriptor(
        "sample-agent",
        ServiceType.AGENT,
        URI.create("https://internal.example/sample-agent"),
        "fake-adapter",
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(
            true,
            true,
            false,
            false,
            true,
            false,
            true,
            true,
            com.usig.domain.service.RetrySafety.IDEMPOTENT_CREATE_ONLY),
        1);
  }

  private static final class RecordingTelemetry implements RunTelemetry {

    private final List<String> events = new ArrayList<>();

    @Override
    public void runCreated(Run run) {
      events.add("created:" + run.id().value());
    }

    @Override
    public void runCreationReused(Run run) {
      events.add("reused:" + run.id().value());
    }

    @Override
    public void runCreateRetryScheduled(Run run, int failedAttempt, Duration delay) {
      events.add("retry:" + run.id().value() + ":" + failedAttempt + ":" + delay.toMillis());
    }
  }

  private static final class FailingOnceDownstreamExecutionPort
      extends FakeDownstreamExecutionPort {

    private int remainingFailures = 1;

    @Override
    public CompletionStage<AdapterExecution> execute(
        CreateRunCommand command, ExecutionContext context) {
      if (remainingFailures > 0) {
        remainingFailures--;
        return CompletableFuture.failedFuture(new IllegalStateException("temporary failure"));
      }
      return super.execute(command, context);
    }
  }

  private static class FakeDownstreamExecutionPort implements DownstreamExecutionPort {

    @Override
    public CompletionStage<AdapterExecution> execute(
        CreateRunCommand command, ExecutionContext context) {
      return CompletableFuture.completedFuture(
          new AdapterExecution(
              new ExecutionHandle(
                  command.serviceId(), "fake-adapter", "task_1", null, null, Map.of()),
              new StaticGatewayEventPublisher(
                  List.of(
                      GatewayEvent.of(
                          "evt_1",
                          EventType.EXECUTION_STARTED,
                          "conv_1",
                          context.runId().value(),
                          command.serviceId(),
                          0,
                          Instant.parse("2026-07-27T07:10:00Z"),
                          Map.of(),
                          new EventMetadata(context.tenantId().value(), null, Map.of()))))));
    }

    @Override
    public CompletionStage<DownstreamExecutionState> cancel(
        ExecutionHandle handle, ExecutionContext context) {
      return CompletableFuture.completedFuture(DownstreamExecutionState.CANCELED);
    }

    @Override
    public CompletionStage<DownstreamExecutionState> queryState(
        ExecutionHandle handle, ExecutionContext context) {
      return CompletableFuture.completedFuture(DownstreamExecutionState.RUNNING);
    }

    @Override
    public CompletionStage<Flow.Publisher<GatewayEvent>> resume(
        ExecutionHandle handle, String cursor, ExecutionContext context) {
      return CompletableFuture.completedFuture(new StaticGatewayEventPublisher(List.of()));
    }

    @Override
    public CompletionStage<Void> submitInput(SubmitInputCommand command, ExecutionContext context) {
      return CompletableFuture.completedFuture(null);
    }
  }
}
