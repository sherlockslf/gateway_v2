package com.usig.adapter.sample;

import com.usig.adapter.spi.StreamingServiceAdapter;
import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.port.AdapterExecution;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.event.EventMetadata;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.service.ServiceDescriptor;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public final class SampleAgentAdapter implements StreamingServiceAdapter {

  public static final String ADAPTER_ID = "sample-agent-v1";

  @Override
  public String adapterId() {
    return ADAPTER_ID;
  }

  @Override
  public boolean supports(ServiceDescriptor service) {
    return ADAPTER_ID.equals(service.adapterId());
  }

  @Override
  public CompletionStage<AdapterExecution> execute(
      CreateRunCommand request, ExecutionContext context) {
    ExecutionHandle handle =
        new ExecutionHandle(
            request.serviceId(),
            ADAPTER_ID,
            "task_" + context.runId().value(),
            null,
            context.runId().value(),
            Map.of("sample", "true"));
    return CompletableFuture.completedFuture(
        new AdapterExecution(handle, new SampleRunPublisher(events(request, context))));
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
    return CompletableFuture.completedFuture(
        new SampleRunPublisher(resumeEvents(handle, cursor, context)));
  }

  @Override
  public CompletionStage<Void> submitInput(SubmitInputCommand command, ExecutionContext context) {
    return CompletableFuture.completedFuture(null);
  }

  private static List<GatewayEvent> events(CreateRunCommand request, ExecutionContext context) {
    Instant now = Instant.now();
    EventMetadata metadata = new EventMetadata(request.tenantId().value(), null, Map.of());
    String conversationId = request.conversationId().value();
    String runId = context.runId().value();
    String serviceId = request.serviceId();
    return List.of(
        GatewayEvent.of(
            "evt_" + runId + "_001",
            EventType.EXECUTION_STARTED,
            conversationId,
            runId,
            serviceId,
            0,
            now,
            Map.of(),
            metadata),
        GatewayEvent.of(
            "evt_" + runId + "_002",
            EventType.TEXT_DELTA,
            conversationId,
            runId,
            serviceId,
            1,
            now,
            Map.of("content", "sample response"),
            metadata),
        GatewayEvent.of(
            "evt_" + runId + "_003",
            EventType.TEXT_END,
            conversationId,
            runId,
            serviceId,
            2,
            now,
            Map.of(),
            metadata),
        GatewayEvent.of(
            "evt_" + runId + "_004",
            EventType.EXECUTION_COMPLETED,
            conversationId,
            runId,
            serviceId,
            3,
            now,
            Map.of(),
            metadata));
  }

  private static List<GatewayEvent> resumeEvents(
      ExecutionHandle handle, String cursor, ExecutionContext context) {
    Instant now = Instant.now();
    EventMetadata metadata = new EventMetadata(context.tenantId().value(), null, Map.of());
    return List.of(
        GatewayEvent.of(
            "evt_" + context.runId().value() + "_resume_001",
            EventType.TEXT_DELTA,
            "conv_resume",
            context.runId().value(),
            handle.serviceId(),
            4,
            now,
            Map.of("content", "resumed after " + cursor),
            metadata),
        GatewayEvent.of(
            "evt_" + context.runId().value() + "_resume_002",
            EventType.EXECUTION_COMPLETED,
            "conv_resume",
            context.runId().value(),
            handle.serviceId(),
            5,
            now,
            Map.of(),
            metadata));
  }

  private static final class SampleRunPublisher implements Flow.Publisher<GatewayEvent> {

    private final List<GatewayEvent> events;

    private SampleRunPublisher(List<GatewayEvent> events) {
      this.events = events;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super GatewayEvent> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            private int index;
            private boolean canceled;

            @Override
            public void request(long requested) {
              if (requested <= 0 || canceled) {
                return;
              }
              long emitted = 0;
              while (emitted < requested && index < events.size() && !canceled) {
                GatewayEvent event = events.get(index);
                index++;
                subscriber.onNext(event);
                emitted++;
              }
              if (index == events.size() && !canceled) {
                subscriber.onComplete();
              }
            }

            @Override
            public void cancel() {
              canceled = true;
            }
          });
    }
  }
}
