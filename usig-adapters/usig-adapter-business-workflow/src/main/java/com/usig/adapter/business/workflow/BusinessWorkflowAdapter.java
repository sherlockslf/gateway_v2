package com.usig.adapter.business.workflow;

import com.usig.adapter.spi.StreamingServiceAdapter;
import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.port.AdapterExecution;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.ConversationId;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.service.ServiceDescriptor;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class BusinessWorkflowAdapter implements StreamingServiceAdapter {

  public static final String ADAPTER_ID = "business-workflow-v1";

  private final BusinessWorkflowAdapterConfig config;
  private final BusinessWorkflowClient client;
  private final Clock clock;

  public BusinessWorkflowAdapter(
      BusinessWorkflowAdapterConfig config, BusinessWorkflowClient client) {
    this(config, client, Clock.systemUTC());
  }

  public BusinessWorkflowAdapter(
      BusinessWorkflowAdapterConfig config, BusinessWorkflowClient client, Clock clock) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.client = Objects.requireNonNull(client, "client must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  @Override
  public String adapterId() {
    return ADAPTER_ID;
  }

  @Override
  public boolean supports(ServiceDescriptor service) {
    return ADAPTER_ID.equals(service.adapterId())
        && config.service().serviceId().equals(service.serviceId());
  }

  @Override
  public CompletionStage<AdapterExecution> execute(
      CreateRunCommand request, ExecutionContext context) {
    return client
        .open(request, context, config)
        .toCompletableFuture()
        .applyToEither(openTimeout(), Function.identity())
        .thenApply(stream -> execution(request, context, stream));
  }

  @Override
  public CompletionStage<DownstreamExecutionState> cancel(
      ExecutionHandle handle, ExecutionContext context) {
    return client
        .cancel(handle, context, config)
        .toCompletableFuture()
        .applyToEither(cancelTimeout(), Function.identity())
        .thenApply(ignored -> DownstreamExecutionState.CANCELED);
  }

  @Override
  public CompletionStage<DownstreamExecutionState> queryState(
      ExecutionHandle handle, ExecutionContext context) {
    return client.queryState(handle, context, config);
  }

  @Override
  public CompletionStage<Flow.Publisher<GatewayEvent>> resume(
      ExecutionHandle handle, String cursor, ExecutionContext context) {
    return client
        .resume(handle, cursor, context, config)
        .thenApply(stream -> publisher(resumeCommand(handle, context), context, stream));
  }

  @Override
  public CompletionStage<Void> submitInput(SubmitInputCommand command, ExecutionContext context) {
    return client.submitInput(command, context, config);
  }

  private AdapterExecution execution(
      CreateRunCommand request, ExecutionContext context, BusinessWorkflowStream stream) {
    ExecutionHandle handle =
        new ExecutionHandle(
            config.service().serviceId(),
            ADAPTER_ID,
            stream.workflowRunId(),
            null,
            context.runId().value(),
            Map.of("protocol", "business-workflow"));
    return new AdapterExecution(handle, publisher(request, context, stream));
  }

  private Flow.Publisher<GatewayEvent> publisher(
      CreateRunCommand request, ExecutionContext context, BusinessWorkflowStream stream) {
    return new BusinessWorkflowMappedPublisher(
        stream.events(),
        new BusinessWorkflowEventMapper(),
        request,
        context,
        config.service().serviceId(),
        stream.workflowRunId(),
        clock,
        config.firstEventTimeout(),
        config.idleEventTimeout(),
        config.totalExecutionTimeout());
  }

  private CreateRunCommand resumeCommand(ExecutionHandle handle, ExecutionContext context) {
    String conversationId = context.attributes().getOrDefault("conversationId", "conv_resume");
    return new CreateRunCommand(
        new ConversationId(conversationId),
        context.tenantId(),
        handle.serviceId(),
        null,
        List.of(),
        null);
  }

  private CompletableFuture<BusinessWorkflowStream> openTimeout() {
    CompletableFuture<BusinessWorkflowStream> timeout = new CompletableFuture<>();
    CompletableFuture.delayedExecutor(config.openTimeout().toMillis(), TimeUnit.MILLISECONDS)
        .execute(
            () ->
                timeout.completeExceptionally(
                    new BusinessWorkflowAdapterException(
                        ErrorCode.DOWNSTREAM_CONNECT_FAILED,
                        "Business workflow adapter openTimeout elapsed")));
    return timeout;
  }

  private CompletableFuture<Void> cancelTimeout() {
    CompletableFuture<Void> timeout = new CompletableFuture<>();
    CompletableFuture.delayedExecutor(config.cancelTimeout().toMillis(), TimeUnit.MILLISECONDS)
        .execute(
            () ->
                timeout.completeExceptionally(
                    new BusinessWorkflowAdapterException(
                        ErrorCode.DOWNSTREAM_CANCEL_TIMEOUT,
                        "Business workflow adapter cancelTimeout elapsed")));
    return timeout;
  }
}
