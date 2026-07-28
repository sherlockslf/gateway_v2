package com.usig.adapter.agent.a2a;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.usig.adapter.spi.StreamingServiceAdapter;
import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.port.AdapterExecution;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class AgentA2aAdapter implements StreamingServiceAdapter {

  public static final String ADAPTER_ID = "agent-a2a-v1";

  private final AgentA2aAdapterConfig config;
  private final AgentA2aClient client;
  private final ObjectMapper objectMapper;

  public AgentA2aAdapter(AgentA2aAdapterConfig config, AgentA2aClient client) {
    this(config, client, new ObjectMapper());
  }

  public AgentA2aAdapter(
      AgentA2aAdapterConfig config, AgentA2aClient client, ObjectMapper objectMapper) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.client = Objects.requireNonNull(client, "client must not be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
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
        .thenApply(
            stream ->
                new AgentA2aMappedPublisher(
                    stream.body(),
                    new AgentA2aSseFrameDecoder(config.maxFrameBytes()),
                    new AgentA2aEventMapper(objectMapper),
                    resumeCommand(handle, context),
                    context,
                    config.service().serviceId(),
                    stream.downstreamTaskId(),
                    config.firstEventTimeout(),
                    config.idleEventTimeout(),
                    config.totalExecutionTimeout()));
  }

  @Override
  public CompletionStage<Void> submitInput(SubmitInputCommand command, ExecutionContext context) {
    return client.submitInput(command, context, config);
  }

  private AdapterExecution execution(
      CreateRunCommand request, ExecutionContext context, AgentA2aStream stream) {
    ExecutionHandle handle =
        new ExecutionHandle(
            config.service().serviceId(),
            ADAPTER_ID,
            stream.downstreamTaskId(),
            null,
            context.runId().value(),
            Map.of("protocol", "agent-a2a"));
    return new AdapterExecution(
        handle,
        new AgentA2aMappedPublisher(
            stream.body(),
            new AgentA2aSseFrameDecoder(config.maxFrameBytes()),
            new AgentA2aEventMapper(objectMapper),
            request,
            context,
            config.service().serviceId(),
            stream.downstreamTaskId(),
            config.firstEventTimeout(),
            config.idleEventTimeout(),
            config.totalExecutionTimeout()));
  }

  private CreateRunCommand resumeCommand(ExecutionHandle handle, ExecutionContext context) {
    String conversationId = context.attributes().getOrDefault("conversationId", "conv_resume");
    return new CreateRunCommand(
        new com.usig.domain.run.ConversationId(conversationId),
        context.tenantId(),
        handle.serviceId(),
        null,
        java.util.List.of(),
        null);
  }

  private CompletableFuture<AgentA2aStream> openTimeout() {
    CompletableFuture<AgentA2aStream> timeout = new CompletableFuture<>();
    CompletableFuture.delayedExecutor(config.openTimeout().toMillis(), TimeUnit.MILLISECONDS)
        .execute(
            () ->
                timeout.completeExceptionally(
                    new AgentA2aAdapterException(
                        ErrorCode.DOWNSTREAM_CONNECT_FAILED,
                        "Agent A2A adapter openTimeout elapsed")));
    return timeout;
  }

  private CompletableFuture<Void> cancelTimeout() {
    CompletableFuture<Void> timeout = new CompletableFuture<>();
    CompletableFuture.delayedExecutor(config.cancelTimeout().toMillis(), TimeUnit.MILLISECONDS)
        .execute(
            () ->
                timeout.completeExceptionally(
                    new AgentA2aAdapterException(
                        ErrorCode.DOWNSTREAM_CANCEL_TIMEOUT,
                        "Agent A2A adapter cancelTimeout elapsed")));
    return timeout;
  }
}
