package com.usig.adapter.generic.sse;

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

public final class GenericSseJsonAdapter implements StreamingServiceAdapter {

  public static final String ADAPTER_ID = "generic-sse-json-v1";

  private final GenericSseAdapterConfig config;
  private final DownstreamSseClient client;
  private final ObjectMapper objectMapper;

  public GenericSseJsonAdapter(GenericSseAdapterConfig config, DownstreamSseClient client) {
    this(config, client, new ObjectMapper());
  }

  public GenericSseJsonAdapter(
      GenericSseAdapterConfig config, DownstreamSseClient client, ObjectMapper objectMapper) {
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
        .open(request, context, config.service(), config)
        .toCompletableFuture()
        .applyToEither(openTimeout(), Function.identity())
        .thenApply(stream -> execution(request, context, stream));
  }

  @Override
  public CompletionStage<DownstreamExecutionState> cancel(
      ExecutionHandle handle, ExecutionContext context) {
    return withCancelTimeout(client.cancel(handle, context))
        .thenApply(ignored -> DownstreamExecutionState.CANCELED);
  }

  @Override
  public CompletionStage<DownstreamExecutionState> queryState(
      ExecutionHandle handle, ExecutionContext context) {
    return CompletableFuture.completedFuture(DownstreamExecutionState.RUNNING);
  }

  @Override
  public CompletionStage<Flow.Publisher<GatewayEvent>> resume(
      ExecutionHandle handle, String cursor, ExecutionContext context) {
    return CompletableFuture.failedFuture(
        new GenericSseAdapterException("Generic SSE adapter does not support resume"));
  }

  @Override
  public CompletionStage<Void> submitInput(SubmitInputCommand command, ExecutionContext context) {
    return CompletableFuture.failedFuture(
        new GenericSseAdapterException("Generic SSE adapter does not support human input"));
  }

  private AdapterExecution execution(
      CreateRunCommand request, ExecutionContext context, DownstreamSseStream stream) {
    ExecutionHandle handle =
        new ExecutionHandle(
            config.service().serviceId(),
            ADAPTER_ID,
            stream.downstreamTaskId(),
            null,
            context.runId().value(),
            Map.of("mappingProfile", config.mappingProfile().profileId()));
    GenericSseEventMapper mapper = new GenericSseEventMapper(objectMapper, config.mappingProfile());
    return new AdapterExecution(
        handle,
        new GenericSseMappedPublisher(
            stream.body(),
            new SseFrameDecoder(config.maxFrameBytes()),
            mapper,
            request,
            context,
            config.service().serviceId(),
            stream.downstreamTaskId(),
            config.firstEventTimeout(),
            config.idleEventTimeout(),
            config.totalExecutionTimeout()));
  }

  private CompletionStage<Void> withCancelTimeout(CompletionStage<Void> cancel) {
    CompletableFuture<Void> timeout = new CompletableFuture<>();
    CompletableFuture.delayedExecutor(config.cancelTimeout().toMillis(), TimeUnit.MILLISECONDS)
        .execute(
            () ->
                timeout.completeExceptionally(
                    new GenericSseAdapterException(
                        ErrorCode.DOWNSTREAM_CANCEL_TIMEOUT,
                        "Generic SSE adapter cancelTimeout elapsed")));
    return cancel.toCompletableFuture().applyToEither(timeout, Function.identity());
  }

  private CompletableFuture<DownstreamSseStream> openTimeout() {
    CompletableFuture<DownstreamSseStream> timeout = new CompletableFuture<>();
    CompletableFuture.delayedExecutor(config.openTimeout().toMillis(), TimeUnit.MILLISECONDS)
        .execute(
            () ->
                timeout.completeExceptionally(
                    new GenericSseAdapterException(
                        ErrorCode.DOWNSTREAM_CONNECT_FAILED,
                        "Generic SSE adapter openTimeout elapsed")));
    return timeout;
  }
}
