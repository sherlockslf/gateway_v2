package com.usig.adapter.openai;

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

public final class OpenAiCompatibleAdapter implements StreamingServiceAdapter {

  public static final String ADAPTER_ID = "openai-compatible-v1";

  private final OpenAiCompatibleAdapterConfig config;
  private final OpenAiCompatibleClient client;
  private final OpenAiCompatibleCredentialProvider credentialProvider;
  private final ObjectMapper objectMapper;
  private final OpenAiCompatibleRequestMapper requestMapper;

  public OpenAiCompatibleAdapter(
      OpenAiCompatibleAdapterConfig config, OpenAiCompatibleClient client) {
    this(config, client, legacyCredentialProvider(config), new ObjectMapper());
  }

  public OpenAiCompatibleAdapter(
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleClient client,
      ObjectMapper objectMapper) {
    this(config, client, legacyCredentialProvider(config), objectMapper);
  }

  public OpenAiCompatibleAdapter(
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleClient client,
      OpenAiCompatibleCredentialProvider credentialProvider) {
    this(config, client, credentialProvider, new ObjectMapper());
  }

  public OpenAiCompatibleAdapter(
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleClient client,
      OpenAiCompatibleCredentialProvider credentialProvider,
      ObjectMapper objectMapper) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.client = Objects.requireNonNull(client, "client must not be null");
    this.credentialProvider =
        Objects.requireNonNull(credentialProvider, "credentialProvider must not be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    this.requestMapper = new OpenAiCompatibleRequestMapper();
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
    OpenAiCompatibleRequest downstreamRequest =
        requestMapper.map(request, context, config, credentialProvider);
    return client
        .open(request, downstreamRequest, context, config.service(), config)
        .toCompletableFuture()
        .applyToEither(openTimeout(), Function.identity())
        .thenApply(stream -> execution(request, context, stream));
  }

  @Override
  public CompletionStage<DownstreamExecutionState> cancel(
      ExecutionHandle handle, ExecutionContext context) {
    return client
        .cancel(handle, context, config.service(), config, credentialProvider)
        .toCompletableFuture()
        .applyToEither(cancelTimeout(), Function.identity())
        .thenApply(ignored -> DownstreamExecutionState.CANCELED);
  }

  @Override
  public CompletionStage<DownstreamExecutionState> queryState(
      ExecutionHandle handle, ExecutionContext context) {
    return client.queryState(handle, context, config.service(), config, credentialProvider);
  }

  @Override
  public CompletionStage<Flow.Publisher<GatewayEvent>> resume(
      ExecutionHandle handle, String cursor, ExecutionContext context) {
    if (config.resumeEndpointTemplate() == null) {
      return CompletableFuture.failedFuture(
          new OpenAiCompatibleAdapterException(
              "OpenAI-compatible adapter does not support resume"));
    }
    return client.resume(handle, cursor, context, config.service(), config, credentialProvider);
  }

  @Override
  public CompletionStage<Void> submitInput(SubmitInputCommand command, ExecutionContext context) {
    return submitInput(command, null, context);
  }

  @Override
  public CompletionStage<Void> submitInput(
      SubmitInputCommand command, ExecutionHandle handle, ExecutionContext context) {
    if (config.submitInputEndpointTemplate() == null) {
      return CompletableFuture.failedFuture(
          new OpenAiCompatibleAdapterException(
              "OpenAI-compatible adapter does not support human input"));
    }
    return client.submitInput(
        command, handle, context, config.service(), config, credentialProvider);
  }

  private AdapterExecution execution(
      CreateRunCommand request, ExecutionContext context, OpenAiCompatibleStream stream) {
    ExecutionHandle handle =
        new ExecutionHandle(
            config.service().serviceId(),
            ADAPTER_ID,
            null,
            null,
            stream.downstreamResponseId(),
            Map.of());
    return new AdapterExecution(
        handle,
        new OpenAiCompatibleEventPublisher(
            stream.body(),
            objectMapper,
            request,
            context,
            config.service().serviceId(),
            stream.downstreamResponseId(),
            config.maxFrameBytes(),
            config.firstEventTimeout(),
            config.idleEventTimeout(),
            config.totalExecutionTimeout()));
  }

  private CompletableFuture<Void> cancelTimeout() {
    CompletableFuture<Void> timeout = new CompletableFuture<>();
    CompletableFuture.delayedExecutor(config.cancelTimeout().toMillis(), TimeUnit.MILLISECONDS)
        .execute(
            () ->
                timeout.completeExceptionally(
                    new OpenAiCompatibleAdapterException(
                        ErrorCode.DOWNSTREAM_CANCEL_TIMEOUT,
                        "OpenAI-compatible adapter cancelTimeout elapsed")));
    return timeout;
  }

  private CompletableFuture<OpenAiCompatibleStream> openTimeout() {
    CompletableFuture<OpenAiCompatibleStream> timeout = new CompletableFuture<>();
    CompletableFuture.delayedExecutor(config.openTimeout().toMillis(), TimeUnit.MILLISECONDS)
        .execute(
            () ->
                timeout.completeExceptionally(
                    new OpenAiCompatibleAdapterException(
                        ErrorCode.DOWNSTREAM_CONNECT_FAILED,
                        "OpenAI-compatible adapter openTimeout elapsed")));
    return timeout;
  }

  private static OpenAiCompatibleCredentialProvider legacyCredentialProvider(
      OpenAiCompatibleAdapterConfig config) {
    if (config.authToken() == null) {
      return (service, context) -> java.util.Optional.empty();
    }
    return new StaticOpenAiCompatibleCredentialProvider(
        Map.of(config.service().serviceId(), config.authToken()));
  }
}
