package com.usig.adapter.openai;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.ConversationId;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceDescriptor;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

public final class JavaHttpOpenAiCompatibleClient implements OpenAiCompatibleClient {

  private static final List<String> RESPONSE_ID_HEADERS =
      List.of("x-request-id", "openai-request-id", "x-ms-request-id");

  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;

  public JavaHttpOpenAiCompatibleClient() {
    this(HttpClient.newHttpClient(), new ObjectMapper());
  }

  public JavaHttpOpenAiCompatibleClient(HttpClient httpClient, ObjectMapper objectMapper) {
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
  }

  @Override
  public CompletionStage<OpenAiCompatibleStream> open(
      CreateRunCommand request,
      OpenAiCompatibleRequest downstreamRequest,
      ExecutionContext context,
      ServiceDescriptor service,
      OpenAiCompatibleAdapterConfig config) {
    Objects.requireNonNull(downstreamRequest, "downstreamRequest must not be null");
    HttpRequest httpRequest = httpRequest(downstreamRequest, config);
    return httpClient
        .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofPublisher())
        .thenCompose(response -> stream(response, context));
  }

  @Override
  public CompletionStage<Void> cancel(ExecutionHandle handle, ExecutionContext context) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletionStage<Void> cancel(
      ExecutionHandle handle,
      ExecutionContext context,
      ServiceDescriptor service,
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleCredentialProvider credentialProvider) {
    Objects.requireNonNull(handle, "handle must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(service, "service must not be null");
    Objects.requireNonNull(config, "config must not be null");
    Objects.requireNonNull(credentialProvider, "credentialProvider must not be null");
    if (config.cancelEndpointTemplate() == null) {
      return CompletableFuture.completedFuture(null);
    }
    if (handle.downstreamRunId() == null) {
      return CompletableFuture.failedFuture(
          new OpenAiCompatibleAdapterException(
              "OpenAI-compatible cancel requires downstreamRunId"));
    }
    HttpRequest httpRequest = cancelRequest(handle, context, service, config, credentialProvider);
    return httpClient
        .sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding())
        .thenCompose(
            response -> {
              if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return CompletableFuture.completedFuture(null);
              }
              return CompletableFuture.failedFuture(
                  new OpenAiCompatibleAdapterException(
                      "OpenAI-compatible cancel returned HTTP " + response.statusCode()));
            });
  }

  @Override
  public CompletionStage<DownstreamExecutionState> queryState(
      ExecutionHandle handle,
      ExecutionContext context,
      ServiceDescriptor service,
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleCredentialProvider credentialProvider) {
    Objects.requireNonNull(handle, "handle must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(service, "service must not be null");
    Objects.requireNonNull(config, "config must not be null");
    Objects.requireNonNull(credentialProvider, "credentialProvider must not be null");
    if (config.queryStateEndpointTemplate() == null) {
      return CompletableFuture.completedFuture(DownstreamExecutionState.UNKNOWN);
    }
    if (handle.downstreamRunId() == null) {
      return CompletableFuture.failedFuture(
          new OpenAiCompatibleAdapterException(
              "OpenAI-compatible queryState requires downstreamRunId"));
    }
    HttpRequest httpRequest =
        queryStateRequest(handle, context, service, config, credentialProvider);
    return httpClient
        .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
        .thenCompose(
            response -> {
              if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return CompletableFuture.completedFuture(downstreamState(response.body()));
              }
              return CompletableFuture.failedFuture(
                  new OpenAiCompatibleAdapterException(
                      "OpenAI-compatible queryState returned HTTP " + response.statusCode()));
            });
  }

  @Override
  public CompletionStage<Void> submitInput(
      SubmitInputCommand command,
      ExecutionHandle handle,
      ExecutionContext context,
      ServiceDescriptor service,
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleCredentialProvider credentialProvider) {
    Objects.requireNonNull(command, "command must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(service, "service must not be null");
    Objects.requireNonNull(config, "config must not be null");
    Objects.requireNonNull(credentialProvider, "credentialProvider must not be null");
    if (config.submitInputEndpointTemplate() == null) {
      return CompletableFuture.failedFuture(
          new OpenAiCompatibleAdapterException(
              "OpenAI-compatible adapter does not support human input"));
    }
    if (handle == null || handle.downstreamRunId() == null) {
      return CompletableFuture.failedFuture(
          new OpenAiCompatibleAdapterException(
              "OpenAI-compatible submitInput requires downstreamRunId"));
    }
    HttpRequest httpRequest =
        submitInputRequest(command, handle, context, service, config, credentialProvider);
    return httpClient
        .sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding())
        .thenCompose(
            response -> {
              if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return CompletableFuture.completedFuture(null);
              }
              return CompletableFuture.failedFuture(
                  new OpenAiCompatibleAdapterException(
                      "OpenAI-compatible submitInput returned HTTP " + response.statusCode()));
            });
  }

  @Override
  public CompletionStage<Flow.Publisher<GatewayEvent>> resume(
      ExecutionHandle handle,
      String cursor,
      ExecutionContext context,
      ServiceDescriptor service,
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleCredentialProvider credentialProvider) {
    Objects.requireNonNull(handle, "handle must not be null");
    Objects.requireNonNull(cursor, "cursor must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(service, "service must not be null");
    Objects.requireNonNull(config, "config must not be null");
    Objects.requireNonNull(credentialProvider, "credentialProvider must not be null");
    if (config.resumeEndpointTemplate() == null) {
      return CompletableFuture.failedFuture(
          new OpenAiCompatibleAdapterException(
              "OpenAI-compatible adapter does not support resume"));
    }
    if (handle.downstreamRunId() == null) {
      return CompletableFuture.failedFuture(
          new OpenAiCompatibleAdapterException(
              "OpenAI-compatible resume requires downstreamRunId"));
    }
    HttpRequest httpRequest =
        resumeRequest(handle, cursor, context, service, config, credentialProvider);
    return httpClient
        .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofPublisher())
        .thenCompose(response -> stream(response, context))
        .thenApply(
            stream ->
                new OpenAiCompatibleEventPublisher(
                    stream.body(),
                    objectMapper,
                    resumedCommand(context, service),
                    context,
                    service.serviceId(),
                    stream.downstreamResponseId(),
                    config.maxFrameBytes(),
                    config.firstEventTimeout(),
                    config.idleEventTimeout(),
                    config.totalExecutionTimeout()));
  }

  private HttpRequest httpRequest(
      OpenAiCompatibleRequest downstreamRequest, OpenAiCompatibleAdapterConfig config) {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(downstreamRequest.endpoint())
            .timeout(config.openTimeout())
            .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes(downstreamRequest)));
    downstreamRequest.headers().forEach(builder::header);
    return builder.build();
  }

  private HttpRequest cancelRequest(
      ExecutionHandle handle,
      ExecutionContext context,
      ServiceDescriptor service,
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleCredentialProvider credentialProvider) {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(cancelUri(handle, config))
            .timeout(config.openTimeout())
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{}"));
    if (config.authHeaderName() != null) {
      credentialProvider
          .bearerToken(service, context)
          .ifPresent(token -> builder.header(config.authHeaderName(), "Bearer " + token));
    }
    return builder.build();
  }

  private HttpRequest queryStateRequest(
      ExecutionHandle handle,
      ExecutionContext context,
      ServiceDescriptor service,
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleCredentialProvider credentialProvider) {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(endpointUri(config.queryStateEndpointTemplate(), handle))
            .timeout(config.openTimeout())
            .header("Accept", "application/json")
            .GET();
    if (config.authHeaderName() != null) {
      credentialProvider
          .bearerToken(service, context)
          .ifPresent(token -> builder.header(config.authHeaderName(), "Bearer " + token));
    }
    return builder.build();
  }

  private HttpRequest submitInputRequest(
      SubmitInputCommand command,
      ExecutionHandle handle,
      ExecutionContext context,
      ServiceDescriptor service,
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleCredentialProvider credentialProvider) {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(endpointUri(config.submitInputEndpointTemplate(), handle))
            .timeout(config.openTimeout())
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(submitInputBody(command, handle)));
    if (config.authHeaderName() != null) {
      credentialProvider
          .bearerToken(service, context)
          .ifPresent(token -> builder.header(config.authHeaderName(), "Bearer " + token));
    }
    return builder.build();
  }

  private HttpRequest resumeRequest(
      ExecutionHandle handle,
      String cursor,
      ExecutionContext context,
      ServiceDescriptor service,
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleCredentialProvider credentialProvider) {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(endpointUri(config.resumeEndpointTemplate(), handle))
            .timeout(config.openTimeout())
            .header("Accept", "text/event-stream")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(resumeBody(context, handle, cursor)));
    if (config.authHeaderName() != null) {
      credentialProvider
          .bearerToken(service, context)
          .ifPresent(token -> builder.header(config.authHeaderName(), "Bearer " + token));
    }
    return builder.build();
  }

  private static URI cancelUri(ExecutionHandle handle, OpenAiCompatibleAdapterConfig config) {
    return endpointUri(config.cancelEndpointTemplate(), handle);
  }

  private static URI endpointUri(String template, ExecutionHandle handle) {
    return URI.create(
        template.replace("{downstreamRunId}", encodePathSegment(handle.downstreamRunId())));
  }

  private byte[] bodyBytes(OpenAiCompatibleRequest downstreamRequest) {
    try {
      return objectMapper.writeValueAsBytes(downstreamRequest.body());
    } catch (JsonProcessingException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  private byte[] submitInputBody(SubmitInputCommand command, ExecutionHandle handle) {
    try {
      return objectMapper.writeValueAsBytes(
          Map.of(
              "runId",
              command.runId().value(),
              "tenantId",
              command.tenantId().value(),
              "downstreamRunId",
              handle.downstreamRunId(),
              "payload",
              command.payload()));
    } catch (JsonProcessingException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  private byte[] resumeBody(ExecutionContext context, ExecutionHandle handle, String cursor) {
    try {
      return objectMapper.writeValueAsBytes(
          Map.of(
              "runId",
              context.runId().value(),
              "tenantId",
              context.tenantId().value(),
              "downstreamRunId",
              handle.downstreamRunId(),
              "cursor",
              cursor));
    } catch (JsonProcessingException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  private static CreateRunCommand resumedCommand(
      ExecutionContext context, ServiceDescriptor service) {
    String conversationId = context.attributes().get("conversationId");
    if (conversationId == null || conversationId.isBlank()) {
      throw new OpenAiCompatibleAdapterException(
          "OpenAI-compatible resume requires conversationId context attribute");
    }
    return new CreateRunCommand(
        new ConversationId(conversationId),
        new TenantId(context.tenantId().value()),
        service.serviceId(),
        null,
        List.of(),
        Map.of());
  }

  private CompletionStage<OpenAiCompatibleStream> stream(
      HttpResponse<Flow.Publisher<List<ByteBuffer>>> response, ExecutionContext context) {
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      return CompletableFuture.failedFuture(
          new OpenAiCompatibleAdapterException(
              "OpenAI-compatible downstream returned HTTP " + response.statusCode()));
    }
    return CompletableFuture.completedFuture(
        new OpenAiCompatibleStream(
            responseId(response, context), new ByteBufferListPublisher(response.body())));
  }

  private static String responseId(
      HttpResponse<Flow.Publisher<List<ByteBuffer>>> response, ExecutionContext context) {
    for (String header : RESPONSE_ID_HEADERS) {
      Optional<String> value = response.headers().firstValue(header);
      if (value.isPresent() && !value.get().isBlank()) {
        return value.get();
      }
    }
    return "http_" + context.runId().value();
  }

  private static String encodePathSegment(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }

  private DownstreamExecutionState downstreamState(String body) {
    try {
      JsonNode root = objectMapper.readTree(body);
      return stateFromText(firstText(root, "status", "state"));
    } catch (JsonProcessingException ex) {
      throw new OpenAiCompatibleAdapterException("Invalid OpenAI-compatible queryState JSON", ex);
    }
  }

  private static String firstText(JsonNode node, String first, String second) {
    JsonNode value = node.get(first);
    if (value != null && !value.isNull()) {
      return value.asText();
    }
    value = node.get(second);
    return value == null || value.isNull() ? "" : value.asText();
  }

  private static DownstreamExecutionState stateFromText(String value) {
    return switch (value.toLowerCase().replace("-", "_")) {
      case "started", "queued", "created" -> DownstreamExecutionState.STARTED;
      case "running", "in_progress", "processing" -> DownstreamExecutionState.RUNNING;
      case "waiting_input", "input_required", "requires_action" ->
          DownstreamExecutionState.WAITING_INPUT;
      case "completed", "succeeded", "success" -> DownstreamExecutionState.COMPLETED;
      case "failed", "error" -> DownstreamExecutionState.FAILED;
      case "cancel_requested", "cancelling", "canceling" ->
          DownstreamExecutionState.CANCEL_REQUESTED;
      case "canceled", "cancelled" -> DownstreamExecutionState.CANCELED;
      default -> DownstreamExecutionState.UNKNOWN;
    };
  }

  private static final class ByteBufferListPublisher implements Flow.Publisher<byte[]> {

    private final Flow.Publisher<List<ByteBuffer>> upstream;

    private ByteBufferListPublisher(Flow.Publisher<List<ByteBuffer>> upstream) {
      this.upstream = Objects.requireNonNull(upstream, "upstream must not be null");
    }

    @Override
    public void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
      upstream.subscribe(new ByteBufferListSubscriber(subscriber));
    }
  }

  private static final class ByteBufferListSubscriber
      implements Flow.Subscriber<List<ByteBuffer>>, Flow.Subscription {

    private final Flow.Subscriber<? super byte[]> downstream;
    private final AtomicBoolean once = new AtomicBoolean();
    private Flow.Subscription upstream;
    private boolean terminated;

    private ByteBufferListSubscriber(Flow.Subscriber<? super byte[]> downstream) {
      this.downstream = Objects.requireNonNull(downstream, "downstream must not be null");
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      if (!once.compareAndSet(false, true)) {
        subscription.cancel();
        return;
      }
      upstream = subscription;
      downstream.onSubscribe(this);
    }

    @Override
    public void request(long requested) {
      if (requested <= 0 || terminated) {
        return;
      }
      upstream.request(requested);
    }

    @Override
    public void cancel() {
      terminated = true;
      upstream.cancel();
    }

    @Override
    public void onNext(List<ByteBuffer> item) {
      if (!terminated) {
        downstream.onNext(bytes(item));
      }
    }

    @Override
    public void onError(Throwable throwable) {
      if (!terminated) {
        terminated = true;
        downstream.onError(throwable);
      }
    }

    @Override
    public void onComplete() {
      if (!terminated) {
        terminated = true;
        downstream.onComplete();
      }
    }

    private static byte[] bytes(List<ByteBuffer> buffers) {
      int size = buffers.stream().mapToInt(ByteBuffer::remaining).sum();
      byte[] bytes = new byte[size];
      int offset = 0;
      for (ByteBuffer buffer : buffers) {
        ByteBuffer duplicate = buffer.duplicate();
        int length = duplicate.remaining();
        duplicate.get(bytes, offset, length);
        offset += length;
      }
      return bytes;
    }
  }
}
