package com.usig.adapter.openai;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.InputPart;
import com.usig.application.command.InputPartType;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.port.AdapterExecution;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.ConversationId;
import com.usig.domain.run.ExecutionHandle;
import com.usig.domain.run.RunId;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class JavaHttpOpenAiCompatibleClientTest {

  private HttpServer server;

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void postsMappedRequestAndExposesStreamingResponse() throws Exception {
    AtomicReference<String> method = new AtomicReference<>();
    AtomicReference<String> path = new AtomicReference<>();
    AtomicReference<String> auth = new AtomicReference<>();
    AtomicReference<String> accept = new AtomicReference<>();
    AtomicReference<String> contentType = new AtomicReference<>();
    AtomicReference<String> body = new AtomicReference<>();
    server =
        server(
            exchange -> {
              method.set(exchange.getRequestMethod());
              path.set(exchange.getRequestURI().getPath());
              auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
              accept.set(exchange.getRequestHeaders().getFirst("Accept"));
              contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
              body.set(
                  new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
              respond(
                  exchange,
                  200,
                  Map.of("Content-Type", "text/event-stream", "x-request-id", "req-1"),
                  """
                  data:{"id":"chatcmpl-1","choices":[{"index":0,"delta":{"content":"hello"}}]}

                  data:[DONE]

                  """);
            });
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            config(serverUri("/v1/chat/completions")),
            new JavaHttpOpenAiCompatibleClient(HttpClient.newHttpClient(), new ObjectMapper()));

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals("POST", method.get());
    assertEquals("/v1/chat/completions", path.get());
    assertEquals("Bearer token-1", auth.get());
    assertEquals("text/event-stream", accept.get());
    assertTrue(contentType.get().contains("application/json"));
    assertTrue(body.get().contains("\"model\":\"gpt-4o-mini\""));
    assertTrue(body.get().contains("\"stream\":true"));
    assertEquals("req-1", execution.handle().downstreamRunId());
    assertEquals(2, events.size());
    assertEquals(EventType.TEXT_DELTA, events.getFirst().eventType());
    assertEquals("hello", events.getFirst().payload().get("content"));
    assertEquals(EventType.EXECUTION_COMPLETED, events.get(1).eventType());
  }

  @Test
  void mapsNonSuccessHttpStatusToAdapterException() throws Exception {
    server =
        server(
            exchange ->
                respond(
                    exchange,
                    429,
                    Map.of("Content-Type", "application/json"),
                    "{\"error\":\"rate_limited\"}"));
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            config(serverUri("/v1/chat/completions")),
            new JavaHttpOpenAiCompatibleClient(HttpClient.newHttpClient(), new ObjectMapper()));

    CompletionException error = assertThrowsCompletion(() -> adapter.execute(command(), context()));

    assertInstanceOf(OpenAiCompatibleAdapterException.class, error.getCause());
    assertTrue(error.getCause().getMessage().contains("HTTP 429"));
  }

  @Test
  void postsCancelRequestWhenCancelEndpointIsConfigured() throws Exception {
    AtomicReference<String> method = new AtomicReference<>();
    AtomicReference<String> path = new AtomicReference<>();
    AtomicReference<String> auth = new AtomicReference<>();
    AtomicReference<String> body = new AtomicReference<>();
    server =
        server(
            "/v1/responses/resp-1/cancel",
            exchange -> {
              method.set(exchange.getRequestMethod());
              path.set(exchange.getRequestURI().getPath());
              auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
              body.set(
                  new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
              respond(exchange, 200, Map.of("Content-Type", "application/json"), "{}");
            });
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            config(
                serverUri("/v1/chat/completions"),
                serverTemplate("/v1/responses/{downstreamRunId}/cancel")),
            new JavaHttpOpenAiCompatibleClient(HttpClient.newHttpClient(), new ObjectMapper()));

    DownstreamExecutionState state =
        adapter
            .cancel(
                new ExecutionHandle(
                    "model-chat",
                    OpenAiCompatibleAdapter.ADAPTER_ID,
                    null,
                    null,
                    "resp-1",
                    Map.of()),
                context())
            .toCompletableFuture()
            .get();

    assertEquals(DownstreamExecutionState.CANCELED, state);
    assertEquals("POST", method.get());
    assertEquals("/v1/responses/resp-1/cancel", path.get());
    assertEquals("Bearer token-1", auth.get());
    assertEquals("{}", body.get());
  }

  @Test
  void getsQueryStateWhenQueryEndpointIsConfigured() throws Exception {
    AtomicReference<String> method = new AtomicReference<>();
    AtomicReference<String> path = new AtomicReference<>();
    AtomicReference<String> auth = new AtomicReference<>();
    server =
        server(
            "/v1/responses/resp-1",
            exchange -> {
              method.set(exchange.getRequestMethod());
              path.set(exchange.getRequestURI().getPath());
              auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
              respond(
                  exchange,
                  200,
                  Map.of("Content-Type", "application/json"),
                  "{\"status\":\"completed\"}");
            });
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            config(
                serverUri("/v1/chat/completions"),
                null,
                serverTemplate("/v1/responses/{downstreamRunId}")),
            new JavaHttpOpenAiCompatibleClient(HttpClient.newHttpClient(), new ObjectMapper()));

    DownstreamExecutionState state =
        adapter
            .queryState(
                new ExecutionHandle(
                    "model-chat",
                    OpenAiCompatibleAdapter.ADAPTER_ID,
                    null,
                    null,
                    "resp-1",
                    Map.of()),
                context())
            .toCompletableFuture()
            .get();

    assertEquals(DownstreamExecutionState.COMPLETED, state);
    assertEquals("GET", method.get());
    assertEquals("/v1/responses/resp-1", path.get());
    assertEquals("Bearer token-1", auth.get());
  }

  @Test
  void postsSubmitInputWhenInputEndpointIsConfigured() throws Exception {
    AtomicReference<String> method = new AtomicReference<>();
    AtomicReference<String> path = new AtomicReference<>();
    AtomicReference<String> auth = new AtomicReference<>();
    AtomicReference<String> contentType = new AtomicReference<>();
    AtomicReference<String> body = new AtomicReference<>();
    server =
        server(
            "/v1/responses/resp-1/input",
            exchange -> {
              method.set(exchange.getRequestMethod());
              path.set(exchange.getRequestURI().getPath());
              auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
              contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
              body.set(
                  new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
              respond(exchange, 202, Map.of("Content-Type", "application/json"), "{}");
            });
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            config(
                serverUri("/v1/chat/completions"),
                null,
                null,
                serverTemplate("/v1/responses/{downstreamRunId}/input")),
            new JavaHttpOpenAiCompatibleClient(HttpClient.newHttpClient(), new ObjectMapper()));

    adapter
        .submitInput(
            new SubmitInputCommand(
                new RunId("run_1"),
                new TenantId("tenant_1"),
                Map.of("approvalRequestId", "appr_1", "approved", true)),
            new ExecutionHandle(
                "model-chat", OpenAiCompatibleAdapter.ADAPTER_ID, null, null, "resp-1", Map.of()),
            context())
        .toCompletableFuture()
        .get();

    assertEquals("POST", method.get());
    assertEquals("/v1/responses/resp-1/input", path.get());
    assertEquals("Bearer token-1", auth.get());
    assertTrue(contentType.get().contains("application/json"));
    assertTrue(body.get().contains("\"runId\":\"run_1\""));
    assertTrue(body.get().contains("\"tenantId\":\"tenant_1\""));
    assertTrue(body.get().contains("\"downstreamRunId\":\"resp-1\""));
    assertTrue(body.get().contains("\"approvalRequestId\":\"appr_1\""));
    assertTrue(body.get().contains("\"approved\":true"));
    assertTrue(!body.get().contains("token-1"));
  }

  @Test
  void postsResumeRequestAndExposesStreamingResponseWhenResumeEndpointIsConfigured()
      throws Exception {
    AtomicReference<String> method = new AtomicReference<>();
    AtomicReference<String> path = new AtomicReference<>();
    AtomicReference<String> auth = new AtomicReference<>();
    AtomicReference<String> accept = new AtomicReference<>();
    AtomicReference<String> contentType = new AtomicReference<>();
    AtomicReference<String> body = new AtomicReference<>();
    server =
        server(
            "/v1/responses/resp-1/resume",
            exchange -> {
              method.set(exchange.getRequestMethod());
              path.set(exchange.getRequestURI().getPath());
              auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
              accept.set(exchange.getRequestHeaders().getFirst("Accept"));
              contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
              body.set(
                  new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
              respond(
                  exchange,
                  200,
                  Map.of("Content-Type", "text/event-stream", "x-request-id", "resume-req-1"),
                  """
                  data:{"id":"resp-1","choices":[{"index":0,"delta":{"content":"resumed"}}]}

                  data:[DONE]

                  """);
            });
    OpenAiCompatibleAdapter adapter =
        new OpenAiCompatibleAdapter(
            config(
                serverUri("/v1/chat/completions"),
                null,
                null,
                null,
                serverTemplate("/v1/responses/{downstreamRunId}/resume")),
            new JavaHttpOpenAiCompatibleClient(HttpClient.newHttpClient(), new ObjectMapper()));

    Flow.Publisher<GatewayEvent> resumed =
        adapter
            .resume(
                new ExecutionHandle(
                    "model-chat",
                    OpenAiCompatibleAdapter.ADAPTER_ID,
                    null,
                    null,
                    "resp-1",
                    Map.of()),
                "evt_9",
                contextWithConversation())
            .toCompletableFuture()
            .get();
    List<GatewayEvent> events = collect(resumed, 10);

    assertEquals("POST", method.get());
    assertEquals("/v1/responses/resp-1/resume", path.get());
    assertEquals("Bearer token-1", auth.get());
    assertEquals("text/event-stream", accept.get());
    assertTrue(contentType.get().contains("application/json"));
    assertTrue(body.get().contains("\"runId\":\"run_1\""));
    assertTrue(body.get().contains("\"tenantId\":\"tenant_1\""));
    assertTrue(body.get().contains("\"downstreamRunId\":\"resp-1\""));
    assertTrue(body.get().contains("\"cursor\":\"evt_9\""));
    assertTrue(!body.get().contains("token-1"));
    assertEquals(2, events.size());
    assertEquals(EventType.TEXT_DELTA, events.getFirst().eventType());
    assertEquals("resumed", events.getFirst().payload().get("content"));
    assertEquals("conv_1", events.getFirst().conversationId());
  }

  private static CompletionException assertThrowsCompletion(CompletionSupplier action) {
    try {
      action.get().toCompletableFuture().join();
      throw new AssertionError("Expected CompletionException");
    } catch (CompletionException ex) {
      return ex;
    }
  }

  private HttpServer server(ExchangeHandler handler) throws IOException {
    return server("/v1/chat/completions", handler);
  }

  private HttpServer server(String path, ExchangeHandler handler) throws IOException {
    HttpServer httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    httpServer.createContext(path, handler::handle);
    httpServer.setExecutor(Executors.newSingleThreadExecutor());
    httpServer.start();
    return httpServer;
  }

  private URI serverUri(String path) {
    return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
  }

  private String serverTemplate(String path) {
    return "http://127.0.0.1:" + server.getAddress().getPort() + path;
  }

  private static void respond(
      HttpExchange exchange, int status, Map<String, String> headers, String response)
      throws IOException {
    headers.forEach((name, value) -> exchange.getResponseHeaders().set(name, value));
    byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }

  private static OpenAiCompatibleAdapterConfig config(URI endpoint) {
    return config(endpoint, null);
  }

  private static OpenAiCompatibleAdapterConfig config(URI endpoint, String cancelEndpointTemplate) {
    return config(endpoint, cancelEndpointTemplate, null, null);
  }

  private static OpenAiCompatibleAdapterConfig config(
      URI endpoint, String cancelEndpointTemplate, String queryStateEndpointTemplate) {
    return config(endpoint, cancelEndpointTemplate, queryStateEndpointTemplate, null);
  }

  private static OpenAiCompatibleAdapterConfig config(
      URI endpoint,
      String cancelEndpointTemplate,
      String queryStateEndpointTemplate,
      String submitInputEndpointTemplate) {
    return config(
        endpoint,
        cancelEndpointTemplate,
        queryStateEndpointTemplate,
        submitInputEndpointTemplate,
        null);
  }

  private static OpenAiCompatibleAdapterConfig config(
      URI endpoint,
      String cancelEndpointTemplate,
      String queryStateEndpointTemplate,
      String submitInputEndpointTemplate,
      String resumeEndpointTemplate) {
    return new OpenAiCompatibleAdapterConfig(
        service(endpoint),
        4096,
        Duration.ofSeconds(2),
        "gpt-4o-mini",
        "Authorization",
        "token-1",
        OpenAiCompatibleApiMode.CHAT_COMPLETIONS,
        cancelEndpointTemplate,
        queryStateEndpointTemplate,
        submitInputEndpointTemplate,
        resumeEndpointTemplate);
  }

  private static ServiceDescriptor service(URI endpoint) {
    return new ServiceDescriptor(
        "model-chat",
        ServiceType.MODEL,
        endpoint,
        OpenAiCompatibleAdapter.ADAPTER_ID,
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, false, false, false, false, true, true),
        1);
  }

  private static CreateRunCommand command() {
    return new CreateRunCommand(
        new ConversationId("conv_1"),
        new TenantId("tenant_1"),
        "model-chat",
        "idem-1",
        List.of(new InputPart(InputPartType.TEXT, "hello", null)),
        Map.of());
  }

  private static ExecutionContext context() {
    return new ExecutionContext(new RunId("run_1"), new TenantId("tenant_1"), "trace-1", Map.of());
  }

  private static ExecutionContext contextWithConversation() {
    return new ExecutionContext(
        new RunId("run_1"),
        new TenantId("tenant_1"),
        "trace-1",
        Map.of("conversationId", "conv_1"));
  }

  private static List<GatewayEvent> collect(Flow.Publisher<GatewayEvent> publisher, long request) {
    CollectingSubscriber subscriber = new CollectingSubscriber(request);
    publisher.subscribe(subscriber);
    return subscriber.result.join();
  }

  @FunctionalInterface
  private interface ExchangeHandler {
    void handle(HttpExchange exchange) throws IOException;
  }

  @FunctionalInterface
  private interface CompletionSupplier {
    CompletionStage<AdapterExecution> get();
  }

  private static final class CollectingSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final long request;
    private final List<GatewayEvent> events = new ArrayList<>();
    private final java.util.concurrent.CompletableFuture<List<GatewayEvent>> result =
        new java.util.concurrent.CompletableFuture<>();

    private CollectingSubscriber(long request) {
      this.request = request;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscription.request(request);
    }

    @Override
    public void onNext(GatewayEvent item) {
      events.add(item);
    }

    @Override
    public void onError(Throwable throwable) {
      result.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
      result.complete(List.copyOf(events));
    }
  }
}
