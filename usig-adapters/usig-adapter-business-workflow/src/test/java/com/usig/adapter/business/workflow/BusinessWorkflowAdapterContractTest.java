package com.usig.adapter.business.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.InputPart;
import com.usig.application.command.InputPartType;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.port.AdapterExecution;
import com.usig.application.port.DownstreamExecutionState;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.error.ErrorCode;
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
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class BusinessWorkflowAdapterContractTest {

  @Test
  void mapsBusinessWorkflowEventsToGatewayEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                event("workflow.started", "biz-1"),
                eventWithNode("node.progress", "biz-2", "retrieve", 40),
                new BusinessWorkflowEvent(
                    "retrieval.document",
                    "biz-3",
                    "retrieve",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "doc-1",
                    "rag.document",
                    Map.of("title", "Policy"),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    Map.of(),
                    Map.of()),
                new BusinessWorkflowEvent(
                    "answer.delta",
                    "biz-4",
                    "generate",
                    "msg-1",
                    "hello",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    Map.of(),
                    Map.of()),
                new BusinessWorkflowEvent(
                    "tool.result",
                    "biz-5",
                    null,
                    null,
                    null,
                    null,
                    "tool-1",
                    "credit_check",
                    null,
                    Map.of("score", 720),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    Map.of(),
                    Map.of()),
                new BusinessWorkflowEvent(
                    "input.required",
                    "biz-6",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "input-1",
                    "Need income file",
                    Map.of("type", "object"),
                    null,
                    null,
                    null,
                    Map.of(),
                    Map.of()),
                new BusinessWorkflowEvent(
                    "approval.required",
                    "biz-7",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "approval-1",
                    "submit_loan",
                    Map.of("amount", 100),
                    Map.of(),
                    Map.of()),
                new BusinessWorkflowEvent(
                    "usage.updated",
                    "biz-8",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    Map.of("input_tokens", 3, "output_tokens", 2, "total_tokens", 5),
                    Map.of()),
                event("workflow.completed", "biz-9")));
    BusinessWorkflowAdapter adapter = new BusinessWorkflowAdapter(config(), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 20);

    assertEquals("workflow-1", execution.handle().downstreamTaskId());
    assertEquals(EventType.EXECUTION_STARTED, events.get(0).eventType());
    assertEquals(EventType.PROGRESS_UPDATED, events.get(1).eventType());
    assertEquals("retrieve", events.get(1).payload().get("nodeId"));
    assertEquals(40, events.get(1).payload().get("progressPercent"));
    assertEquals(EventType.ARTIFACT_UPDATED, events.get(2).eventType());
    assertEquals("doc-1", events.get(2).payload().get("artifactId"));
    assertEquals(EventType.TEXT_DELTA, events.get(3).eventType());
    assertEquals("hello", events.get(3).payload().get("content"));
    assertEquals(EventType.TOOL_RESULT, events.get(4).eventType());
    assertEquals("tool-1", events.get(4).payload().get("toolCallId"));
    assertEquals(EventType.INPUT_REQUIRED, events.get(5).eventType());
    assertEquals("input-1", events.get(5).payload().get("inputRequestId"));
    assertEquals(EventType.APPROVAL_REQUIRED, events.get(6).eventType());
    assertEquals("approval-1", events.get(6).payload().get("approvalRequestId"));
    assertEquals(EventType.USAGE_UPDATED, events.get(7).eventType());
    assertEquals(5, events.get(7).payload().get("total_tokens"));
    assertEquals(EventType.EXECUTION_COMPLETED, events.get(8).eventType());
    assertEquals("biz-9", events.get(8).sourceEventId());
  }

  @Test
  void preservesUnknownBusinessEventAsRawExtension() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                new BusinessWorkflowEvent(
                    "underwriting.score.updated",
                    "biz-custom-1",
                    "underwrite",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    Map.of(),
                    Map.of("score", 88))));
    BusinessWorkflowAdapter adapter = new BusinessWorkflowAdapter(config(), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(EventType.EXTENSION, events.getFirst().eventType());
    assertEquals("business.workflow.raw", events.getFirst().payload().get("extensionType"));
    assertEquals(
        "underwriting.score.updated", events.getFirst().payload().get("downstreamEventType"));
    assertEquals("biz-custom-1", events.getFirst().sourceEventId());
  }

  @Test
  void supportsOnlyConfiguredBusinessWorkflowService() {
    BusinessWorkflowAdapter adapter = new BusinessWorkflowAdapter(config(), new RecordingClient());

    assertTrue(adapter.supports(service()));
    ServiceDescriptor otherAdapter =
        new ServiceDescriptor(
            "loan-workflow",
            ServiceType.WORKFLOW,
            URI.create("https://workflow.example.com/runs"),
            "generic-sse-json",
            Set.of("tenant_1"),
            Set.of(Transport.SSE),
            new ServiceCapabilities(true, true, true, false, true, true, false, false),
            1);

    assertTrue(!adapter.supports(otherAdapter));
  }

  @Test
  void delegatesCancelQuerySubmitInputAndResumeToClient() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(event("workflow.started", "biz-1")),
            completedStream(event("workflow.running", "biz-resume-1")));
    BusinessWorkflowAdapter adapter = new BusinessWorkflowAdapter(config(), client);
    ExecutionHandle handle =
        new ExecutionHandle(
            service().serviceId(),
            BusinessWorkflowAdapter.ADAPTER_ID,
            "workflow-1",
            null,
            "run_1",
            Map.of());

    assertEquals(
        DownstreamExecutionState.CANCELED,
        adapter.cancel(handle, context()).toCompletableFuture().get());
    assertEquals(
        DownstreamExecutionState.RUNNING,
        adapter.queryState(handle, context()).toCompletableFuture().get());
    adapter
        .submitInput(
            new SubmitInputCommand(new RunId("run_1"), new TenantId("tenant_1"), Map.of("x", 1)),
            context())
        .toCompletableFuture()
        .get();
    Flow.Publisher<GatewayEvent> resumed =
        adapter.resume(handle, "cursor-1", context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(resumed, 10);

    assertEquals(1, client.cancelCalls);
    assertEquals(1, client.queryCalls);
    assertEquals(1, client.submitInputCalls);
    assertEquals("cursor-1", client.lastResumeCursor);
    assertEquals(EventType.EXECUTION_RUNNING, events.getFirst().eventType());
  }

  @Test
  void timesOutWhenOpenDoesNotComplete() {
    RecordingClient client = new RecordingClient();
    client.openStream = new CompletableFuture<>();
    BusinessWorkflowAdapter adapter =
        new BusinessWorkflowAdapter(config(Duration.ofMillis(10)), client);

    ExecutionException error =
        assertThrows(
            ExecutionException.class,
            () ->
                adapter
                    .execute(command(), context())
                    .toCompletableFuture()
                    .get(1, TimeUnit.SECONDS));

    BusinessWorkflowAdapterException cause =
        assertInstanceOf(BusinessWorkflowAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_CONNECT_FAILED, cause.errorCode());
  }

  @Test
  void timesOutWhenCancelDoesNotComplete() {
    RecordingClient client = new RecordingClient();
    client.cancelResult = new CompletableFuture<>();
    BusinessWorkflowAdapter adapter =
        new BusinessWorkflowAdapter(
            new BusinessWorkflowAdapterConfig(
                service(), Duration.ofSeconds(1), Duration.ofMillis(10)),
            client);
    ExecutionHandle handle =
        new ExecutionHandle(
            service().serviceId(),
            BusinessWorkflowAdapter.ADAPTER_ID,
            "workflow-1",
            null,
            "run_1",
            Map.of());

    ExecutionException error =
        assertThrows(
            ExecutionException.class,
            () -> adapter.cancel(handle, context()).toCompletableFuture().get(1, TimeUnit.SECONDS));

    BusinessWorkflowAdapterException cause =
        assertInstanceOf(BusinessWorkflowAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_CANCEL_TIMEOUT, cause.errorCode());
  }

  @Test
  void timesOutWhenFirstBusinessEventDoesNotArrive() throws Exception {
    RecordingClient client =
        new RecordingClient(
            CompletableFuture.completedFuture(
                new BusinessWorkflowStream("workflow-1", new NeverEventPublisher())));
    BusinessWorkflowAdapter adapter =
        new BusinessWorkflowAdapter(
            config(
                Duration.ofSeconds(1),
                Duration.ofMillis(10),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)),
            client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    ExecutionException error =
        assertThrows(ExecutionException.class, () -> collect(execution.events(), 2));

    BusinessWorkflowAdapterException cause =
        assertInstanceOf(BusinessWorkflowAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_FIRST_EVENT_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("firstEventTimeout"));
  }

  @Test
  void timesOutWhenBusinessEventStreamBecomesIdle() throws Exception {
    RecordingClient client =
        new RecordingClient(
            CompletableFuture.completedFuture(
                new BusinessWorkflowStream(
                    "workflow-1",
                    new OneAndStallEventPublisher(event("workflow.started", "biz-1")))));
    BusinessWorkflowAdapter adapter =
        new BusinessWorkflowAdapter(
            config(
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofMillis(10),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)),
            client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    ExecutionException error =
        assertThrows(ExecutionException.class, () -> collect(execution.events(), 2));

    BusinessWorkflowAdapterException cause =
        assertInstanceOf(BusinessWorkflowAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_IDLE_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("idleEventTimeout"));
  }

  @Test
  void timesOutWhenBusinessWorkflowExecutionExceedsTotalTimeout() throws Exception {
    RecordingClient client =
        new RecordingClient(
            CompletableFuture.completedFuture(
                new BusinessWorkflowStream(
                    "workflow-1",
                    new OneAndStallEventPublisher(event("workflow.started", "biz-1")))));
    BusinessWorkflowAdapter adapter =
        new BusinessWorkflowAdapter(
            config(
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofMillis(10),
                Duration.ofSeconds(1)),
            client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    ExecutionException error =
        assertThrows(ExecutionException.class, () -> collect(execution.events(), 2));

    BusinessWorkflowAdapterException cause =
        assertInstanceOf(BusinessWorkflowAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_TOTAL_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("totalExecutionTimeout"));
  }

  private static BusinessWorkflowEvent event(String type, String eventId) {
    return BusinessWorkflowEvent.of(type, eventId);
  }

  private static BusinessWorkflowEvent eventWithNode(
      String type, String eventId, String nodeId, int progressPercent) {
    return new BusinessWorkflowEvent(
        type,
        eventId,
        nodeId,
        null,
        null,
        progressPercent,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        Map.of(),
        Map.of());
  }

  private static BusinessWorkflowAdapterConfig config() {
    return config(Duration.ofSeconds(1));
  }

  private static BusinessWorkflowAdapterConfig config(Duration openTimeout) {
    return new BusinessWorkflowAdapterConfig(service(), openTimeout, Duration.ofSeconds(1));
  }

  private static BusinessWorkflowAdapterConfig config(
      Duration openTimeout,
      Duration firstEventTimeout,
      Duration idleEventTimeout,
      Duration totalExecutionTimeout,
      Duration cancelTimeout) {
    return new BusinessWorkflowAdapterConfig(
        service(),
        openTimeout,
        firstEventTimeout,
        idleEventTimeout,
        totalExecutionTimeout,
        cancelTimeout);
  }

  private static CreateRunCommand command() {
    return new CreateRunCommand(
        new ConversationId("conv_1"),
        new TenantId("tenant_1"),
        service().serviceId(),
        "idem_1",
        List.of(new InputPart(InputPartType.TEXT, "check loan", null)),
        Map.of("businessCase", "loan"));
  }

  private static ExecutionContext context() {
    return new ExecutionContext(
        new RunId("run_1"),
        new TenantId("tenant_1"),
        "trace_1",
        Map.of("conversationId", "conv_1"));
  }

  private static ServiceDescriptor service() {
    return new ServiceDescriptor(
        "loan-workflow",
        ServiceType.WORKFLOW,
        URI.create("https://workflow.example.com/runs"),
        BusinessWorkflowAdapter.ADAPTER_ID,
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, true, false, true, true, false, false),
        1);
  }

  private static CompletionStage<BusinessWorkflowStream> completedStream(
      BusinessWorkflowEvent... events) {
    return CompletableFuture.completedFuture(
        new BusinessWorkflowStream("workflow-1", new EventPublisher(List.of(events))));
  }

  private static List<GatewayEvent> collect(Flow.Publisher<GatewayEvent> publisher, int limit)
      throws Exception {
    CollectingSubscriber subscriber = new CollectingSubscriber(limit);
    publisher.subscribe(subscriber);
    return subscriber.result.get(1, TimeUnit.SECONDS);
  }

  private static final class RecordingClient implements BusinessWorkflowClient {

    private CompletionStage<BusinessWorkflowStream> openStream =
        completedStream(event("workflow.started", "biz-1"));
    private CompletionStage<BusinessWorkflowStream> resumeStream = openStream;
    private int cancelCalls;
    private int queryCalls;
    private int submitInputCalls;
    private String lastResumeCursor;
    private CompletionStage<Void> cancelResult = CompletableFuture.completedFuture(null);

    private RecordingClient() {}

    private RecordingClient(CompletionStage<BusinessWorkflowStream> openStream) {
      this(openStream, openStream);
    }

    private RecordingClient(
        CompletionStage<BusinessWorkflowStream> openStream,
        CompletionStage<BusinessWorkflowStream> resumeStream) {
      this.openStream = openStream;
      this.resumeStream = resumeStream;
    }

    @Override
    public CompletionStage<BusinessWorkflowStream> open(
        CreateRunCommand request, ExecutionContext context, BusinessWorkflowAdapterConfig config) {
      return openStream;
    }

    @Override
    public CompletionStage<Void> cancel(
        ExecutionHandle handle, ExecutionContext context, BusinessWorkflowAdapterConfig config) {
      cancelCalls++;
      return cancelResult;
    }

    @Override
    public CompletionStage<DownstreamExecutionState> queryState(
        ExecutionHandle handle, ExecutionContext context, BusinessWorkflowAdapterConfig config) {
      queryCalls++;
      return CompletableFuture.completedFuture(DownstreamExecutionState.RUNNING);
    }

    @Override
    public CompletionStage<BusinessWorkflowStream> resume(
        ExecutionHandle handle,
        String cursor,
        ExecutionContext context,
        BusinessWorkflowAdapterConfig config) {
      lastResumeCursor = cursor;
      return resumeStream;
    }

    @Override
    public CompletionStage<Void> submitInput(
        SubmitInputCommand command,
        ExecutionContext context,
        BusinessWorkflowAdapterConfig config) {
      submitInputCalls++;
      return CompletableFuture.completedFuture(null);
    }
  }

  private static final class EventPublisher implements Flow.Publisher<BusinessWorkflowEvent> {

    private final List<BusinessWorkflowEvent> events;

    private EventPublisher(List<BusinessWorkflowEvent> events) {
      this.events = events;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super BusinessWorkflowEvent> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            private int index;
            private boolean canceled;
            private boolean completed;

            @Override
            public void request(long n) {
              if (n <= 0 || canceled || completed) {
                return;
              }
              long emitted = 0;
              while (emitted < n && index < events.size() && !canceled) {
                subscriber.onNext(events.get(index));
                index++;
                emitted++;
              }
              if (index == events.size() && !canceled && !completed) {
                completed = true;
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

  private static final class NeverEventPublisher implements Flow.Publisher<BusinessWorkflowEvent> {

    @Override
    public void subscribe(Flow.Subscriber<? super BusinessWorkflowEvent> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            @Override
            public void request(long n) {}

            @Override
            public void cancel() {}
          });
    }
  }

  private static final class OneAndStallEventPublisher
      implements Flow.Publisher<BusinessWorkflowEvent> {

    private final BusinessWorkflowEvent event;

    private OneAndStallEventPublisher(BusinessWorkflowEvent event) {
      this.event = event;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super BusinessWorkflowEvent> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            private boolean emitted;
            private boolean canceled;

            @Override
            public void request(long n) {
              if (n > 0 && !emitted && !canceled) {
                emitted = true;
                subscriber.onNext(event);
              }
            }

            @Override
            public void cancel() {
              canceled = true;
            }
          });
    }
  }

  private static final class CollectingSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final int limit;
    private final List<GatewayEvent> events = new ArrayList<>();
    private final CompletableFuture<List<GatewayEvent>> result = new CompletableFuture<>();
    private Flow.Subscription subscription;

    private CollectingSubscriber(int limit) {
      this.limit = limit;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      this.subscription = subscription;
      subscription.request(limit);
    }

    @Override
    public void onNext(GatewayEvent item) {
      events.add(item);
      if (events.size() >= limit) {
        subscription.cancel();
        result.complete(List.copyOf(events));
      }
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
