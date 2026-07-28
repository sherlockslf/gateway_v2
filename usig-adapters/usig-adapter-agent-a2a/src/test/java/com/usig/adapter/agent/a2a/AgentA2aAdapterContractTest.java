package com.usig.adapter.agent.a2a;

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
import java.nio.charset.StandardCharsets;
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

class AgentA2aAdapterContractTest {

  @Test
  void mapsA2aTaskMessageArtifactAndInputEventsToGatewayEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                id:a2a-1
                data:{"type":"task.status","task_id":"task-1","status":"started"}

                id:a2a-2
                data:{"type":"message.part","task_id":"task-1","message_id":"msg-1","part_type":"text","text":"hello"}

                id:a2a-3
                data:{"type":"artifact.created","task_id":"task-1","artifact_id":"art-1","artifact_type":"document","name":"report"}

                id:a2a-4
                data:{"type":"input.required","task_id":"task-1","input_request_id":"input-1","prompt":"Need file"}

                id:a2a-5
                data:{"type":"approval.required","task_id":"task-1","approval_request_id":"approval-1","name":"charge","arguments":{"amount":10}}

                id:a2a-6
                data:{"type":"task.status","task_id":"task-1","status":"completed"}

                """));
    AgentA2aAdapter adapter = new AgentA2aAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals("task-1", execution.handle().downstreamTaskId());
    assertEquals(EventType.EXECUTION_STARTED, events.get(0).eventType());
    assertEquals(EventType.TEXT_DELTA, events.get(1).eventType());
    assertEquals("hello", events.get(1).payload().get("content"));
    assertEquals(EventType.ARTIFACT_CREATED, events.get(2).eventType());
    assertEquals("art-1", events.get(2).payload().get("artifactId"));
    assertEquals(EventType.INPUT_REQUIRED, events.get(3).eventType());
    assertEquals("input-1", events.get(3).payload().get("inputRequestId"));
    assertEquals(EventType.APPROVAL_REQUIRED, events.get(4).eventType());
    assertEquals("approval-1", events.get(4).payload().get("approvalRequestId"));
    assertEquals(EventType.EXECUTION_COMPLETED, events.get(5).eventType());
    assertEquals("a2a-6", events.get(5).sourceEventId());
  }

  @Test
  void mapsNonTextMessagePartAndUnknownEventsWithoutDroppingSemantics() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                data:{"type":"message.part","task_id":"task-1","message_id":"msg-2","part_type":"file","part":{"uri":"s3://bucket/file.pdf"}}

                data:{"type":"agent.custom","task_id":"task-1","value":42}

                """));
    AgentA2aAdapter adapter = new AgentA2aAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(EventType.ARTIFACT_UPDATED, events.get(0).eventType());
    assertEquals("agent.message.part", events.get(0).payload().get("artifactType"));
    assertEquals(EventType.EXTENSION, events.get(1).eventType());
    assertEquals("agent.a2a.raw", events.get(1).payload().get("extensionType"));
    assertEquals("agent.custom", events.get(1).payload().get("downstreamEventType"));
  }

  @Test
  void decodesChunkedFramesAndMultipleDataLines() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                "data:{\"type\":\"message.part\",\"task_id\":\"task-1\",\"message_id\":\"m",
                "sg-1\",\"part_type\":\"text\",\n",
                "data:\"text\":\"hello\"}\n\n"));
    AgentA2aAdapter adapter = new AgentA2aAdapter(config(4096), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(1, events.size());
    assertEquals("hello", events.getFirst().payload().get("content"));
  }

  @Test
  void rejectsInvalidJsonData() throws Exception {
    RecordingClient client = new RecordingClient(completedStream("data:not-json\n\n"));
    AgentA2aAdapter adapter = new AgentA2aAdapter(config(512), client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    ExecutionException error =
        assertThrows(ExecutionException.class, () -> collect(execution.events(), 1));

    assertInstanceOf(AgentA2aAdapterException.class, error.getCause());
  }

  @Test
  void rejectsOverlongFrame() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream("data:{\"type\":\"message.part\",\"text\":\"too long\"}\n\n"));
    AgentA2aAdapter adapter = new AgentA2aAdapter(config(16), client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    ExecutionException error =
        assertThrows(ExecutionException.class, () -> collect(execution.events(), 1));

    assertInstanceOf(AgentA2aAdapterException.class, error.getCause());
    assertTrue(error.getCause().getMessage().contains("maxFrameBytes"));
  }

  @Test
  void timesOutWhenDownstreamOpenDoesNotComplete() {
    RecordingClient client = new RecordingClient(new CompletableFuture<>());
    AgentA2aAdapter adapter = new AgentA2aAdapter(config(512, Duration.ofMillis(10)), client);

    ExecutionException error =
        assertThrows(
            ExecutionException.class,
            () ->
                adapter
                    .execute(command(), context())
                    .toCompletableFuture()
                    .get(1, TimeUnit.SECONDS));

    AgentA2aAdapterException cause =
        assertInstanceOf(AgentA2aAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_CONNECT_FAILED, cause.errorCode());
  }

  @Test
  void timesOutWhenFirstEventDoesNotArrive() throws Exception {
    RecordingClient client =
        new RecordingClient(
            CompletableFuture.completedFuture(
                new AgentA2aStream("task-1", new HangingPublisher())));
    AgentA2aAdapter adapter =
        new AgentA2aAdapter(
            config(
                512,
                Duration.ofSeconds(1),
                Duration.ofMillis(10),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)),
            client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    ExecutionException error =
        assertThrows(ExecutionException.class, () -> collect(execution.events(), 1));

    AgentA2aAdapterException cause =
        assertInstanceOf(AgentA2aAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_FIRST_EVENT_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("firstEventTimeout"));
  }

  @Test
  void timesOutWhenStreamStopsAfterFirstEvent() throws Exception {
    RecordingClient client =
        new RecordingClient(
            CompletableFuture.completedFuture(
                new AgentA2aStream(
                    "task-1",
                    new SingleThenHangPublisher(
                        """
                        data:{"type":"task.status","task_id":"task-1","status":"running"}

                        """))));
    AgentA2aAdapter adapter =
        new AgentA2aAdapter(
            config(
                512,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofMillis(10),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)),
            client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    ExecutionException error =
        assertThrows(ExecutionException.class, () -> collect(execution.events(), 10));

    AgentA2aAdapterException cause =
        assertInstanceOf(AgentA2aAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_IDLE_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("idleEventTimeout"));
  }

  @Test
  void timesOutWhenTotalExecutionLimitElapses() throws Exception {
    RecordingClient client =
        new RecordingClient(
            CompletableFuture.completedFuture(
                new AgentA2aStream(
                    "task-1",
                    new SingleThenHangPublisher(
                        """
                        data:{"type":"task.status","task_id":"task-1","status":"running"}

                        """))));
    AgentA2aAdapter adapter =
        new AgentA2aAdapter(
            config(
                512,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofMillis(10),
                Duration.ofSeconds(1)),
            client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    ExecutionException error =
        assertThrows(ExecutionException.class, () -> collect(execution.events(), 10));

    AgentA2aAdapterException cause =
        assertInstanceOf(AgentA2aAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_TOTAL_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("totalExecutionTimeout"));
  }

  @Test
  void timesOutWhenCancelDoesNotComplete() {
    RecordingClient client = new RecordingClient(completedStream(""));
    client.cancelResult = new CompletableFuture<>();
    AgentA2aAdapter adapter =
        new AgentA2aAdapter(
            config(
                512,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofMillis(10)),
            client);
    ExecutionHandle handle =
        new ExecutionHandle(
            service().serviceId(), AgentA2aAdapter.ADAPTER_ID, "task-1", null, "run_1", Map.of());

    ExecutionException error =
        assertThrows(
            ExecutionException.class,
            () -> adapter.cancel(handle, context()).toCompletableFuture().get(1, TimeUnit.SECONDS));

    AgentA2aAdapterException cause =
        assertInstanceOf(AgentA2aAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_CANCEL_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("cancelTimeout"));
  }

  @Test
  void delegatesCancelQuerySubmitInputAndResumeToClient() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(""),
            completedStream(
                """
                data:{"type":"task.status","task_id":"task-1","status":"running"}

                """));
    AgentA2aAdapter adapter = new AgentA2aAdapter(config(4096), client);
    ExecutionHandle handle =
        new ExecutionHandle(
            service().serviceId(), AgentA2aAdapter.ADAPTER_ID, "task-1", null, "run_1", Map.of());

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

  private static List<GatewayEvent> collect(Flow.Publisher<GatewayEvent> publisher, int limit)
      throws Exception {
    CollectingSubscriber subscriber = new CollectingSubscriber(limit);
    publisher.subscribe(subscriber);
    return subscriber.result.get(1, TimeUnit.SECONDS);
  }

  private static AgentA2aAdapterConfig config(int maxFrameBytes) {
    return config(maxFrameBytes, Duration.ofSeconds(1));
  }

  private static AgentA2aAdapterConfig config(int maxFrameBytes, Duration openTimeout) {
    return new AgentA2aAdapterConfig(
        service(), maxFrameBytes, openTimeout, Duration.ofSeconds(1), "Authorization", "token-1");
  }

  private static AgentA2aAdapterConfig config(
      int maxFrameBytes,
      Duration openTimeout,
      Duration firstEventTimeout,
      Duration idleEventTimeout,
      Duration totalExecutionTimeout,
      Duration cancelTimeout) {
    return new AgentA2aAdapterConfig(
        service(),
        maxFrameBytes,
        openTimeout,
        firstEventTimeout,
        idleEventTimeout,
        totalExecutionTimeout,
        cancelTimeout,
        "Authorization",
        "token-1");
  }

  private static CreateRunCommand command() {
    return new CreateRunCommand(
        new ConversationId("conv_1"),
        new TenantId("tenant_1"),
        service().serviceId(),
        "idem_1",
        List.of(new InputPart(InputPartType.TEXT, "hello", null)),
        Map.of());
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
        "finance-agent",
        ServiceType.AGENT,
        URI.create("https://agent.example.com/a2a/tasks"),
        AgentA2aAdapter.ADAPTER_ID,
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, true, false, true, true, false, true),
        1);
  }

  private static CompletionStage<AgentA2aStream> completedStream(String... chunks) {
    return CompletableFuture.completedFuture(
        new AgentA2aStream("task-1", new BytePublisher(chunks)));
  }

  private static final class RecordingClient implements AgentA2aClient {

    private final CompletionStage<AgentA2aStream> openStream;
    private final CompletionStage<AgentA2aStream> resumeStream;
    private int cancelCalls;
    private int queryCalls;
    private int submitInputCalls;
    private String lastResumeCursor;
    private CompletionStage<Void> cancelResult = CompletableFuture.completedFuture(null);

    private RecordingClient(CompletionStage<AgentA2aStream> openStream) {
      this(openStream, openStream);
    }

    private RecordingClient(
        CompletionStage<AgentA2aStream> openStream, CompletionStage<AgentA2aStream> resumeStream) {
      this.openStream = openStream;
      this.resumeStream = resumeStream;
    }

    @Override
    public CompletionStage<AgentA2aStream> open(
        CreateRunCommand request, ExecutionContext context, AgentA2aAdapterConfig config) {
      return openStream;
    }

    @Override
    public CompletionStage<Void> cancel(
        ExecutionHandle handle, ExecutionContext context, AgentA2aAdapterConfig config) {
      cancelCalls++;
      return cancelResult;
    }

    @Override
    public CompletionStage<DownstreamExecutionState> queryState(
        ExecutionHandle handle, ExecutionContext context, AgentA2aAdapterConfig config) {
      queryCalls++;
      return CompletableFuture.completedFuture(DownstreamExecutionState.RUNNING);
    }

    @Override
    public CompletionStage<AgentA2aStream> resume(
        ExecutionHandle handle,
        String cursor,
        ExecutionContext context,
        AgentA2aAdapterConfig config) {
      lastResumeCursor = cursor;
      return resumeStream;
    }

    @Override
    public CompletionStage<Void> submitInput(
        SubmitInputCommand command, ExecutionContext context, AgentA2aAdapterConfig config) {
      submitInputCalls++;
      return CompletableFuture.completedFuture(null);
    }
  }

  private static final class HangingPublisher implements Flow.Publisher<byte[]> {

    @Override
    public void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            @Override
            public void request(long n) {}

            @Override
            public void cancel() {}
          });
    }
  }

  private static final class SingleThenHangPublisher implements Flow.Publisher<byte[]> {

    private final byte[] chunk;

    private SingleThenHangPublisher(String chunk) {
      this.chunk = chunk.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            private boolean emitted;
            private boolean canceled;

            @Override
            public void request(long n) {
              if (n <= 0 || emitted || canceled) {
                return;
              }
              emitted = true;
              subscriber.onNext(chunk);
            }

            @Override
            public void cancel() {
              canceled = true;
            }
          });
    }
  }

  private static final class BytePublisher implements Flow.Publisher<byte[]> {

    private final List<byte[]> chunks;

    private BytePublisher(String... chunks) {
      this.chunks =
          java.util.Arrays.stream(chunks)
              .map(chunk -> chunk.getBytes(StandardCharsets.UTF_8))
              .toList();
    }

    @Override
    public void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
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
              while (emitted < n && index < chunks.size() && !canceled) {
                subscriber.onNext(chunks.get(index));
                index++;
                emitted++;
              }
              if (index == chunks.size() && !canceled && !completed) {
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
