package com.usig.adapter.generic.sse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.InputPart;
import com.usig.application.command.InputPartType;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class GenericSseJsonAdapterContractTest {

  @Test
  void mapsDownstreamSseJsonToGatewayEvents() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                """
                id:source-1
                event:message
                data:{"kind":"message","text":"hello"}

                id:source-2
                event:done
                data:{"kind":"done"}

                """));
    GenericSseJsonAdapter adapter = new GenericSseJsonAdapter(config(512), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals("task-1", execution.handle().downstreamTaskId());
    assertEquals(2, events.size());
    assertEquals(EventType.TEXT_DELTA, events.get(0).eventType());
    assertEquals("hello", events.get(0).payload().get("content"));
    assertEquals("src-0", events.get(0).sourceEventId());
    assertEquals(EventType.EXECUTION_COMPLETED, events.get(1).eventType());
  }

  @Test
  void decodesChunkedFramesAndMultipleDataLines() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream(
                "id:source-1\nevent:message\ndata:{\"kind\":\"mess",
                "age\",\"text\":\"hello\"}\n\n",
                "id:source-2\nevent:message\ndata:{\"kind\":\"message\",\n",
                "data:\"text\":\"line\"}\n\n"));
    GenericSseJsonAdapter adapter = new GenericSseJsonAdapter(config(512), client);

    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();
    List<GatewayEvent> events = collect(execution.events(), 10);

    assertEquals(2, events.size());
    assertEquals("hello", events.get(0).payload().get("content"));
    assertEquals("line", events.get(1).payload().get("content"));
  }

  @Test
  void rejectsInvalidJsonData() throws Exception {
    RecordingClient client = new RecordingClient(completedStream("data:not-json\n\n"));
    GenericSseJsonAdapter adapter = new GenericSseJsonAdapter(config(512), client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    CompletionException error =
        assertThrows(CompletionException.class, () -> collect(execution.events(), 1));

    assertInstanceOf(GenericSseAdapterException.class, error.getCause());
  }

  @Test
  void rejectsOverlongFrame() throws Exception {
    RecordingClient client =
        new RecordingClient(
            completedStream("data:{\"kind\":\"message\",\"text\":\"too long\"}\n\n"));
    GenericSseJsonAdapter adapter = new GenericSseJsonAdapter(config(16), client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    CompletionException error =
        assertThrows(CompletionException.class, () -> collect(execution.events(), 1));

    assertInstanceOf(GenericSseAdapterException.class, error.getCause());
    assertTrue(error.getCause().getMessage().contains("maxFrameBytes"));
  }

  @Test
  void propagatesDownstreamStreamInterruption() throws Exception {
    RecordingClient client =
        new RecordingClient(
            CompletableFuture.completedFuture(
                new DownstreamSseStream(
                    "task-1",
                    new ErroringByteChunkPublisher(
                        "data:{\"kind\":\"message\",\"text\":\"hello\"}\n\n"))));
    GenericSseJsonAdapter adapter = new GenericSseJsonAdapter(config(512), client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    CompletionException error =
        assertThrows(CompletionException.class, () -> collect(execution.events(), 10));

    assertInstanceOf(IllegalStateException.class, error.getCause());
  }

  @Test
  void timesOutWhenDownstreamOpenDoesNotComplete() {
    RecordingClient client = new RecordingClient(new CompletableFuture<>());
    GenericSseJsonAdapter adapter =
        new GenericSseJsonAdapter(config(512, Duration.ofMillis(10)), client);

    ExecutionException error =
        assertThrows(
            ExecutionException.class,
            () ->
                adapter
                    .execute(command(), context())
                    .toCompletableFuture()
                    .get(1, TimeUnit.SECONDS));

    GenericSseAdapterException cause =
        assertInstanceOf(GenericSseAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_CONNECT_FAILED, cause.errorCode());
    assertTrue(cause.getMessage().contains("openTimeout"));
  }

  @Test
  void timesOutWhenFirstMappedEventDoesNotArrive() throws Exception {
    RecordingClient client =
        new RecordingClient(
            CompletableFuture.completedFuture(
                new DownstreamSseStream("task-1", new NeverByteChunkPublisher())));
    GenericSseJsonAdapter adapter =
        new GenericSseJsonAdapter(
            config(512, Duration.ofSeconds(1), Duration.ofMillis(10)), client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    CollectingSubscriber subscriber = new CollectingSubscriber(1);
    execution.events().subscribe(subscriber);
    ExecutionException error =
        assertThrows(ExecutionException.class, () -> subscriber.result.get(1, TimeUnit.SECONDS));

    GenericSseAdapterException cause =
        assertInstanceOf(GenericSseAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_FIRST_EVENT_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("firstEventTimeout"));
  }

  @Test
  void timesOutWhenMappedEventsBecomeIdle() throws Exception {
    RecordingClient client =
        new RecordingClient(
            CompletableFuture.completedFuture(
                new DownstreamSseStream(
                    "task-1",
                    new OneChunkThenNeverByteChunkPublisher(
                        "data:{\"kind\":\"message\",\"text\":\"hello\"}\n\n"))));
    GenericSseJsonAdapter adapter =
        new GenericSseJsonAdapter(
            config(512, Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofMillis(10)),
            client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    CollectingSubscriber subscriber = new CollectingSubscriber(2);
    execution.events().subscribe(subscriber);
    ExecutionException error =
        assertThrows(ExecutionException.class, () -> subscriber.result.get(1, TimeUnit.SECONDS));

    assertEquals(1, subscriber.events.size());
    assertEquals("hello", subscriber.events.getFirst().payload().get("content"));
    GenericSseAdapterException cause =
        assertInstanceOf(GenericSseAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_IDLE_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("idleEventTimeout"));
  }

  @Test
  void timesOutWhenTotalExecutionTimeElapses() throws Exception {
    RecordingClient client =
        new RecordingClient(
            CompletableFuture.completedFuture(
                new DownstreamSseStream(
                    "task-1",
                    new OneChunkThenNeverByteChunkPublisher(
                        "data:{\"kind\":\"message\",\"text\":\"hello\"}\n\n"))));
    GenericSseJsonAdapter adapter =
        new GenericSseJsonAdapter(
            config(
                512,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                Duration.ofMillis(10)),
            client);
    AdapterExecution execution = adapter.execute(command(), context()).toCompletableFuture().get();

    CollectingSubscriber subscriber = new CollectingSubscriber(2);
    execution.events().subscribe(subscriber);
    ExecutionException error =
        assertThrows(ExecutionException.class, () -> subscriber.result.get(1, TimeUnit.SECONDS));

    assertEquals(1, subscriber.events.size());
    assertEquals("hello", subscriber.events.getFirst().payload().get("content"));
    GenericSseAdapterException cause =
        assertInstanceOf(GenericSseAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_TOTAL_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("totalExecutionTimeout"));
  }

  @Test
  void delegatesCancelToDownstreamClient() throws Exception {
    RecordingClient client = new RecordingClient(completedStream(""));
    GenericSseJsonAdapter adapter = new GenericSseJsonAdapter(config(512), client);
    ExecutionHandle handle =
        new ExecutionHandle(
            "generic-doc-parser",
            GenericSseJsonAdapter.ADAPTER_ID,
            "task-1",
            null,
            "run_1",
            Map.of());

    DownstreamExecutionState state =
        adapter.cancel(handle, context()).toCompletableFuture().get(1, TimeUnit.SECONDS);

    assertEquals(DownstreamExecutionState.CANCELED, state);
    assertEquals(handle, client.canceledHandle);
  }

  @Test
  void timesOutWhenDownstreamCancelDoesNotComplete() {
    RecordingClient client = new RecordingClient(completedStream(""), new CompletableFuture<>());
    GenericSseJsonAdapter adapter =
        new GenericSseJsonAdapter(
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
            "generic-doc-parser",
            GenericSseJsonAdapter.ADAPTER_ID,
            "task-1",
            null,
            "run_1",
            Map.of());

    CompletionException error =
        assertThrows(
            CompletionException.class,
            () -> adapter.cancel(handle, context()).toCompletableFuture().join());

    assertEquals(handle, client.canceledHandle);
    GenericSseAdapterException cause =
        assertInstanceOf(GenericSseAdapterException.class, error.getCause());
    assertEquals(ErrorCode.DOWNSTREAM_CANCEL_TIMEOUT, cause.errorCode());
    assertTrue(cause.getMessage().contains("cancelTimeout"));
  }

  @Test
  void rejectsResumeWhenNotSupported() {
    RecordingClient client = new RecordingClient(completedStream(""));
    GenericSseJsonAdapter adapter = new GenericSseJsonAdapter(config(512), client);
    ExecutionHandle handle =
        new ExecutionHandle(
            "generic-doc-parser",
            GenericSseJsonAdapter.ADAPTER_ID,
            "task-1",
            null,
            "run_1",
            Map.of());

    CompletionException error =
        assertThrows(
            CompletionException.class,
            () -> adapter.resume(handle, "evt_1", context()).toCompletableFuture().join());

    assertInstanceOf(GenericSseAdapterException.class, error.getCause());
    assertTrue(error.getCause().getMessage().contains("does not support resume"));
  }

  private static GenericSseAdapterConfig config(int maxFrameBytes) {
    return config(maxFrameBytes, Duration.ofSeconds(1));
  }

  private static GenericSseAdapterConfig config(int maxFrameBytes, Duration timeout) {
    return new GenericSseAdapterConfig(service(), mappingProfile(), maxFrameBytes, timeout);
  }

  private static GenericSseAdapterConfig config(
      int maxFrameBytes, Duration openTimeout, Duration firstEventTimeout) {
    return new GenericSseAdapterConfig(
        service(), mappingProfile(), maxFrameBytes, openTimeout, firstEventTimeout);
  }

  private static GenericSseAdapterConfig config(
      int maxFrameBytes,
      Duration openTimeout,
      Duration firstEventTimeout,
      Duration idleEventTimeout) {
    return new GenericSseAdapterConfig(
        service(),
        mappingProfile(),
        maxFrameBytes,
        openTimeout,
        firstEventTimeout,
        idleEventTimeout);
  }

  private static GenericSseAdapterConfig config(
      int maxFrameBytes,
      Duration openTimeout,
      Duration firstEventTimeout,
      Duration idleEventTimeout,
      Duration totalExecutionTimeout) {
    return config(
        maxFrameBytes,
        openTimeout,
        firstEventTimeout,
        idleEventTimeout,
        totalExecutionTimeout,
        Duration.ofSeconds(30));
  }

  private static GenericSseAdapterConfig config(
      int maxFrameBytes,
      Duration openTimeout,
      Duration firstEventTimeout,
      Duration idleEventTimeout,
      Duration totalExecutionTimeout,
      Duration cancelTimeout) {
    return new GenericSseAdapterConfig(
        service(),
        mappingProfile(),
        maxFrameBytes,
        openTimeout,
        firstEventTimeout,
        idleEventTimeout,
        totalExecutionTimeout,
        cancelTimeout);
  }

  private static GenericSseMappingProfile mappingProfile() {
    return new GenericSseMappingProfile(
        "document-parser-v1",
        "/kind",
        Map.of("message", EventType.TEXT_DELTA, "done", EventType.EXECUTION_COMPLETED),
        Set.of(EventType.TEXT_DELTA, EventType.EXECUTION_COMPLETED),
        Map.of("content", FieldMapping.pointerOrDefault("/text", "")),
        FieldMapping.template("src-${sequence}"));
  }

  private static ServiceDescriptor service() {
    return new ServiceDescriptor(
        "generic-doc-parser",
        ServiceType.STREAMING_TASK,
        URI.create("https://internal.example/parser/run"),
        GenericSseJsonAdapter.ADAPTER_ID,
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, false, false, false, false, false, true),
        1);
  }

  private static CreateRunCommand command() {
    return new CreateRunCommand(
        new ConversationId("conv_1"),
        new TenantId("tenant_1"),
        "generic-doc-parser",
        "idem-1",
        List.of(new InputPart(InputPartType.TEXT, "hello", null)),
        Map.of());
  }

  private static ExecutionContext context() {
    return new ExecutionContext(new RunId("run_1"), new TenantId("tenant_1"), "trace-1", Map.of());
  }

  private static CompletionStage<DownstreamSseStream> completedStream(String... chunks) {
    return CompletableFuture.completedFuture(
        new DownstreamSseStream("task-1", new ByteChunkPublisher(chunks)));
  }

  private static List<GatewayEvent> collect(Flow.Publisher<GatewayEvent> publisher, long request) {
    CollectingSubscriber subscriber = new CollectingSubscriber(request);
    publisher.subscribe(subscriber);
    return subscriber.result.join();
  }

  private static final class RecordingClient implements DownstreamSseClient {

    private final CompletionStage<DownstreamSseStream> stream;
    private final CompletionStage<Void> cancelResult;
    private ExecutionHandle canceledHandle;

    private RecordingClient(CompletionStage<DownstreamSseStream> stream) {
      this(stream, CompletableFuture.completedFuture(null));
    }

    private RecordingClient(
        CompletionStage<DownstreamSseStream> stream, CompletionStage<Void> cancelResult) {
      this.stream = stream;
      this.cancelResult = cancelResult;
    }

    @Override
    public CompletionStage<DownstreamSseStream> open(
        CreateRunCommand request,
        ExecutionContext context,
        ServiceDescriptor service,
        GenericSseAdapterConfig config) {
      return stream;
    }

    @Override
    public CompletionStage<Void> cancel(ExecutionHandle handle, ExecutionContext context) {
      canceledHandle = handle;
      return cancelResult;
    }
  }

  private static final class ByteChunkPublisher implements Flow.Publisher<byte[]> {

    private final List<byte[]> chunks;

    private ByteChunkPublisher(String... chunks) {
      this.chunks =
          List.of(chunks).stream().map(value -> value.getBytes(StandardCharsets.UTF_8)).toList();
    }

    @Override
    public void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
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
              while (emitted < requested && index < chunks.size() && !canceled) {
                byte[] chunk = chunks.get(index);
                index++;
                subscriber.onNext(chunk);
                emitted++;
              }
              if (index == chunks.size() && !canceled) {
                canceled = true;
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

  private static final class ErroringByteChunkPublisher implements Flow.Publisher<byte[]> {

    private final byte[] chunk;

    private ErroringByteChunkPublisher(String chunk) {
      this.chunk = chunk.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            private boolean emitted;

            @Override
            public void request(long requested) {
              if (requested <= 0 || emitted) {
                return;
              }
              emitted = true;
              subscriber.onNext(chunk);
              subscriber.onError(new IllegalStateException("downstream interrupted"));
            }

            @Override
            public void cancel() {}
          });
    }
  }

  private static final class NeverByteChunkPublisher implements Flow.Publisher<byte[]> {

    @Override
    public void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            @Override
            public void request(long requested) {}

            @Override
            public void cancel() {}
          });
    }
  }

  private static final class OneChunkThenNeverByteChunkPublisher implements Flow.Publisher<byte[]> {

    private final byte[] chunk;

    private OneChunkThenNeverByteChunkPublisher(String chunk) {
      this.chunk = chunk.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            private boolean emitted;

            @Override
            public void request(long requested) {
              if (requested <= 0 || emitted) {
                return;
              }
              emitted = true;
              subscriber.onNext(chunk);
            }

            @Override
            public void cancel() {}
          });
    }
  }

  private static final class CollectingSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final long request;
    private final List<GatewayEvent> events = new ArrayList<>();
    private final CompletableFuture<List<GatewayEvent>> result = new CompletableFuture<>();

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
