package com.usig.application.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.event.EventMetadata;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.RunId;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;

class ReliableRunEventPublisherTest {

  private static final Instant NOW = Instant.parse("2026-07-27T05:00:00Z");
  private static final RunId RUN_ID = new RunId("run_1");

  @Test
  void replaysEventsAfterLastEventId() {
    InMemoryReplayStore replayStore = new InMemoryReplayStore(10, 1024);
    ReliableRunEventPublisher publisher =
        new ReliableRunEventPublisher(replayStore, ReliabilityOptions.defaults());
    publisher.attach(
        RUN_ID,
        new ListGatewayEventPublisher(
            List.of(
                event("evt_1", EventType.EXECUTION_STARTED, Map.of()),
                event("evt_2", EventType.TEXT_DELTA, Map.of("content", "hello")),
                event("evt_3", EventType.TEXT_END, Map.of()),
                event("evt_4", EventType.EXECUTION_COMPLETED, Map.of()))));

    List<GatewayEvent> live = collect(publisher.subscribe(RUN_ID, null), Long.MAX_VALUE);
    List<GatewayEvent> replay = collect(publisher.subscribe(RUN_ID, "evt_2"), Long.MAX_VALUE);

    assertEquals(4, live.size());
    assertEquals(List.of("evt_3", "evt_4"), replay.stream().map(GatewayEvent::eventId).toList());
  }

  @Test
  void coalescesTextDeltaBeforeRollingRedaction() {
    InMemoryReplayStore replayStore = new InMemoryReplayStore(10, 1024);
    ReliableRunEventPublisher publisher =
        new ReliableRunEventPublisher(
            replayStore,
            new ReliabilityOptions(256, 1024, Duration.ofMinutes(10), true, List.of("SECRET")));
    publisher.attach(
        RUN_ID,
        new ListGatewayEventPublisher(
            List.of(
                event("evt_1", EventType.TEXT_DELTA, Map.of("content", "SEC")),
                event("evt_2", EventType.TEXT_DELTA, Map.of("content", "RET")),
                event("evt_3", EventType.TEXT_END, Map.of()))));

    List<GatewayEvent> events = collect(publisher.subscribe(RUN_ID, null), Long.MAX_VALUE);

    assertEquals(2, events.size());
    assertEquals("evt_2", events.get(0).eventId());
    assertEquals("[REDACTED]", events.get(0).payload().get("content"));
    assertEquals(EventType.TEXT_END, events.get(1).eventType());
  }

  @Test
  void returnsCursorExpiredWhenReplayCursorIsNoLongerAvailable() {
    InMemoryReplayStore replayStore = new InMemoryReplayStore(1, 1024);
    ReliableRunEventPublisher publisher =
        new ReliableRunEventPublisher(replayStore, ReliabilityOptions.defaults());
    publisher.attach(
        RUN_ID,
        new ListGatewayEventPublisher(
            List.of(
                event("evt_1", EventType.EXECUTION_STARTED, Map.of()),
                event("evt_2", EventType.EXECUTION_COMPLETED, Map.of()))));
    collect(publisher.subscribe(RUN_ID, null), Long.MAX_VALUE);

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () -> collect(publisher.subscribe(RUN_ID, "evt_1"), Long.MAX_VALUE));

    UseCaseException cause = assertInstanceOf(UseCaseException.class, exception.getCause());
    assertEquals(ErrorCode.CURSOR_EXPIRED, cause.errorCode());
  }

  @Test
  void failsWithClientTooSlowWhenPendingBufferOverflows() {
    ReliableRunEventPublisher publisher =
        new ReliableRunEventPublisher(
            new InMemoryReplayStore(10, 1024),
            new ReliabilityOptions(1, 1024, Duration.ofMinutes(10), false, List.of()));
    publisher.attach(
        RUN_ID,
        new DemandIgnoringPublisher(
            List.of(
                event("evt_1", EventType.PROGRESS_UPDATED, Map.of()),
                event("evt_2", EventType.PROGRESS_UPDATED, Map.of()),
                event("evt_3", EventType.PROGRESS_UPDATED, Map.of()))));

    CompletionException exception =
        assertThrows(
            CompletionException.class, () -> collect(publisher.subscribe(RUN_ID, null), 1));

    UseCaseException cause = assertInstanceOf(UseCaseException.class, exception.getCause());
    assertEquals(ErrorCode.CLIENT_TOO_SLOW, cause.errorCode());
  }

  @Test
  void failsStreamWhenReplayAppendFails() {
    ReliableRunEventPublisher publisher =
        new ReliableRunEventPublisher(
            new FailingReplayStore(),
            new ReliabilityOptions(10, 1024, Duration.ofMinutes(10), false, List.of()));
    publisher.attach(
        RUN_ID,
        new ListGatewayEventPublisher(
            List.of(event("evt_1", EventType.EXECUTION_STARTED, Map.of()))));

    CompletionException exception =
        assertThrows(
            CompletionException.class, () -> collect(publisher.subscribe(RUN_ID, null), 1));

    assertInstanceOf(IllegalStateException.class, exception.getCause());
  }

  @Test
  void slowRunDoesNotBlockAnotherRun() {
    RunId slowRunId = new RunId("run_slow");
    RunId normalRunId = new RunId("run_normal");
    ReliableRunEventPublisher publisher =
        new ReliableRunEventPublisher(
            new InMemoryReplayStore(10, 1024),
            new ReliabilityOptions(1, 1024, Duration.ofMinutes(10), false, List.of()));
    publisher.attach(
        slowRunId,
        new DemandIgnoringPublisher(
            List.of(
                event("evt_1", EventType.PROGRESS_UPDATED, Map.of(), slowRunId),
                event("evt_2", EventType.PROGRESS_UPDATED, Map.of(), slowRunId),
                event("evt_3", EventType.PROGRESS_UPDATED, Map.of(), slowRunId))));
    publisher.attach(
        normalRunId,
        new ListGatewayEventPublisher(
            List.of(
                event("evt_1", EventType.EXECUTION_STARTED, Map.of(), normalRunId),
                event("evt_2", EventType.EXECUTION_COMPLETED, Map.of(), normalRunId))));

    assertThrows(CompletionException.class, () -> collect(publisher.subscribe(slowRunId, null), 1));
    List<GatewayEvent> normal = collect(publisher.subscribe(normalRunId, null), Long.MAX_VALUE);

    assertEquals(
        List.of(EventType.EXECUTION_STARTED, EventType.EXECUTION_COMPLETED),
        normal.stream().map(GatewayEvent::eventType).toList());
  }

  private static GatewayEvent event(
      String eventId, EventType eventType, Map<String, Object> payload) {
    return event(eventId, eventType, payload, RUN_ID);
  }

  private static GatewayEvent event(
      String eventId, EventType eventType, Map<String, Object> payload, RunId runId) {
    return GatewayEvent.of(
        eventId,
        eventType,
        "conv_1",
        runId.value(),
        "sample-agent",
        Long.parseLong(eventId.substring(eventId.lastIndexOf('_') + 1)) - 1,
        NOW,
        payload,
        new EventMetadata("tenant_1", null, Map.of()));
  }

  private static final class FailingReplayStore implements com.usig.application.port.ReplayStore {

    @Override
    public java.util.concurrent.CompletionStage<Void> append(
        RunId runId, GatewayEvent event, Duration ttl) {
      return CompletableFuture.failedFuture(new IllegalStateException("redis unavailable"));
    }

    @Override
    public Flow.Publisher<GatewayEvent> readAfter(RunId runId, String lastEventId) {
      return new ListGatewayEventPublisher(List.of());
    }

    @Override
    public java.util.concurrent.CompletionStage<Void> delete(RunId runId) {
      return CompletableFuture.completedFuture(null);
    }
  }

  private static List<GatewayEvent> collect(Flow.Publisher<GatewayEvent> publisher, long request) {
    CollectingSubscriber subscriber = new CollectingSubscriber(request);
    publisher.subscribe(subscriber);
    return subscriber.result.join();
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

  private static final class DemandIgnoringPublisher implements Flow.Publisher<GatewayEvent> {

    private final List<GatewayEvent> events;

    private DemandIgnoringPublisher(List<GatewayEvent> events) {
      this.events = List.copyOf(events);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super GatewayEvent> subscriber) {
      subscriber.onSubscribe(
          new Flow.Subscription() {
            @Override
            public void request(long n) {
              events.forEach(subscriber::onNext);
              subscriber.onComplete();
            }

            @Override
            public void cancel() {}
          });
    }
  }
}
