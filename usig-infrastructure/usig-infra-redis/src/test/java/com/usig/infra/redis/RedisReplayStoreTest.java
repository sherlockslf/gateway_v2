package com.usig.infra.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.event.EventMetadata;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.RunId;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;

class RedisReplayStoreTest {

  private static final RunId RUN_ID = new RunId("run_1");
  private static final Instant NOW = Instant.parse("2026-07-27T06:00:00Z");

  @Test
  void appendsGatewayEventWithBoundedStreamKeyAndTtl() {
    RecordingRedisCommands commands = new RecordingRedisCommands();
    RedisReplayStore store = new RedisReplayStore(commands, "usig-test", 128);

    store
        .append(RUN_ID, event("evt_1", EventType.TEXT_DELTA), Duration.ofMinutes(10))
        .toCompletableFuture()
        .join();

    assertEquals("usig-test:run:run_1:events", commands.appendedKey);
    assertEquals("evt_1", commands.appendedEventId);
    assertEquals(Duration.ofMinutes(10), commands.appendedTtl);
    assertEquals(128, commands.appendedMaxEvents);
    assertTrue(commands.appendedPayload.contains("\"eventId\":\"evt_1\""));
  }

  @Test
  void readsEventsAfterCursor() {
    RecordingRedisCommands commands = new RecordingRedisCommands();
    RedisReplayStore store = new RedisReplayStore(commands, "usig-test", 128);
    store
        .append(RUN_ID, event("evt_1", EventType.TEXT_DELTA), Duration.ofMinutes(10))
        .toCompletableFuture()
        .join();
    store
        .append(RUN_ID, event("evt_2", EventType.TEXT_END), Duration.ofMinutes(10))
        .toCompletableFuture()
        .join();

    List<GatewayEvent> events = collect(store.readAfter(RUN_ID, "evt_1"));

    assertEquals(List.of("evt_2"), events.stream().map(GatewayEvent::eventId).toList());
    assertEquals("evt_1", commands.lastCursor);
  }

  @Test
  void returnsCursorExpiredWhenRedisReturnsNoEntries() {
    RedisReplayStore store = new RedisReplayStore(new RecordingRedisCommands(), "usig-test", 128);

    CompletionException exception =
        assertThrows(
            CompletionException.class, () -> collect(store.readAfter(RUN_ID, "evt_missing")));

    UseCaseException cause = assertInstanceOf(UseCaseException.class, exception.getCause());
    assertEquals(ErrorCode.CURSOR_EXPIRED, cause.errorCode());
  }

  @Test
  void propagatesRedisUnavailableAsStreamFailure() {
    RecordingRedisCommands commands = new RecordingRedisCommands();
    commands.failReads = true;
    RedisReplayStore store = new RedisReplayStore(commands, "usig-test", 128);

    CompletionException exception =
        assertThrows(CompletionException.class, () -> collect(store.readAfter(RUN_ID, "evt_1")));

    assertInstanceOf(IllegalStateException.class, exception.getCause());
  }

  @Test
  void deletesReplayKey() {
    RecordingRedisCommands commands = new RecordingRedisCommands();
    RedisReplayStore store = new RedisReplayStore(commands, "usig-test", 128);

    store.delete(RUN_ID).toCompletableFuture().join();

    assertEquals("usig-test:run:run_1:events", commands.deletedKey);
  }

  private static GatewayEvent event(String eventId, EventType eventType) {
    return GatewayEvent.of(
        eventId,
        eventType,
        "conv_1",
        RUN_ID.value(),
        "sample-agent",
        Long.parseLong(eventId.substring(eventId.lastIndexOf('_') + 1)) - 1,
        NOW,
        eventType == EventType.TEXT_DELTA ? Map.of("content", "hello") : Map.of(),
        new EventMetadata("tenant_1", null, Map.of()));
  }

  private static List<GatewayEvent> collect(Flow.Publisher<GatewayEvent> publisher) {
    CollectingSubscriber subscriber = new CollectingSubscriber();
    publisher.subscribe(subscriber);
    return subscriber.result.join();
  }

  private static final class RecordingRedisCommands implements RedisReplayCommands {

    private final List<RedisReplayEntry> entries = new ArrayList<>();
    private String appendedKey;
    private String appendedEventId;
    private String appendedPayload;
    private Duration appendedTtl;
    private int appendedMaxEvents;
    private String lastCursor;
    private String deletedKey;
    private boolean failReads;

    @Override
    public CompletionStage<Void> append(
        String key, String eventId, String payload, Duration ttl, int maxEvents) {
      appendedKey = key;
      appendedEventId = eventId;
      appendedPayload = payload;
      appendedTtl = ttl;
      appendedMaxEvents = maxEvents;
      entries.add(new RedisReplayEntry(eventId, payload));
      entries.sort(Comparator.comparing(RedisReplayEntry::eventId));
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<List<RedisReplayEntry>> readAfter(String key, String lastEventId) {
      if (failReads) {
        return CompletableFuture.failedFuture(new IllegalStateException("redis unavailable"));
      }
      lastCursor = lastEventId;
      return CompletableFuture.completedFuture(
          entries.stream().filter(entry -> entry.eventId().compareTo(lastEventId) > 0).toList());
    }

    @Override
    public CompletionStage<Void> delete(String key) {
      deletedKey = key;
      return CompletableFuture.completedFuture(null);
    }
  }

  private static final class CollectingSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final List<GatewayEvent> events = new ArrayList<>();
    private final CompletableFuture<List<GatewayEvent>> result = new CompletableFuture<>();

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscription.request(Long.MAX_VALUE);
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
