package com.usig.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.usig.application.memory.InMemoryReplayStore;
import com.usig.application.memory.ListGatewayEventPublisher;
import com.usig.application.memory.ReliabilityOptions;
import com.usig.application.memory.ReliableRunEventPublisher;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.event.EventMetadata;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.RunId;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;

class Phase5CapacityGateTest {

  private static final Instant NOW = Instant.parse("2026-07-27T06:30:00Z");
  private static final int RUNS = 100;
  private static final int EVENTS_PER_NORMAL_RUN = 20;
  private static final int MAX_CI_P99_MILLIS = 500;

  @Test
  void onePercentSlowClientsDoNotDegradeNormalRuns() {
    ReliableRunEventPublisher publisher =
        new ReliableRunEventPublisher(
            new InMemoryReplayStore(RUNS * 32, 8 * 1024 * 1024),
            new ReliabilityOptions(1, 8192, Duration.ofMinutes(10), false, List.of()));
    RunId slowRunId = new RunId("run_slow_0");
    publisher.attach(slowRunId, new DemandIgnoringPublisher(eventsFor(slowRunId, 3)));
    for (int i = 1; i < RUNS; i++) {
      RunId runId = new RunId("run_normal_" + i);
      publisher.attach(
          runId, new ListGatewayEventPublisher(eventsFor(runId, EVENTS_PER_NORMAL_RUN)));
    }

    long started = System.nanoTime();
    CompletionException slowFailure =
        assertThrowsCompletion(() -> collect(publisher.subscribe(slowRunId, null), 1));
    UseCaseException slowCause = assertInstanceOf(UseCaseException.class, slowFailure.getCause());
    assertEquals(ErrorCode.CLIENT_TOO_SLOW, slowCause.errorCode());

    List<Long> normalLatenciesMillis = new ArrayList<>();
    for (int i = 1; i < RUNS; i++) {
      RunId runId = new RunId("run_normal_" + i);
      long runStarted = System.nanoTime();
      List<GatewayEvent> events = collect(publisher.subscribe(runId, null), Long.MAX_VALUE);
      normalLatenciesMillis.add(Duration.ofNanos(System.nanoTime() - runStarted).toMillis());
      assertEquals(EVENTS_PER_NORMAL_RUN, events.size());
    }

    long totalMillis = Duration.ofNanos(System.nanoTime() - started).toMillis();
    long p99Millis = percentile99(normalLatenciesMillis);
    assertTrue(
        p99Millis <= MAX_CI_P99_MILLIS,
        "normal run p99 latency must stay under "
            + MAX_CI_P99_MILLIS
            + "ms in the CI capacity smoke gate, actual="
            + p99Millis
            + "ms, total="
            + totalMillis
            + "ms");
  }

  private static CompletionException assertThrowsCompletion(Runnable runnable) {
    try {
      runnable.run();
    } catch (CompletionException exception) {
      return exception;
    }
    throw new AssertionError("Expected CompletionException");
  }

  private static long percentile99(List<Long> values) {
    List<Long> sorted = new ArrayList<>(values);
    sorted.sort(Comparator.naturalOrder());
    int index = Math.max(0, (int) Math.ceil(sorted.size() * 0.99) - 1);
    return sorted.get(index);
  }

  private static List<GatewayEvent> eventsFor(RunId runId, int count) {
    List<GatewayEvent> events = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      events.add(event("evt_" + runId.value() + "_" + i, EventType.PROGRESS_UPDATED, runId, i));
    }
    return events;
  }

  private static GatewayEvent event(
      String eventId, EventType eventType, RunId runId, long sequence) {
    return GatewayEvent.of(
        eventId,
        eventType,
        "conv_1",
        runId.value(),
        "sample-agent",
        sequence,
        NOW.plusMillis(sequence),
        Map.of("step", sequence),
        new EventMetadata("tenant_1", null, Map.of()));
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
      this.events = Collections.unmodifiableList(new ArrayList<>(events));
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
