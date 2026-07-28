package com.usig.application.sideevent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class SideEventDispatcherTest {

  private static final Instant NOW = Instant.parse("2026-07-27T12:40:00Z");
  private static final Clock CLOCK = Clock.fixed(NOW.plusSeconds(1), ZoneOffset.UTC);

  @Test
  void ignoresFailureByDefaultSoSidePathDoesNotBlockHotPath() {
    SideEventDispatcher dispatcher =
        new SideEventDispatcher(
            event -> CompletableFuture.failedFuture(new IllegalStateException("kafka down")),
            SideEventDispatchPolicy.bestEffort());

    dispatcher
        .dispatch(sideEvent("usig.message.persist.v1", "tenant_1"))
        .toCompletableFuture()
        .join();
  }

  @Test
  void recordsAsyncFailureInOutboxWithoutBlockingBestEffortEvent() {
    InMemorySideEventOutbox outbox = new InMemorySideEventOutbox(10);
    SideEventDispatcher dispatcher =
        new SideEventDispatcher(
            event -> CompletableFuture.failedFuture(new IllegalStateException("kafka down")),
            SideEventDispatchPolicy.bestEffort(),
            outbox,
            CLOCK);

    dispatcher
        .dispatch(sideEvent("usig.message.persist.v1", "tenant_1"))
        .toCompletableFuture()
        .join();

    SideEventOutboxEntry entry = outbox.snapshot().getFirst();
    assertEquals("usig.message.persist.v1", entry.topic());
    assertEquals("evt_1", entry.eventId());
    assertEquals("run_1", entry.runId());
    assertEquals("tenant_1", entry.tenantId());
    assertEquals("trace_1", entry.traceId());
    assertEquals(CLOCK.instant(), entry.failedAt());
    assertEquals(CLOCK.instant(), entry.nextAttemptAt());
    assertEquals(1, entry.attemptCount());
    assertEquals(IllegalStateException.class.getName(), entry.failureType());
    assertEquals("kafka down", entry.failureMessage());
  }

  @Test
  void recordsSynchronousFailureInOutboxWithoutBlockingBestEffortEvent() {
    InMemorySideEventOutbox outbox = new InMemorySideEventOutbox(10);
    SideEventDispatcher dispatcher =
        new SideEventDispatcher(
            event -> {
              throw new IllegalStateException("sender unavailable");
            },
            SideEventDispatchPolicy.bestEffort(),
            outbox,
            CLOCK);

    dispatcher
        .dispatch(sideEvent("usig.message.persist.v1", "tenant_1"))
        .toCompletableFuture()
        .join();

    SideEventOutboxEntry entry = outbox.snapshot().getFirst();
    assertEquals(IllegalStateException.class.getName(), entry.failureType());
    assertEquals("sender unavailable", entry.failureMessage());
  }

  @Test
  void ignoresAuditFailureForTenantWithoutCriticalPolicy() {
    SideEventDispatcher dispatcher =
        new SideEventDispatcher(
            event -> CompletableFuture.failedFuture(new IllegalStateException("audit down")),
            SideEventDispatchPolicy.failFastAuditForTenants(Set.of("tenant_critical")));

    dispatcher.dispatch(sideEvent("usig.audit.v1", "tenant_normal")).toCompletableFuture().join();
  }

  @Test
  void propagatesCriticalAuditFailureForConfiguredTenant() {
    InMemorySideEventOutbox outbox = new InMemorySideEventOutbox(10);
    SideEventDispatcher dispatcher =
        new SideEventDispatcher(
            event -> CompletableFuture.failedFuture(new IllegalStateException("audit down")),
            SideEventDispatchPolicy.failFastAuditForTenants(Set.of("tenant_critical")),
            outbox,
            CLOCK);

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                dispatcher
                    .dispatch(sideEvent("usig.audit.v1", "tenant_critical"))
                    .toCompletableFuture()
                    .join());

    assertInstanceOf(IllegalStateException.class, exception.getCause());
    assertEquals(1, outbox.snapshot().size());
    assertEquals("usig.audit.v1", outbox.snapshot().getFirst().topic());
  }

  @Test
  void propagatesSynchronousPublisherFailureForCriticalAuditTenant() {
    InMemorySideEventOutbox outbox = new InMemorySideEventOutbox(10);
    SideEventDispatcher dispatcher =
        new SideEventDispatcher(
            event -> {
              throw new IllegalStateException("audit sync failure");
            },
            SideEventDispatchPolicy.failFastAuditForTenants(Set.of("tenant_critical")),
            outbox,
            CLOCK);

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                dispatcher
                    .dispatch(sideEvent("usig.audit.v1", "tenant_critical"))
                    .toCompletableFuture()
                    .join());

    assertInstanceOf(IllegalStateException.class, exception.getCause());
    assertEquals(1, outbox.snapshot().size());
  }

  @Test
  void outboxKeepsBoundedNewestFailureRecords() {
    InMemorySideEventOutbox outbox = new InMemorySideEventOutbox(2);

    outbox
        .recordFailure(sideEvent("usig.audit.v1", "tenant_1"), new IllegalStateException("1"), NOW)
        .toCompletableFuture()
        .join();
    outbox
        .recordFailure(sideEvent("usig.audit.v1", "tenant_2"), new IllegalStateException("2"), NOW)
        .toCompletableFuture()
        .join();
    outbox
        .recordFailure(sideEvent("usig.audit.v1", "tenant_3"), new IllegalStateException("3"), NOW)
        .toCompletableFuture()
        .join();

    assertEquals(2, outbox.snapshot().size());
    assertEquals("tenant_2", outbox.snapshot().get(0).tenantId());
    assertEquals("tenant_3", outbox.snapshot().get(1).tenantId());
  }

  @Test
  void drainerPublishesReadyEntriesAndRemovesThemFromOutbox() {
    InMemorySideEventOutbox outbox = new InMemorySideEventOutbox(10);
    RecordingPublisher publisher = new RecordingPublisher();
    SideEventOutboxDrainer drainer =
        new SideEventOutboxDrainer(
            outbox,
            publisher,
            new SideEventOutboxDrainPolicy(10, java.time.Duration.ofSeconds(30)),
            CLOCK);
    outbox
        .recordFailure(
            sideEvent("usig.message.persist.v1", "tenant_1"),
            new IllegalStateException("down"),
            NOW)
        .toCompletableFuture()
        .join();

    SideEventOutboxDrainResult result = drainer.drainOnce().toCompletableFuture().join();

    assertEquals(new SideEventOutboxDrainResult(1, 1, 0), result);
    assertEquals(1, publisher.events.size());
    assertEquals("evt_1", publisher.events.getFirst().eventId());
    assertEquals(0, outbox.snapshot().size());
  }

  @Test
  void drainerRecordsRetryFailureAndDelaysNextAttempt() {
    InMemorySideEventOutbox outbox = new InMemorySideEventOutbox(10);
    SideEventOutboxDrainer drainer =
        new SideEventOutboxDrainer(
            outbox,
            event -> CompletableFuture.failedFuture(new IllegalStateException("still down")),
            new SideEventOutboxDrainPolicy(10, java.time.Duration.ofSeconds(30)),
            CLOCK);
    outbox
        .recordFailure(
            sideEvent("usig.message.persist.v1", "tenant_1"),
            new IllegalStateException("down"),
            NOW)
        .toCompletableFuture()
        .join();

    SideEventOutboxDrainResult result = drainer.drainOnce().toCompletableFuture().join();

    SideEventOutboxEntry entry = outbox.snapshot().getFirst();
    assertEquals(new SideEventOutboxDrainResult(1, 0, 1), result);
    assertEquals(2, entry.attemptCount());
    assertEquals(CLOCK.instant(), entry.failedAt());
    assertEquals(CLOCK.instant().plusSeconds(30), entry.nextAttemptAt());
    assertEquals("still down", entry.failureMessage());
  }

  @Test
  void drainerSkipsEntriesBeforeNextAttemptTime() {
    InMemorySideEventOutbox outbox = new InMemorySideEventOutbox(10);
    SideEventOutboxDrainer drainer =
        new SideEventOutboxDrainer(
            outbox,
            new RecordingPublisher(),
            new SideEventOutboxDrainPolicy(10, java.time.Duration.ZERO),
            CLOCK);
    outbox
        .recordFailure(
            sideEvent("usig.message.persist.v1", "tenant_1"),
            new IllegalStateException("down"),
            NOW)
        .toCompletableFuture()
        .join();
    outbox
        .markRetryFailure(
            "evt_1", new IllegalStateException("still down"), NOW, CLOCK.instant().plusSeconds(10))
        .toCompletableFuture()
        .join();

    SideEventOutboxDrainResult result = drainer.drainOnce().toCompletableFuture().join();

    assertEquals(SideEventOutboxDrainResult.empty(), result);
    assertEquals(1, outbox.snapshot().size());
  }

  @Test
  void drainerHonorsBatchSize() {
    InMemorySideEventOutbox outbox = new InMemorySideEventOutbox(10);
    RecordingPublisher publisher = new RecordingPublisher();
    SideEventOutboxDrainer drainer =
        new SideEventOutboxDrainer(
            outbox, publisher, new SideEventOutboxDrainPolicy(1, java.time.Duration.ZERO), CLOCK);
    outbox
        .recordFailure(
            sideEvent("usig.message.persist.v1", "evt_1", "tenant_1"),
            new IllegalStateException("down"),
            NOW)
        .toCompletableFuture()
        .join();
    outbox
        .recordFailure(
            sideEvent("usig.message.persist.v1", "evt_2", "tenant_2"),
            new IllegalStateException("down"),
            NOW)
        .toCompletableFuture()
        .join();

    SideEventOutboxDrainResult result = drainer.drainOnce().toCompletableFuture().join();

    assertEquals(new SideEventOutboxDrainResult(1, 1, 0), result);
    assertEquals(1, publisher.events.size());
    assertEquals(1, outbox.snapshot().size());
  }

  private static SideEvent sideEvent(String topic, String tenantId) {
    return sideEvent(topic, "evt_1", tenantId);
  }

  private static SideEvent sideEvent(String topic, String eventId, String tenantId) {
    return new SideEvent(
        topic, eventId, "run_1", tenantId, "trace_1", NOW, Map.of("action", "persisted"));
  }

  private static final class RecordingPublisher
      implements com.usig.application.port.SideEventPublisher {

    private final List<SideEvent> events = new ArrayList<>();

    @Override
    public java.util.concurrent.CompletionStage<Void> publish(SideEvent event) {
      events.add(event);
      return CompletableFuture.completedFuture(null);
    }
  }
}
