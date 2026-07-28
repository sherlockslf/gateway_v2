package com.usig.application.sideevent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class IdempotentSideEventConsumerTest {

  private static final Instant NOW = Instant.parse("2026-07-28T03:10:00Z");

  @Test
  void processesFirstDeliveryAndSkipsConsumedDuplicate() {
    InMemorySideEventConsumptionStore store = new InMemorySideEventConsumptionStore(10);
    List<SideEvent> handled = new ArrayList<>();
    IdempotentSideEventConsumer consumer =
        new IdempotentSideEventConsumer(
            store,
            event -> {
              handled.add(event);
              return CompletableFuture.completedFuture(null);
            });

    assertEquals(
        SideEventConsumptionResult.processed("evt_1"),
        consumer.consume(sideEvent("evt_1")).toCompletableFuture().join());
    assertEquals(
        SideEventConsumptionResult.skippedConsumed("evt_1"),
        consumer.consume(sideEvent("evt_1")).toCompletableFuture().join());

    assertEquals(1, handled.size());
    assertEquals("evt_1", handled.getFirst().eventId());
  }

  @Test
  void skipsConcurrentDuplicateWhileFirstDeliveryIsInProgress() {
    InMemorySideEventConsumptionStore store = new InMemorySideEventConsumptionStore(10);
    CompletableFuture<Void> firstHandle = new CompletableFuture<>();
    IdempotentSideEventConsumer consumer =
        new IdempotentSideEventConsumer(store, event -> firstHandle);

    CompletableFuture<SideEventConsumptionResult> running =
        consumer.consume(sideEvent("evt_1")).toCompletableFuture();

    assertEquals(
        SideEventConsumptionResult.skippedInProgress("evt_1"),
        consumer.consume(sideEvent("evt_1")).toCompletableFuture().join());
    firstHandle.complete(null);
    assertEquals(SideEventConsumptionResult.processed("evt_1"), running.join());
  }

  @Test
  void releasesClaimWhenHandlerFailsSoBrokerRedeliveryCanRetry() {
    InMemorySideEventConsumptionStore store = new InMemorySideEventConsumptionStore(10);
    IdempotentSideEventConsumer failing =
        new IdempotentSideEventConsumer(
            store,
            event -> CompletableFuture.failedFuture(new IllegalStateException("handler down")));

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () -> failing.consume(sideEvent("evt_1")).toCompletableFuture().join());

    assertEquals("handler down", exception.getCause().getMessage());
    assertTrue(store.inProgressSnapshot().isEmpty());

    List<SideEvent> handled = new ArrayList<>();
    IdempotentSideEventConsumer retry =
        new IdempotentSideEventConsumer(
            store,
            event -> {
              handled.add(event);
              return CompletableFuture.completedFuture(null);
            });

    assertEquals(
        SideEventConsumptionResult.processed("evt_1"),
        retry.consume(sideEvent("evt_1")).toCompletableFuture().join());
    assertEquals(1, handled.size());
  }

  @Test
  void keepsConsumedWindowBounded() {
    InMemorySideEventConsumptionStore store = new InMemorySideEventConsumptionStore(2);
    IdempotentSideEventConsumer consumer =
        new IdempotentSideEventConsumer(store, event -> CompletableFuture.completedFuture(null));

    consumer.consume(sideEvent("evt_1")).toCompletableFuture().join();
    consumer.consume(sideEvent("evt_2")).toCompletableFuture().join();
    consumer.consume(sideEvent("evt_3")).toCompletableFuture().join();

    assertEquals(java.util.Set.of("evt_2", "evt_3"), store.consumedSnapshot());
  }

  private static SideEvent sideEvent(String eventId) {
    return new SideEvent(
        "usig.audit.v1", eventId, "run_1", "tenant_1", "trace_1", NOW, Map.of("action", "saved"));
  }
}
