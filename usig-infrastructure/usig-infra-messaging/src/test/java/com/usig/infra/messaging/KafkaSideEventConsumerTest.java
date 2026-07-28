package com.usig.infra.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.usig.application.sideevent.IdempotentSideEventConsumer;
import com.usig.application.sideevent.InMemorySideEventConsumptionStore;
import com.usig.application.sideevent.SideEventConsumptionResult;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class KafkaSideEventConsumerTest {

  private static final Instant NOW = Instant.parse("2026-07-28T03:15:00Z");

  @Test
  void mapsKafkaRecordToSideEventAndSkipsDuplicateEventId() {
    List<String> handled = new ArrayList<>();
    KafkaSideEventConsumer consumer =
        new KafkaSideEventConsumer(
            new IdempotentSideEventConsumer(
                new InMemorySideEventConsumptionStore(10),
                event -> {
                  handled.add(event.topic() + ":" + event.eventId() + ":" + event.runId());
                  return CompletableFuture.completedFuture(null);
                }));

    assertEquals(
        SideEventConsumptionResult.processed("evt_1"),
        consumer.consume(record("evt_1")).toCompletableFuture().join());
    assertEquals(
        SideEventConsumptionResult.skippedConsumed("evt_1"),
        consumer.consume(record("evt_1")).toCompletableFuture().join());

    assertEquals(List.of("usig.audit.v1:evt_1:run_1"), handled);
  }

  private static KafkaSideEventRecord record(String eventId) {
    return new KafkaSideEventRecord(
        "usig.audit.v1",
        "run_1",
        eventId,
        "run_1",
        "tenant_1",
        "trace_1",
        NOW,
        Map.of("action", "consumed"));
  }
}
