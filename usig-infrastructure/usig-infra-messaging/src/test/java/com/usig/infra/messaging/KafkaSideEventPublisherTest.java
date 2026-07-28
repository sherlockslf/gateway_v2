package com.usig.infra.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.usig.application.sideevent.SideEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class KafkaSideEventPublisherTest {

  private static final Instant NOW = Instant.parse("2026-07-27T07:00:00Z");

  @Test
  void usesTopicFromSideEventAndRunIdAsKafkaKey() {
    RecordingSender sender = new RecordingSender();
    KafkaSideEventPublisher publisher = new KafkaSideEventPublisher(sender);

    publisher.publish(sideEvent()).toCompletableFuture().join();

    KafkaSideEventRecord record = sender.records.getFirst();
    assertEquals("usig.audit.v1", record.topic());
    assertEquals("run_1", record.key());
    assertEquals("evt_1", record.eventId());
    assertEquals("tenant_1", record.tenantId());
    assertEquals("trace_1", record.traceId());
    assertEquals(Map.of("action", "created"), record.payload());
  }

  @Test
  void propagatesSenderAck() {
    RecordingSender sender = new RecordingSender();
    KafkaSideEventPublisher publisher = new KafkaSideEventPublisher(sender);

    publisher.publish(sideEvent()).toCompletableFuture().join();

    assertEquals(1, sender.records.size());
  }

  @Test
  void propagatesSenderFailure() {
    KafkaSideEventPublisher publisher =
        new KafkaSideEventPublisher(
            record ->
                CompletableFuture.failedFuture(new IllegalStateException("kafka unavailable")));

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () -> publisher.publish(sideEvent()).toCompletableFuture().join());

    assertInstanceOf(IllegalStateException.class, exception.getCause());
  }

  private static SideEvent sideEvent() {
    return new SideEvent(
        "usig.audit.v1", "evt_1", "run_1", "tenant_1", "trace_1", NOW, Map.of("action", "created"));
  }

  private static final class RecordingSender implements KafkaSideEventSender {

    private final List<KafkaSideEventRecord> records = new ArrayList<>();

    @Override
    public CompletionStage<Void> send(KafkaSideEventRecord record) {
      records.add(record);
      return CompletableFuture.completedFuture(null);
    }
  }
}
