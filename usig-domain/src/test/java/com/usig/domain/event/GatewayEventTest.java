package com.usig.domain.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GatewayEventTest {

  @Test
  void createsImmutableGatewayEventWithDefaultSchema() {
    GatewayEvent event =
        GatewayEvent.of(
            "evt_1",
            EventType.TEXT_DELTA,
            "conv_1",
            "run_1",
            "sample-agent",
            1,
            Instant.parse("2026-07-27T03:00:00Z"),
            Map.of("content", "hello"),
            new EventMetadata("tenant_1", "trace_1", Map.of("adapter", "sample")));

    assertEquals(GatewayEvent.CURRENT_SCHEMA_VERSION, event.schemaVersion());
    assertEquals("text.delta", event.wireEventType());
    assertEquals(event.observedAt(), event.occurredAt());
    assertThrows(UnsupportedOperationException.class, () -> event.payload().put("x", "y"));
  }

  @Test
  void rejectsMissingRequiredEnvelopeFields() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayEvent.of(
                "evt_1",
                EventType.TEXT_DELTA,
                " ",
                "run_1",
                "sample-agent",
                1,
                Instant.parse("2026-07-27T03:00:00Z"),
                Map.of(),
                new EventMetadata("tenant_1", null, Map.of())));
  }

  @Test
  void rejectsNegativeSequence() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GatewayEvent.of(
                "evt_1",
                EventType.TEXT_DELTA,
                "conv_1",
                "run_1",
                "sample-agent",
                -1,
                Instant.parse("2026-07-27T03:00:00Z"),
                Map.of(),
                new EventMetadata("tenant_1", null, Map.of())));
  }
}
