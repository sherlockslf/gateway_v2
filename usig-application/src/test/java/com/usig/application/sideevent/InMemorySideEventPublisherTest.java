package com.usig.application.sideevent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.usig.domain.event.EventMetadata;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InMemorySideEventPublisherTest {

  private static final Instant NOW = Instant.parse("2026-07-27T06:50:00Z");

  @Test
  void createsSideEventFromGatewayEventWithRequiredRoutingFields() {
    GatewayEvent event = gatewayEvent("evt_1", "trace_1");

    SideEvent sideEvent =
        SideEvent.fromGatewayEvent("usig.audit.v1", event, Map.of("action", "started"));

    assertEquals("usig.audit.v1", sideEvent.topic());
    assertEquals("evt_1", sideEvent.eventId());
    assertEquals("run_1", sideEvent.runId());
    assertEquals("tenant_1", sideEvent.tenantId());
    assertEquals("trace_1", sideEvent.traceId());
    assertEquals(Map.of("action", "started"), sideEvent.payload());
  }

  @Test
  void rejectsSideEventWithoutTraceId() {
    GatewayEvent event = gatewayEvent("evt_1", null);

    assertThrows(
        IllegalArgumentException.class,
        () -> SideEvent.fromGatewayEvent("usig.audit.v1", event, Map.of()));
  }

  @Test
  void storesSideEventsWithinConfiguredBound() {
    InMemorySideEventPublisher publisher = new InMemorySideEventPublisher(2);

    publisher.publish(sideEvent("evt_1")).toCompletableFuture().join();
    publisher.publish(sideEvent("evt_2")).toCompletableFuture().join();
    publisher.publish(sideEvent("evt_3")).toCompletableFuture().join();

    assertEquals(
        java.util.List.of("evt_2", "evt_3"),
        publisher.snapshot().stream().map(SideEvent::eventId).toList());
  }

  @Test
  void rejectsUnboundedConfiguration() {
    assertThrows(IllegalArgumentException.class, () -> new InMemorySideEventPublisher(0));
  }

  private static SideEvent sideEvent(String eventId) {
    return new SideEvent(
        "usig.message.persist.v1",
        eventId,
        "run_1",
        "tenant_1",
        "trace_1",
        NOW,
        Map.of("content", "safe text"));
  }

  private static GatewayEvent gatewayEvent(String eventId, String traceId) {
    return GatewayEvent.of(
        eventId,
        EventType.EXECUTION_STARTED,
        "conv_1",
        "run_1",
        "sample-agent",
        0,
        NOW,
        Map.of(),
        new EventMetadata("tenant_1", traceId, Map.of()));
  }
}
