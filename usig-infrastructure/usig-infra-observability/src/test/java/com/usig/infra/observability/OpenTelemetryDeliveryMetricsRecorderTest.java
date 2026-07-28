package com.usig.infra.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.usig.application.port.DeliveryMetric;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OpenTelemetryDeliveryMetricsRecorderTest {

  @Test
  void recordsEventsOutWithLowCardinalityAttributes() {
    RecordingTelemetrySink sink = new RecordingTelemetrySink();
    OpenTelemetryDeliveryMetricsRecorder recorder = new OpenTelemetryDeliveryMetricsRecorder(sink);

    recorder.eventOut(new DeliveryMetric("sample-agent", "text.delta", "success", "sse"));

    assertEquals("usig_events_out_total", sink.counters.getFirst().name());
    assertEquals(
        Map.of(
            "service.id",
            "sample-agent",
            "event.type",
            "text.delta",
            "result",
            "success",
            "transport",
            "sse"),
        sink.counters.getFirst().attributes());
  }

  @Test
  void recordsStreamAddedLatencyWithoutHighCardinalityIdentifiers() {
    RecordingTelemetrySink sink = new RecordingTelemetrySink();
    OpenTelemetryDeliveryMetricsRecorder recorder = new OpenTelemetryDeliveryMetricsRecorder(sink);

    recorder.streamAddedLatency(
        new DeliveryMetric("sample-agent", "text.delta", "success", "websocket"),
        Duration.ofMillis(37));

    Map<String, String> attributes = sink.counters.getFirst().attributes();
    assertEquals("usig_stream_added_latency", sink.counters.getFirst().name());
    assertEquals("37", attributes.get("value.ms"));
    assertFalse(attributes.containsKey("runId"));
    assertFalse(attributes.containsKey("tenantId"));
    assertFalse(attributes.containsKey("traceId"));
    assertFalse(attributes.containsKey("payload"));
  }

  private static final class RecordingTelemetrySink implements TelemetrySink {

    private final List<Observation> counters = new ArrayList<>();

    @Override
    public void incrementCounter(String name, Map<String, String> attributes) {
      counters.add(new Observation(name, Map.copyOf(attributes)));
    }

    @Override
    public void recordSpan(String name, Map<String, String> attributes) {}
  }

  private record Observation(String name, Map<String, String> attributes) {}
}
