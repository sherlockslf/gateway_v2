package com.usig.infra.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.usig.domain.run.ConversationId;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunId;
import com.usig.domain.run.RunStatus;
import com.usig.domain.run.TenantId;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OpenTelemetryRunTelemetryTest {

  private static final Instant NOW = Instant.parse("2026-07-27T07:20:00Z");

  @Test
  void recordsRunCreatedMetricAndTraceAttributes() {
    RecordingTelemetrySink sink = new RecordingTelemetrySink();
    OpenTelemetryRunTelemetry telemetry = new OpenTelemetryRunTelemetry(sink);

    telemetry.runCreated(run(RunStatus.STARTING));

    assertEquals("usig_runs_created_total", sink.counters.getFirst().name());
    assertEquals(
        Map.of("service.id", "sample-agent", "run.status", "STARTING"),
        sink.counters.getFirst().attributes());
    assertEquals("usig.run.create", sink.spans.getFirst().name());
    assertEquals("run_1", sink.spans.getFirst().attributes().get("usig.run_id"));
    assertEquals("tenant_1", sink.spans.getFirst().attributes().get("usig.tenant_id"));
  }

  @Test
  void recordsIdempotencyReuseSeparately() {
    RecordingTelemetrySink sink = new RecordingTelemetrySink();
    OpenTelemetryRunTelemetry telemetry = new OpenTelemetryRunTelemetry(sink);

    telemetry.runCreationReused(run(RunStatus.STARTING));

    assertEquals("usig_runs_create_reused_total", sink.counters.getFirst().name());
    assertEquals("usig.run.create.reused", sink.spans.getFirst().name());
  }

  @Test
  void recordsCreateRetryScheduledMetricAndTraceAttributes() {
    RecordingTelemetrySink sink = new RecordingTelemetrySink();
    OpenTelemetryRunTelemetry telemetry = new OpenTelemetryRunTelemetry(sink);

    telemetry.runCreateRetryScheduled(run(RunStatus.ACCEPTED), 1, Duration.ofMillis(37));

    assertEquals("usig_runs_create_retry_scheduled_total", sink.counters.getFirst().name());
    assertEquals(
        Map.of("service.id", "sample-agent", "run.status", "ACCEPTED"),
        sink.counters.getFirst().attributes());
    assertEquals("usig.run.create.retry_scheduled", sink.spans.getFirst().name());
    assertEquals("run_1", sink.spans.getFirst().attributes().get("usig.run_id"));
    assertEquals("1", sink.spans.getFirst().attributes().get("usig.failed_attempt"));
    assertEquals("37", sink.spans.getFirst().attributes().get("usig.retry_delay_ms"));
  }

  @Test
  void keepsHighCardinalityIdentifiersOutOfMetricAttributes() {
    RecordingTelemetrySink sink = new RecordingTelemetrySink();
    OpenTelemetryRunTelemetry telemetry = new OpenTelemetryRunTelemetry(sink);

    telemetry.runCreated(run(RunStatus.STARTING));

    Map<String, String> metricAttributes = sink.counters.getFirst().attributes();
    assertFalse(metricAttributes.containsKey("runId"));
    assertFalse(metricAttributes.containsKey("userId"));
    assertFalse(metricAttributes.containsKey("tenantId"));
    assertFalse(metricAttributes.containsKey("usig.run_id"));
    assertFalse(metricAttributes.containsKey("usig.tenant_id"));
  }

  private static Run run(RunStatus status) {
    return new Run(
        new RunId("run_1"),
        new ConversationId("conv_1"),
        new TenantId("tenant_1"),
        "sample-agent",
        status,
        null,
        1,
        NOW,
        NOW);
  }

  private static final class RecordingTelemetrySink implements TelemetrySink {

    private final List<Observation> counters = new ArrayList<>();
    private final List<Observation> spans = new ArrayList<>();

    @Override
    public void incrementCounter(String name, Map<String, String> attributes) {
      counters.add(new Observation(name, Map.copyOf(attributes)));
    }

    @Override
    public void recordSpan(String name, Map<String, String> attributes) {
      spans.add(new Observation(name, Map.copyOf(attributes)));
    }
  }

  private record Observation(String name, Map<String, String> attributes) {}
}
