package com.usig.infra.observability;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.opentelemetry.api.OpenTelemetry;
import java.util.Map;
import org.junit.jupiter.api.Test;

class QuarkusOpenTelemetrySinkTest {

  @Test
  void recordsCounterAndSpanThroughOpenTelemetryApi() {
    QuarkusOpenTelemetrySink sink = new QuarkusOpenTelemetrySink(OpenTelemetry.noop());

    assertDoesNotThrow(
        () -> {
          sink.incrementCounter(
              "usig_runs_created_total",
              Map.of("service.id", "sample-agent", "run.status", "STARTING"));
          sink.recordSpan(
              "usig.run.create", Map.of("usig.run_id", "run_1", "usig.service_id", "sample-agent"));
        });
  }
}
