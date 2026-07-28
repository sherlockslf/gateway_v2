package com.usig.infra.observability;

import com.usig.application.port.RunTelemetry;
import com.usig.domain.run.Run;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class OpenTelemetryRunTelemetry implements RunTelemetry {

  static final String RUN_CREATED_COUNTER = "usig_runs_created_total";
  static final String RUN_CREATE_REUSED_COUNTER = "usig_runs_create_reused_total";
  static final String RUN_CREATE_RETRY_SCHEDULED_COUNTER = "usig_runs_create_retry_scheduled_total";
  static final String RUN_CREATED_SPAN = "usig.run.create";
  static final String RUN_CREATE_REUSED_SPAN = "usig.run.create.reused";
  static final String RUN_CREATE_RETRY_SCHEDULED_SPAN = "usig.run.create.retry_scheduled";

  private final TelemetrySink sink;

  public OpenTelemetryRunTelemetry(TelemetrySink sink) {
    this.sink = Objects.requireNonNull(sink, "sink must not be null");
  }

  @Override
  public void runCreated(Run run) {
    Objects.requireNonNull(run, "run must not be null");
    sink.incrementCounter(RUN_CREATED_COUNTER, lowCardinalityMetricAttributes(run));
    sink.recordSpan(RUN_CREATED_SPAN, traceAttributes(run));
  }

  @Override
  public void runCreationReused(Run run) {
    Objects.requireNonNull(run, "run must not be null");
    sink.incrementCounter(RUN_CREATE_REUSED_COUNTER, lowCardinalityMetricAttributes(run));
    sink.recordSpan(RUN_CREATE_REUSED_SPAN, traceAttributes(run));
  }

  @Override
  public void runCreateRetryScheduled(Run run, int failedAttempt, Duration delay) {
    Objects.requireNonNull(run, "run must not be null");
    Objects.requireNonNull(delay, "delay must not be null");
    sink.incrementCounter(RUN_CREATE_RETRY_SCHEDULED_COUNTER, lowCardinalityMetricAttributes(run));
    sink.recordSpan(
        RUN_CREATE_RETRY_SCHEDULED_SPAN, retryTraceAttributes(run, failedAttempt, delay));
  }

  private static Map<String, String> lowCardinalityMetricAttributes(Run run) {
    return Map.of("service.id", run.serviceId(), "run.status", run.status().name());
  }

  private static Map<String, String> traceAttributes(Run run) {
    return Map.of(
        "usig.run_id",
        run.id().value(),
        "usig.conversation_id",
        run.conversationId().value(),
        "usig.tenant_id",
        run.tenantId().value(),
        "usig.service_id",
        run.serviceId(),
        "usig.run_status",
        run.status().name());
  }

  private static Map<String, String> retryTraceAttributes(
      Run run, int failedAttempt, Duration delay) {
    Map<String, String> attributes = new HashMap<>(traceAttributes(run));
    attributes.put("usig.failed_attempt", Integer.toString(failedAttempt));
    attributes.put("usig.retry_delay_ms", Long.toString(delay.toMillis()));
    return Map.copyOf(attributes);
  }
}
