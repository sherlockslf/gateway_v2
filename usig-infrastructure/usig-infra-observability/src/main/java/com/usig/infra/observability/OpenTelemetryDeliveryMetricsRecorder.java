package com.usig.infra.observability;

import com.usig.application.port.DeliveryMetric;
import com.usig.application.port.DeliveryMetricsRecorder;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class OpenTelemetryDeliveryMetricsRecorder implements DeliveryMetricsRecorder {

  static final String EVENTS_OUT_COUNTER = "usig_events_out_total";
  static final String STREAM_ADDED_LATENCY_METRIC = "usig_stream_added_latency";

  private final TelemetrySink sink;

  public OpenTelemetryDeliveryMetricsRecorder(TelemetrySink sink) {
    this.sink = Objects.requireNonNull(sink, "sink must not be null");
  }

  @Override
  public void eventOut(DeliveryMetric metric) {
    Objects.requireNonNull(metric, "metric must not be null");
    sink.incrementCounter(EVENTS_OUT_COUNTER, metric.attributes());
  }

  @Override
  public void streamAddedLatency(DeliveryMetric metric, Duration latency) {
    Objects.requireNonNull(metric, "metric must not be null");
    Objects.requireNonNull(latency, "latency must not be null");
    Map<String, String> attributes = new HashMap<>(metric.attributes());
    attributes.put("value.ms", Long.toString(latency.toMillis()));
    sink.incrementCounter(STREAM_ADDED_LATENCY_METRIC, Map.copyOf(attributes));
  }
}
