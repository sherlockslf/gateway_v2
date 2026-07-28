package com.usig.infra.observability;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class QuarkusOpenTelemetrySink implements TelemetrySink {

  private static final String INSTRUMENTATION_NAME = "com.usig.gateway";

  private final OpenTelemetry openTelemetry;
  private final Tracer tracer;
  private final Map<String, LongCounter> counters = new ConcurrentHashMap<>();

  public QuarkusOpenTelemetrySink(OpenTelemetry openTelemetry) {
    this.openTelemetry = Objects.requireNonNull(openTelemetry, "openTelemetry must not be null");
    this.tracer = this.openTelemetry.getTracer(INSTRUMENTATION_NAME);
  }

  @Override
  public void incrementCounter(String name, Map<String, String> attributes) {
    counters
        .computeIfAbsent(
            name,
            counterName ->
                openTelemetry.getMeter(INSTRUMENTATION_NAME).counterBuilder(counterName).build())
        .add(1, attributes(attributes));
  }

  @Override
  public void recordSpan(String name, Map<String, String> attributes) {
    Span span = tracer.spanBuilder(name).setAllAttributes(attributes(attributes)).startSpan();
    span.end();
  }

  private static Attributes attributes(Map<String, String> attributes) {
    AttributesBuilder builder = Attributes.builder();
    attributes.forEach(builder::put);
    return builder.build();
  }
}
