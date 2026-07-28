package com.usig.infra.observability;

import java.util.Map;

public final class NoopTelemetrySink implements TelemetrySink {

  @Override
  public void incrementCounter(String name, Map<String, String> attributes) {}

  @Override
  public void recordSpan(String name, Map<String, String> attributes) {}
}
