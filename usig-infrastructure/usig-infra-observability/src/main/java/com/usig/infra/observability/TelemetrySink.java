package com.usig.infra.observability;

import java.util.Map;

public interface TelemetrySink {

  void incrementCounter(String name, Map<String, String> attributes);

  void recordSpan(String name, Map<String, String> attributes);
}
