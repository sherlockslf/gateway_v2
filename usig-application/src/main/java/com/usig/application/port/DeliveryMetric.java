package com.usig.application.port;

import java.util.Map;

public record DeliveryMetric(String serviceId, String eventType, String result, String transport) {

  public DeliveryMetric {
    serviceId = requireText(serviceId, "serviceId");
    eventType = requireText(eventType, "eventType");
    result = requireText(result, "result");
    transport = requireText(transport, "transport");
  }

  public Map<String, String> attributes() {
    return Map.of(
        "service.id", serviceId,
        "event.type", eventType,
        "result", result,
        "transport", transport);
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
