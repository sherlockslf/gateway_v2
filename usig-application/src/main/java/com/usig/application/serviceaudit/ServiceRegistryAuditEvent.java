package com.usig.application.serviceaudit;

import com.usig.domain.service.ServiceDescriptor;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public record ServiceRegistryAuditEvent(
    String eventId,
    String action,
    String serviceId,
    long version,
    Instant occurredAt,
    Map<String, Object> payload) {

  public ServiceRegistryAuditEvent {
    eventId = requireText(eventId, "eventId");
    action = requireText(action, "action");
    serviceId = requireText(serviceId, "serviceId");
    if (version < 1) {
      throw new IllegalArgumentException("version must be positive");
    }
    occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
    payload = Map.copyOf(Objects.requireNonNullElse(payload, Map.of()));
  }

  public static ServiceRegistryAuditEvent fromDescriptor(
      String action, ServiceDescriptor service, Instant occurredAt) {
    Objects.requireNonNull(service, "service must not be null");
    return fromDescriptor(
        "service-registry-" + service.serviceId() + "-v" + service.version(),
        action,
        service,
        occurredAt);
  }

  public static ServiceRegistryAuditEvent removed(ServiceDescriptor service, Instant occurredAt) {
    Objects.requireNonNull(service, "service must not be null");
    return fromDescriptor(
        "service-registry-" + service.serviceId() + "-removed-v" + service.version(),
        "service.removed",
        service,
        occurredAt);
  }

  private static ServiceRegistryAuditEvent fromDescriptor(
      String eventId, String action, ServiceDescriptor service, Instant occurredAt) {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("adapterId", service.adapterId());
    payload.put("serviceType", service.serviceType().name());
    payload.put("endpoint", service.endpoint().toString());
    payload.put("visibleTenantIds", service.visibleTenantIds());
    payload.put("transports", service.transports().stream().map(Enum::name).sorted().toList());
    payload.put("capabilities", service.capabilities());
    payload.put("limits", service.limits());
    payload.put("circuitBreakerPolicy", service.circuitBreakerPolicy());
    payload.put("allowedImageHosts", service.allowedImageHosts());
    if (service.serviceGroup() != null) {
      payload.put("serviceGroup", service.serviceGroup());
    }
    return new ServiceRegistryAuditEvent(
        eventId, action, service.serviceId(), service.version(), occurredAt, payload);
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
