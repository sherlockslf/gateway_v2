package com.usig.application.serviceaudit;

import com.usig.application.port.ServiceRegistryAuditPublisher;
import com.usig.application.sideevent.SideEvent;
import com.usig.application.sideevent.SideEventDispatcher;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

public final class ServiceRegistrySideEventAuditPublisher implements ServiceRegistryAuditPublisher {

  public static final String TOPIC = "usig.service.registry.audit.v1";
  private static final String RUN_ID = "service-registry";
  private static final String TENANT_ID = "system";

  private final ServiceRegistryAuditPublisher primaryPublisher;
  private final SideEventDispatcher sideEventDispatcher;

  public ServiceRegistrySideEventAuditPublisher(
      ServiceRegistryAuditPublisher primaryPublisher, SideEventDispatcher sideEventDispatcher) {
    this.primaryPublisher = Objects.requireNonNull(primaryPublisher, "primaryPublisher");
    this.sideEventDispatcher = Objects.requireNonNull(sideEventDispatcher, "sideEventDispatcher");
  }

  @Override
  public CompletionStage<Void> publish(ServiceRegistryAuditEvent event) {
    Objects.requireNonNull(event, "event");
    return primaryPublisher
        .publish(event)
        .thenCompose(
            ignored ->
                sideEventDispatcher.dispatch(toSideEvent(event)).exceptionally(failure -> null));
  }

  private static SideEvent toSideEvent(ServiceRegistryAuditEvent event) {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("action", event.action());
    payload.put("serviceId", event.serviceId());
    payload.put("version", event.version());
    payload.put("occurredAt", event.occurredAt().toString());
    payload.put("service", event.payload());
    return new SideEvent(
        TOPIC, event.eventId(), RUN_ID, TENANT_ID, event.eventId(), event.occurredAt(), payload);
  }
}
