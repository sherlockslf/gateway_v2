package com.usig.adapter.generic.sse;

import com.usig.domain.event.EventType;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public record GenericSseMappingProfile(
    String profileId,
    String sourceEventPointer,
    Map<String, EventType> eventTypeMappings,
    Set<EventType> allowedEventTypes,
    Map<String, FieldMapping> payloadMappings,
    FieldMapping sourceEventIdMapping) {

  public GenericSseMappingProfile {
    profileId = requireText(profileId, "profileId");
    sourceEventPointer = requirePointer(sourceEventPointer);
    eventTypeMappings = Map.copyOf(Objects.requireNonNull(eventTypeMappings));
    allowedEventTypes = Set.copyOf(Objects.requireNonNull(allowedEventTypes));
    payloadMappings = Map.copyOf(Objects.requireNonNullElse(payloadMappings, Map.of()));
    if (eventTypeMappings.isEmpty()) {
      throw new IllegalArgumentException("eventTypeMappings must not be empty");
    }
    if (allowedEventTypes.isEmpty()) {
      throw new IllegalArgumentException("allowedEventTypes must not be empty");
    }
    for (EventType eventType : eventTypeMappings.values()) {
      if (!allowedEventTypes.contains(eventType)) {
        throw new IllegalArgumentException("eventTypeMappings must use whitelisted event types");
      }
    }
  }

  public EventType mapEventType(String sourceEventType) {
    EventType eventType = eventTypeMappings.get(sourceEventType);
    if (eventType == null) {
      throw new GenericSseAdapterException("Unsupported downstream event type: " + sourceEventType);
    }
    if (!allowedEventTypes.contains(eventType)) {
      throw new GenericSseAdapterException(
          "Event type is not whitelisted: " + eventType.wireName());
    }
    return eventType;
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static String requirePointer(String jsonPointer) {
    if (jsonPointer == null || jsonPointer.isBlank() || !jsonPointer.startsWith("/")) {
      throw new IllegalArgumentException("sourceEventPointer must start with /");
    }
    return jsonPointer;
  }
}
