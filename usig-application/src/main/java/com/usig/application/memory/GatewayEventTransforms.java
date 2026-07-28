package com.usig.application.memory;

import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import java.util.HashMap;
import java.util.Map;

final class GatewayEventTransforms {

  private GatewayEventTransforms() {}

  static boolean canCoalesce(GatewayEvent left, GatewayEvent right) {
    return left.eventType() == EventType.TEXT_DELTA
        && right.eventType() == EventType.TEXT_DELTA
        && left.conversationId().equals(right.conversationId())
        && left.runId().equals(right.runId())
        && left.sourceService().equals(right.sourceService());
  }

  static GatewayEvent coalesce(GatewayEvent left, GatewayEvent right) {
    String leftContent = content(left);
    String rightContent = content(right);
    Map<String, Object> payload = new HashMap<>(right.payload());
    payload.put("content", leftContent + rightContent);
    return copyWithPayload(right, payload);
  }

  static GatewayEvent redact(GatewayEvent event, RollingTextRedactor redactor) {
    if (event.eventType() != EventType.TEXT_DELTA || !event.payload().containsKey("content")) {
      return event;
    }
    Map<String, Object> payload = new HashMap<>(event.payload());
    payload.put("content", redactor.redact(content(event)));
    return copyWithPayload(event, payload);
  }

  static int estimateBytes(GatewayEvent event) {
    Object content = event.payload().get("content");
    return event.eventId().length()
        + event.wireEventType().length()
        + (content == null ? 0 : content.toString().length() * 2);
  }

  private static String content(GatewayEvent event) {
    Object content = event.payload().get("content");
    return content == null ? "" : content.toString();
  }

  private static GatewayEvent copyWithPayload(GatewayEvent event, Map<String, Object> payload) {
    return new GatewayEvent(
        event.schemaVersion(),
        event.eventId(),
        event.eventType(),
        event.conversationId(),
        event.runId(),
        event.taskId(),
        event.sourceService(),
        event.sourceEventId(),
        event.sequence(),
        event.occurredAt(),
        event.observedAt(),
        payload,
        event.metadata());
  }
}
