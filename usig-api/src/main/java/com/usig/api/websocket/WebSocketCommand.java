package com.usig.api.websocket;

import java.util.Map;
import java.util.Objects;

record WebSocketCommand(
    String type, String commandId, String runId, String tenantId, Map<String, Object> payload) {

  WebSocketCommand {
    payload = Map.copyOf(Objects.requireNonNullElse(payload, Map.of()));
  }
}
