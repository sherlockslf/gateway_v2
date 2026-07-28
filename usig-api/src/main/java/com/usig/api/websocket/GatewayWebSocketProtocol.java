package com.usig.api.websocket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.usig.application.result.CancelRunResult;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class GatewayWebSocketProtocol {

  public static final String SUBPROTOCOL = "usig.events.v1";
  public static final String COMMAND_CANCEL = "command.cancel";
  public static final String COMMAND_INPUT = "command.input";
  public static final String COMMAND_AUTH_REFRESH = "command.auth.refresh";
  public static final String COMMAND_RESULT = "command.result";

  private final ObjectMapper objectMapper;
  private final int maxMessageBytes;

  public GatewayWebSocketProtocol(ObjectMapper objectMapper, int maxMessageBytes) {
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    if (maxMessageBytes <= 0) {
      throw new IllegalArgumentException("maxMessageBytes must be positive");
    }
    this.maxMessageBytes = maxMessageBytes;
  }

  public WebSocketCommand decode(String message) {
    if (message == null || message.isBlank()) {
      throw new IllegalArgumentException("message must not be blank");
    }
    if (message.getBytes(StandardCharsets.UTF_8).length > maxMessageBytes) {
      throw new IllegalArgumentException("message exceeds maxMessageBytes");
    }
    try {
      WebSocketCommand command = objectMapper.readValue(message, WebSocketCommand.class);
      requireText(command.type(), "type");
      requireText(command.commandId(), "commandId");
      if (!COMMAND_AUTH_REFRESH.equals(command.type())) {
        requireText(command.runId(), "runId");
      }
      return command;
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("invalid websocket command json", exception);
    }
  }

  public String cancelAccepted(String commandId, CancelRunResult result) {
    return accepted(
        commandId,
        result.run().id().value(),
        Map.of(
            "status",
            result.run().status().name(),
            "downstreamState",
            result.downstreamState().name()));
  }

  public String inputAccepted(String commandId, String runId) {
    return accepted(commandId, runId, Map.of());
  }

  public String authRefreshAccepted(String commandId) {
    return accepted(commandId, null, Map.of("renewed", true));
  }

  public String authExpired(String commandId, String runId) {
    return result(commandId, runId, false, Map.of("errorCode", "UNAUTHORIZED"));
  }

  public String rejected(String commandId, String runId, Throwable failure) {
    Throwable cause = unwrap(failure);
    ErrorCode errorCode =
        cause instanceof UseCaseException useCaseException
            ? useCaseException.errorCode()
            : ErrorCode.INVALID_REQUEST;
    return result(commandId, runId, false, Map.of("errorCode", errorCode.name()));
  }

  private String accepted(String commandId, String runId, Map<String, Object> result) {
    return result(commandId, runId, true, result);
  }

  private String result(
      String commandId, String runId, boolean accepted, Map<String, Object> result) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("type", COMMAND_RESULT);
    body.put("commandId", commandId);
    if (runId != null && !runId.isBlank()) {
      body.put("runId", runId);
    }
    body.put("accepted", accepted);
    body.put("result", result);
    try {
      return objectMapper.writeValueAsString(body);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("failed to encode websocket command result", exception);
    }
  }

  private static void requireText(String value, String fieldName) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(fieldName + " must not be blank");
    }
  }

  private static Throwable unwrap(Throwable failure) {
    Throwable current = failure;
    while (current.getCause() != null
        && (current instanceof java.util.concurrent.CompletionException
            || current instanceof java.util.concurrent.ExecutionException)) {
      current = current.getCause();
    }
    return current;
  }
}
