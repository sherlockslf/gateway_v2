package com.usig.application.command;

import com.usig.domain.run.ConversationId;
import com.usig.domain.run.TenantId;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record CreateRunCommand(
    ConversationId conversationId,
    TenantId tenantId,
    String serviceId,
    String idempotencyKey,
    List<InputPart> input,
    Map<String, Object> metadata) {

  public CreateRunCommand {
    conversationId = Objects.requireNonNull(conversationId, "conversationId must not be null");
    tenantId = Objects.requireNonNull(tenantId, "tenantId must not be null");
    serviceId = requireText(serviceId, "serviceId");
    idempotencyKey = normalize(idempotencyKey);
    input = List.copyOf(Objects.requireNonNullElse(input, List.of()));
    metadata = Map.copyOf(Objects.requireNonNullElse(metadata, Map.of()));
  }

  private static String requireText(String value, String name) {
    String normalized = normalize(value);
    if (normalized == null) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return normalized;
  }

  private static String normalize(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }
}
