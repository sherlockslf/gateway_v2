package com.usig.api.dto;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.InputPart;
import com.usig.domain.run.ConversationId;
import com.usig.domain.run.TenantId;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record CreateRunRequest(
    String conversationId,
    String tenantId,
    String serviceId,
    List<InputPartRequest> input,
    Map<String, Object> metadata) {

  public CreateRunCommand toCommand(String idempotencyKey) {
    List<InputPart> parts =
        Objects.requireNonNullElse(input, List.<InputPartRequest>of()).stream()
            .map(InputPartRequest::toCommandPart)
            .toList();
    return new CreateRunCommand(
        new ConversationId(conversationId),
        new TenantId(tenantId),
        serviceId,
        idempotencyKey,
        parts,
        metadata);
  }
}
