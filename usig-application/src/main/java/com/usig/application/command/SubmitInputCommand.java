package com.usig.application.command;

import com.usig.domain.run.RunId;
import com.usig.domain.run.TenantId;
import java.util.Map;
import java.util.Objects;

public record SubmitInputCommand(RunId runId, TenantId tenantId, Map<String, Object> payload) {

  public SubmitInputCommand {
    runId = Objects.requireNonNull(runId, "runId must not be null");
    tenantId = Objects.requireNonNull(tenantId, "tenantId must not be null");
    payload = Map.copyOf(Objects.requireNonNullElse(payload, Map.of()));
  }
}
