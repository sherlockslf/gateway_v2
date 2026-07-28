package com.usig.application.port;

import com.usig.domain.run.RunId;
import com.usig.domain.run.TenantId;
import java.util.Map;
import java.util.Objects;

public record ExecutionContext(
    RunId runId, TenantId tenantId, String traceId, Map<String, String> attributes) {

  public ExecutionContext {
    runId = Objects.requireNonNull(runId, "runId must not be null");
    tenantId = Objects.requireNonNull(tenantId, "tenantId must not be null");
    traceId = normalize(traceId);
    attributes = Map.copyOf(Objects.requireNonNullElse(attributes, Map.of()));
  }

  private static String normalize(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }
}
