package com.usig.application.pipeline;

import com.usig.domain.run.RunId;
import com.usig.domain.run.TenantId;
import java.util.Map;

public record PipelineContext(
    RunId runId, TenantId tenantId, String serviceId, Map<String, String> attributes) {

  public PipelineContext {
    if (runId == null) {
      throw new IllegalArgumentException("runId must not be null");
    }
    if (tenantId == null) {
      throw new IllegalArgumentException("tenantId must not be null");
    }
    if (serviceId == null || serviceId.isBlank()) {
      throw new IllegalArgumentException("serviceId must not be blank");
    }
    attributes = Map.copyOf(attributes == null ? Map.of() : attributes);
  }
}
