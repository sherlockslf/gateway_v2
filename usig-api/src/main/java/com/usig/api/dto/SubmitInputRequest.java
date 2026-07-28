package com.usig.api.dto;

import com.usig.application.command.SubmitInputCommand;
import com.usig.domain.run.RunId;
import com.usig.domain.run.TenantId;
import java.util.Map;

public record SubmitInputRequest(Map<String, Object> payload) {

  public SubmitInputCommand toCommand(String runId, String tenantId) {
    return new SubmitInputCommand(new RunId(runId), new TenantId(tenantId), payload);
  }
}
