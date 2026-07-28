package com.usig.api.dto;

import com.usig.domain.run.Run;

public record RunResponse(
    String runId,
    String conversationId,
    String tenantId,
    String serviceId,
    String status,
    long version,
    String eventsUrl) {

  public static RunResponse from(Run run, String eventsUrl) {
    return new RunResponse(
        run.id().value(),
        run.conversationId().value(),
        run.tenantId().value(),
        run.serviceId(),
        run.status().name(),
        run.version(),
        eventsUrl);
  }
}
