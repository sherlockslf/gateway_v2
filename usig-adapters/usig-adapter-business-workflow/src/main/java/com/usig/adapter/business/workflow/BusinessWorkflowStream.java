package com.usig.adapter.business.workflow;

import java.util.Objects;
import java.util.concurrent.Flow;

public record BusinessWorkflowStream(
    String workflowRunId, Flow.Publisher<BusinessWorkflowEvent> events) {

  public BusinessWorkflowStream {
    if (workflowRunId == null || workflowRunId.isBlank()) {
      throw new IllegalArgumentException("workflowRunId must not be blank");
    }
    events = Objects.requireNonNull(events, "events must not be null");
  }
}
