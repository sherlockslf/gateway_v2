package com.usig.adapter.agent.a2a;

import java.util.Objects;
import java.util.concurrent.Flow;

public record AgentA2aStream(String downstreamTaskId, Flow.Publisher<byte[]> body) {

  public AgentA2aStream {
    downstreamTaskId = normalize(downstreamTaskId);
    body = Objects.requireNonNull(body, "body must not be null");
  }

  private static String normalize(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }
}
