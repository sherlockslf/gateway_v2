package com.usig.adapter.generic.sse;

import java.util.Objects;
import java.util.concurrent.Flow;

public record DownstreamSseStream(String downstreamTaskId, Flow.Publisher<byte[]> body) {

  public DownstreamSseStream {
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
