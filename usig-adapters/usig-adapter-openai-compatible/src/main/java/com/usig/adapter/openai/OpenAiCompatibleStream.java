package com.usig.adapter.openai;

import java.util.Objects;
import java.util.concurrent.Flow;

public record OpenAiCompatibleStream(String downstreamResponseId, Flow.Publisher<byte[]> body) {

  public OpenAiCompatibleStream {
    downstreamResponseId = normalize(downstreamResponseId);
    body = Objects.requireNonNull(body, "body must not be null");
  }

  private static String normalize(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }
}
