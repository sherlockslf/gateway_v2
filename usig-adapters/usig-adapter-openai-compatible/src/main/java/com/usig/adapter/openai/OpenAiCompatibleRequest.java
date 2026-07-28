package com.usig.adapter.openai;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

public record OpenAiCompatibleRequest(
    URI endpoint, Map<String, String> headers, Map<String, Object> body) {

  public OpenAiCompatibleRequest {
    endpoint = Objects.requireNonNull(endpoint, "endpoint must not be null");
    headers = Map.copyOf(Objects.requireNonNullElse(headers, Map.of()));
    body = Map.copyOf(Objects.requireNonNullElse(body, Map.of()));
  }
}
