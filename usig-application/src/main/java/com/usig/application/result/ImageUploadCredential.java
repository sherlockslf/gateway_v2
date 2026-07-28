package com.usig.application.result;

import java.time.Instant;
import java.util.Map;

public record ImageUploadCredential(
    String uploadUrl,
    String objectUrl,
    String method,
    Map<String, String> headers,
    Instant expiresAt,
    Instant retentionExpiresAt,
    long maxBytes) {

  public ImageUploadCredential {
    headers = Map.copyOf(headers == null ? Map.of() : headers);
  }
}
