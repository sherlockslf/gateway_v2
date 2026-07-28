package com.usig.api.dto;

import com.usig.application.result.ImageUploadCredential;
import java.time.Instant;
import java.util.Map;

public record ImageUploadCredentialResponse(
    String uploadUrl,
    String objectUrl,
    String method,
    Map<String, String> headers,
    Instant expiresAt,
    Instant retentionExpiresAt,
    long maxBytes) {

  public static ImageUploadCredentialResponse from(ImageUploadCredential credential) {
    return new ImageUploadCredentialResponse(
        credential.uploadUrl(),
        credential.objectUrl(),
        credential.method(),
        credential.headers(),
        credential.expiresAt(),
        credential.retentionExpiresAt(),
        credential.maxBytes());
  }
}
