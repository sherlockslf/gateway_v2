package com.usig.api.dto;

import com.usig.application.result.ConfirmedImageUpload;
import java.time.Instant;

public record ConfirmImageUploadResponse(
    String objectUrl, String mimeType, long contentLengthBytes, Instant retentionExpiresAt) {

  public static ConfirmImageUploadResponse from(ConfirmedImageUpload upload) {
    return new ConfirmImageUploadResponse(
        upload.objectUrl(),
        upload.mimeType(),
        upload.contentLengthBytes(),
        upload.retentionExpiresAt());
  }
}
