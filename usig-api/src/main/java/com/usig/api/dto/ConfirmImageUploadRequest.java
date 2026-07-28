package com.usig.api.dto;

import com.usig.application.command.ConfirmImageUploadCommand;
import com.usig.domain.run.TenantId;

public record ConfirmImageUploadRequest(
    String tenantId,
    String objectUrl,
    String mimeType,
    long contentLengthBytes,
    String callbackSignature) {

  public ConfirmImageUploadCommand toCommand(String serviceId, String signatureHeader) {
    String signature =
        signatureHeader == null || signatureHeader.isBlank() ? callbackSignature : signatureHeader;
    return new ConfirmImageUploadCommand(
        serviceId, new TenantId(tenantId), objectUrl, mimeType, contentLengthBytes, signature);
  }
}
