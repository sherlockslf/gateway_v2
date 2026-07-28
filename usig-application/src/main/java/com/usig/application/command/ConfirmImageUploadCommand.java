package com.usig.application.command;

import com.usig.domain.run.TenantId;

public record ConfirmImageUploadCommand(
    String serviceId,
    TenantId tenantId,
    String objectUrl,
    String mimeType,
    long contentLengthBytes,
    String callbackSignature) {

  public ConfirmImageUploadCommand(
      String serviceId,
      TenantId tenantId,
      String objectUrl,
      String mimeType,
      long contentLengthBytes) {
    this(serviceId, tenantId, objectUrl, mimeType, contentLengthBytes, null);
  }
}
