package com.usig.api.dto;

import com.usig.application.command.RequestImageUploadCredentialCommand;
import com.usig.domain.run.TenantId;

public record ImageUploadCredentialRequest(
    String tenantId, String filename, String mimeType, long contentLengthBytes) {

  public RequestImageUploadCredentialCommand toCommand(String serviceId) {
    return new RequestImageUploadCredentialCommand(
        serviceId, new TenantId(tenantId), filename, mimeType, contentLengthBytes);
  }
}
