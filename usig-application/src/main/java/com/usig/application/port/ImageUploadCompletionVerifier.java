package com.usig.application.port;

import com.usig.application.result.ConfirmedImageUpload;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceDescriptor;
import java.util.concurrent.CompletionStage;

public interface ImageUploadCompletionVerifier {

  default CompletionStage<ConfirmedImageUpload> confirm(
      TenantId tenantId,
      ServiceDescriptor service,
      String objectUrl,
      String mimeType,
      long contentLengthBytes) {
    return confirm(tenantId, service, objectUrl, mimeType, contentLengthBytes, null);
  }

  CompletionStage<ConfirmedImageUpload> confirm(
      TenantId tenantId,
      ServiceDescriptor service,
      String objectUrl,
      String mimeType,
      long contentLengthBytes,
      String callbackSignature);
}
