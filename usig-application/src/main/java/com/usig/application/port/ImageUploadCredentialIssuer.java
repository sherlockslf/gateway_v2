package com.usig.application.port;

import com.usig.application.result.ImageUploadCredential;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceDescriptor;
import java.util.concurrent.CompletionStage;

public interface ImageUploadCredentialIssuer {

  CompletionStage<ImageUploadCredential> issue(
      TenantId tenantId,
      ServiceDescriptor service,
      String filename,
      String mimeType,
      long contentLengthBytes);
}
