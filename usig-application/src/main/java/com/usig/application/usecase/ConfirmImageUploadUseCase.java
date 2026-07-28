package com.usig.application.usecase;

import com.usig.application.command.ConfirmImageUploadCommand;
import com.usig.application.port.ImageUploadCompletionVerifier;
import com.usig.application.port.ServiceRegistry;
import com.usig.application.result.ConfirmedImageUpload;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.service.ServiceDescriptor;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletionStage;

public final class ConfirmImageUploadUseCase {

  private static final long MAX_IMAGE_BYTES = 10L * 1024 * 1024;
  private static final Set<String> SUPPORTED_IMAGE_MIME_TYPES =
      Set.of("image/jpeg", "image/png", "image/webp", "image/gif");

  private final ServiceRegistry serviceRegistry;
  private final ImageUploadCompletionVerifier completionVerifier;

  public ConfirmImageUploadUseCase(
      ServiceRegistry serviceRegistry, ImageUploadCompletionVerifier completionVerifier) {
    this.serviceRegistry = serviceRegistry;
    this.completionVerifier = completionVerifier;
  }

  public CompletionStage<ConfirmedImageUpload> confirm(ConfirmImageUploadCommand command) {
    ServiceDescriptor service =
        serviceRegistry
            .find(command.serviceId())
            .orElseThrow(() -> new UseCaseException(ErrorCode.SERVICE_NOT_FOUND));
    if (!service.visibleToTenant(command.tenantId().value())) {
      throw new UseCaseException(ErrorCode.FORBIDDEN);
    }
    if (!service.capabilities().multimodalInput()) {
      throw new UseCaseException(ErrorCode.SERVICE_CAPABILITY_UNSUPPORTED);
    }
    String mimeType = normalizedMimeType(command.mimeType());
    if (!SUPPORTED_IMAGE_MIME_TYPES.contains(mimeType)) {
      throw invalid("image MIME type is not allowed");
    }
    if (command.contentLengthBytes() <= 0) {
      throw invalid("image content length must be positive");
    }
    if (command.contentLengthBytes() > MAX_IMAGE_BYTES) {
      throw invalid("image content length exceeds maximum");
    }
    if (command.objectUrl() == null || command.objectUrl().isBlank()) {
      throw invalid("image object URL is required");
    }
    return completionVerifier.confirm(
        command.tenantId(),
        service,
        command.objectUrl(),
        mimeType,
        command.contentLengthBytes(),
        command.callbackSignature());
  }

  private static String normalizedMimeType(String value) {
    if (value == null) {
      return "";
    }
    int parameterIndex = value.indexOf(';');
    String mimeType = parameterIndex >= 0 ? value.substring(0, parameterIndex) : value;
    return mimeType.trim().toLowerCase(Locale.ROOT);
  }

  private static UseCaseException invalid(String message) {
    return new UseCaseException(ErrorCode.INVALID_REQUEST, message);
  }
}
