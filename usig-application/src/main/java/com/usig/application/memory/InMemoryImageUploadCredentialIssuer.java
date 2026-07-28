package com.usig.application.memory;

import com.usig.application.port.ClockPort;
import com.usig.application.port.ImageUploadCompletionVerifier;
import com.usig.application.port.ImageUploadCredentialIssuer;
import com.usig.application.port.ImageUploadLifecycleCleaner;
import com.usig.application.result.ConfirmedImageUpload;
import com.usig.application.result.ImageUploadCleanupResult;
import com.usig.application.result.ImageUploadCredential;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceDescriptor;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class InMemoryImageUploadCredentialIssuer
    implements ImageUploadCredentialIssuer,
        ImageUploadCompletionVerifier,
        ImageUploadLifecycleCleaner {

  private static final Duration DEFAULT_TTL = Duration.ofMinutes(15);
  private static final Duration DEFAULT_RETENTION = Duration.ofHours(24);
  private static final String DEFAULT_OBJECT_HOST = "cdn.usig.local";

  private final ClockPort clock;
  private final Duration retentionDuration;
  private final Map<String, IssuedImageUpload> issuedUploads = new ConcurrentHashMap<>();

  public InMemoryImageUploadCredentialIssuer(ClockPort clock) {
    this(clock, DEFAULT_RETENTION);
  }

  public InMemoryImageUploadCredentialIssuer(ClockPort clock, Duration retentionDuration) {
    this.clock = clock;
    if (retentionDuration == null || retentionDuration.isZero() || retentionDuration.isNegative()) {
      throw new IllegalArgumentException("retentionDuration must be positive");
    }
    this.retentionDuration = retentionDuration;
  }

  @Override
  public CompletionStage<ImageUploadCredential> issue(
      TenantId tenantId,
      ServiceDescriptor service,
      String filename,
      String mimeType,
      long contentLengthBytes) {
    String objectHost =
        service.allowedImageHosts().stream()
            .min(Comparator.naturalOrder())
            .orElse(DEFAULT_OBJECT_HOST);
    String objectKey =
        "uploads/"
            + segment(tenantId.value())
            + "/"
            + segment(service.serviceId())
            + "/"
            + UUID.randomUUID();
    String objectUrl = "https://" + objectHost + "/" + objectKey;
    String uploadUrl = "https://upload.usig.local/" + objectKey;
    Instant now = clock.now();
    ImageUploadCredential credential =
        new ImageUploadCredential(
            uploadUrl,
            objectUrl,
            "PUT",
            Map.of("Content-Type", mimeType),
            now.plus(DEFAULT_TTL),
            now.plus(retentionDuration),
            contentLengthBytes);
    issuedUploads.put(
        objectUrl,
        new IssuedImageUpload(
            tenantId.value(),
            service.serviceId(),
            mimeType,
            contentLengthBytes,
            credential.expiresAt(),
            credential.retentionExpiresAt(),
            false));
    return CompletableFuture.completedFuture(credential);
  }

  @Override
  public CompletionStage<ConfirmedImageUpload> confirm(
      TenantId tenantId,
      ServiceDescriptor service,
      String objectUrl,
      String mimeType,
      long contentLengthBytes,
      String callbackSignature) {
    IssuedImageUpload issued = issuedUploads.get(objectUrl);
    if (issued == null) {
      throw invalid("image object URL was not issued by this gateway");
    }
    if (!tenantId.value().equals(issued.tenantId())
        || !service.serviceId().equals(issued.serviceId())) {
      throw invalid("image object URL does not belong to this tenant or service");
    }
    if (clock.now().isAfter(issued.expiresAt())) {
      issuedUploads.remove(objectUrl);
      throw invalid("image upload credential has expired");
    }
    if (!mimeType.equals(issued.mimeType())) {
      throw invalid("image MIME type does not match issued credential");
    }
    if (contentLengthBytes != issued.contentLengthBytes()) {
      throw invalid("image content length does not match issued credential");
    }
    issuedUploads.put(
        objectUrl,
        new IssuedImageUpload(
            issued.tenantId(),
            issued.serviceId(),
            issued.mimeType(),
            issued.contentLengthBytes(),
            issued.expiresAt(),
            issued.retentionExpiresAt(),
            true));
    return CompletableFuture.completedFuture(
        new ConfirmedImageUpload(
            objectUrl,
            issued.mimeType(),
            issued.contentLengthBytes(),
            issued.retentionExpiresAt()));
  }

  @Override
  public CompletionStage<ImageUploadCleanupResult> cleanupExpiredUploads() {
    Instant now = clock.now();
    AtomicInteger removed = new AtomicInteger();
    issuedUploads
        .entrySet()
        .removeIf(
            entry -> {
              IssuedImageUpload issued = entry.getValue();
              boolean expired =
                  (!issued.confirmed() && now.isAfter(issued.expiresAt()))
                      || !issued.retentionExpiresAt().isAfter(now);
              if (expired) {
                removed.incrementAndGet();
              }
              return expired;
            });
    return CompletableFuture.completedFuture(new ImageUploadCleanupResult(removed.get()));
  }

  private static String segment(String value) {
    return value.replaceAll("[^A-Za-z0-9._-]", "_");
  }

  private static UseCaseException invalid(String message) {
    return new UseCaseException(ErrorCode.INVALID_REQUEST, message);
  }

  private record IssuedImageUpload(
      String tenantId,
      String serviceId,
      String mimeType,
      long contentLengthBytes,
      Instant expiresAt,
      Instant retentionExpiresAt,
      boolean confirmed) {}
}
