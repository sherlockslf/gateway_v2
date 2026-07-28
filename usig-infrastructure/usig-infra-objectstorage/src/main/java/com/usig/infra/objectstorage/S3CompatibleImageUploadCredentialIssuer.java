package com.usig.infra.objectstorage;

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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public final class S3CompatibleImageUploadCredentialIssuer
    implements ImageUploadCredentialIssuer,
        ImageUploadCompletionVerifier,
        ImageUploadLifecycleCleaner {

  private static final DateTimeFormatter AMZ_DATE =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter DATE_STAMP =
      DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
  private static final String ALGORITHM = "AWS4-HMAC-SHA256";
  private static final String PAYLOAD_HASH = "UNSIGNED-PAYLOAD";

  private final S3CompatibleObjectStorageConfig config;
  private final ClockPort clock;
  private final Supplier<String> objectIdGenerator;
  private final S3CompatibleObjectDeletionClient objectDeletionClient;
  private final Map<String, IssuedImageUpload> issuedUploads = new ConcurrentHashMap<>();

  public S3CompatibleImageUploadCredentialIssuer(
      S3CompatibleObjectStorageConfig config, ClockPort clock) {
    this(config, clock, () -> UUID.randomUUID().toString());
  }

  S3CompatibleImageUploadCredentialIssuer(
      S3CompatibleObjectStorageConfig config, ClockPort clock, Supplier<String> objectIdGenerator) {
    this(config, clock, objectIdGenerator, new JavaHttpS3CompatibleObjectDeletionClient(config));
  }

  S3CompatibleImageUploadCredentialIssuer(
      S3CompatibleObjectStorageConfig config,
      ClockPort clock,
      Supplier<String> objectIdGenerator,
      S3CompatibleObjectDeletionClient objectDeletionClient) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.objectIdGenerator =
        Objects.requireNonNull(objectIdGenerator, "objectIdGenerator must not be null");
    this.objectDeletionClient =
        Objects.requireNonNull(objectDeletionClient, "objectDeletionClient must not be null");
  }

  @Override
  public CompletionStage<ImageUploadCredential> issue(
      TenantId tenantId,
      ServiceDescriptor service,
      String filename,
      String mimeType,
      long contentLengthBytes) {
    Objects.requireNonNull(tenantId, "tenantId must not be null");
    Objects.requireNonNull(service, "service must not be null");
    String objectKey = objectKey(tenantId, service, filename);
    Instant now = clock.now();
    String uploadUrl = presignedPutUrl(objectKey, mimeType, now);
    String objectUrl = publicObjectUrl(service, objectKey);
    Instant expiresAt = now.plus(config.uploadTtl());
    Instant retentionExpiresAt = now.plus(config.retentionDuration());
    issuedUploads.put(
        objectUrl,
        new IssuedImageUpload(
            tenantId.value(),
            service.serviceId(),
            objectKey,
            mimeType,
            contentLengthBytes,
            expiresAt,
            retentionExpiresAt,
            false));
    return CompletableFuture.completedFuture(
        new ImageUploadCredential(
            uploadUrl,
            objectUrl,
            "PUT",
            Map.of("Content-Type", mimeType),
            expiresAt,
            retentionExpiresAt,
            contentLengthBytes));
  }

  @Override
  public CompletionStage<ConfirmedImageUpload> confirm(
      TenantId tenantId,
      ServiceDescriptor service,
      String objectUrl,
      String mimeType,
      long contentLengthBytes,
      String callbackSignature) {
    verifyCallbackSignature(
        tenantId, service, objectUrl, mimeType, contentLengthBytes, callbackSignature);
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
            issued.objectKey(),
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
    AtomicInteger deletedObjects = new AtomicInteger();
    AtomicInteger failedObjectDeletes = new AtomicInteger();
    issuedUploads
        .entrySet()
        .removeIf(
            entry -> {
              IssuedImageUpload issued = entry.getValue();
              if (issued.retentionExpiresAt().isAfter(now)) {
                if (!issued.confirmed() && now.isAfter(issued.expiresAt())) {
                  removed.incrementAndGet();
                  return true;
                }
                return false;
              }
              if (!issued.confirmed()) {
                removed.incrementAndGet();
                return true;
              }
              try {
                objectDeletionClient
                    .deleteObject(issued.objectKey(), now)
                    .toCompletableFuture()
                    .join();
                removed.incrementAndGet();
                deletedObjects.incrementAndGet();
                return true;
              } catch (RuntimeException exception) {
                failedObjectDeletes.incrementAndGet();
                return false;
              }
            });
    return CompletableFuture.completedFuture(
        new ImageUploadCleanupResult(
            removed.get(), deletedObjects.get(), failedObjectDeletes.get()));
  }

  private void verifyCallbackSignature(
      TenantId tenantId,
      ServiceDescriptor service,
      String objectUrl,
      String mimeType,
      long contentLengthBytes,
      String callbackSignature) {
    config
        .callbackSigningSecretOptional()
        .ifPresent(
            secret -> {
              String supplied = normalizeSignature(callbackSignature);
              if (supplied == null) {
                throw invalid("image upload callback signature is required");
              }
              String expected =
                  callbackSignature(
                      secret, tenantId, service, objectUrl, mimeType, contentLengthBytes);
              if (!java.security.MessageDigest.isEqual(
                  expected.getBytes(StandardCharsets.US_ASCII),
                  supplied.getBytes(StandardCharsets.US_ASCII))) {
                throw invalid("image upload callback signature is invalid");
              }
            });
  }

  private String presignedPutUrl(String objectKey, String mimeType, Instant now) {
    String amzDate = AMZ_DATE.format(now);
    String dateStamp = DATE_STAMP.format(now);
    String credentialScope = dateStamp + "/" + config.region() + "/s3/aws4_request";
    String canonicalPath = canonicalPath(objectKey);
    String host = host(config.endpoint());
    String signedHeaders = "content-type;host";
    Map<String, String> query = new LinkedHashMap<>();
    query.put("X-Amz-Algorithm", ALGORITHM);
    query.put("X-Amz-Credential", config.accessKeyId() + "/" + credentialScope);
    query.put("X-Amz-Date", amzDate);
    query.put("X-Amz-Expires", Long.toString(config.uploadTtl().toSeconds()));
    query.put("X-Amz-SignedHeaders", signedHeaders);
    config.sessionTokenOptional().ifPresent(token -> query.put("X-Amz-Security-Token", token));
    String canonicalQueryWithoutSignature = canonicalQuery(query);
    String canonicalHeaders = "content-type:" + mimeType + "\n" + "host:" + host + "\n";
    String canonicalRequest =
        "PUT\n"
            + canonicalPath
            + "\n"
            + canonicalQueryWithoutSignature
            + "\n"
            + canonicalHeaders
            + "\n"
            + signedHeaders
            + "\n"
            + PAYLOAD_HASH;
    String stringToSign =
        ALGORITHM + "\n" + amzDate + "\n" + credentialScope + "\n" + sha256Hex(canonicalRequest);
    String signature =
        signature(config.secretAccessKey(), dateStamp, config.region(), stringToSign);
    return config.endpoint().toString().replaceAll("/+$", "")
        + canonicalPath
        + "?"
        + canonicalQueryWithoutSignature
        + "&X-Amz-Signature="
        + signature;
  }

  private String objectKey(TenantId tenantId, ServiceDescriptor service, String filename) {
    String suffix = normalizedFilename(filename);
    return "uploads/"
        + segment(tenantId.value())
        + "/"
        + segment(service.serviceId())
        + "/"
        + segment(objectIdGenerator.get())
        + suffix;
  }

  private String publicObjectUrl(ServiceDescriptor service, String objectKey) {
    URI base =
        service.allowedImageHosts().stream()
            .min(Comparator.naturalOrder())
            .map(host -> URI.create("https://" + host))
            .orElse(config.publicBaseUri());
    return base.toString().replaceAll("/+$", "") + "/" + config.bucket() + "/" + objectKey;
  }

  private String canonicalPath(String objectKey) {
    return "/" + encodePath(config.bucket()) + "/" + encodePath(objectKey);
  }

  private static String canonicalQuery(Map<String, String> query) {
    return query.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(entry -> encode(entry.getKey()) + "=" + encode(entry.getValue()))
        .reduce((left, right) -> left + "&" + right)
        .orElse("");
  }

  private static String signature(
      String secretAccessKey, String dateStamp, String region, String stringToSign) {
    byte[] dateKey = hmac(("AWS4" + secretAccessKey).getBytes(StandardCharsets.UTF_8), dateStamp);
    byte[] dateRegionKey = hmac(dateKey, region);
    byte[] dateRegionServiceKey = hmac(dateRegionKey, "s3");
    byte[] signingKey = hmac(dateRegionServiceKey, "aws4_request");
    return HexFormat.of().formatHex(hmac(signingKey, stringToSign));
  }

  static String callbackSignature(
      String secret,
      TenantId tenantId,
      ServiceDescriptor service,
      String objectUrl,
      String mimeType,
      long contentLengthBytes) {
    String canonicalPayload =
        service.serviceId()
            + "\n"
            + tenantId.value()
            + "\n"
            + objectUrl
            + "\n"
            + mimeType
            + "\n"
            + contentLengthBytes;
    return HexFormat.of()
        .formatHex(hmac(secret.getBytes(StandardCharsets.UTF_8), canonicalPayload));
  }

  private static String normalizeSignature(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    String normalized = value.trim();
    if (normalized.regionMatches(true, 0, "sha256=", 0, "sha256=".length())) {
      normalized = normalized.substring("sha256=".length());
    }
    if (!normalized.matches("[0-9a-fA-F]{64}")) {
      return null;
    }
    return normalized.toLowerCase(Locale.ROOT);
  }

  private static byte[] hmac(byte[] key, String value) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key, "HmacSHA256"));
      return mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
    } catch (java.security.GeneralSecurityException exception) {
      throw new IllegalStateException("HMAC-SHA256 is not available", exception);
    }
  }

  private static String sha256Hex(String value) {
    try {
      java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
      return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (java.security.GeneralSecurityException exception) {
      throw new IllegalStateException("SHA-256 is not available", exception);
    }
  }

  private static String host(URI uri) {
    int port = uri.getPort();
    if (port < 0) {
      return uri.getHost();
    }
    return uri.getHost() + ":" + port;
  }

  private static String normalizedFilename(String filename) {
    if (filename == null || filename.isBlank()) {
      return "";
    }
    String normalized = segment(filename);
    if (normalized.isBlank()) {
      return "";
    }
    return "-" + normalized;
  }

  private static String segment(String value) {
    return value == null ? "" : value.replaceAll("[^A-Za-z0-9._-]", "_");
  }

  private static String encodePath(String value) {
    return java.util.Arrays.stream(value.split("/", -1))
        .map(S3CompatibleImageUploadCredentialIssuer::encode)
        .reduce((left, right) -> left + "/" + right)
        .orElse("");
  }

  private static String encode(String value) {
    StringBuilder builder = new StringBuilder();
    for (byte current : value.getBytes(StandardCharsets.UTF_8)) {
      int c = current & 0xff;
      if ((c >= 'A' && c <= 'Z')
          || (c >= 'a' && c <= 'z')
          || (c >= '0' && c <= '9')
          || c == '-'
          || c == '_'
          || c == '.'
          || c == '~') {
        builder.append((char) c);
      } else {
        builder.append('%').append(String.format(Locale.ROOT, "%02X", c));
      }
    }
    return builder.toString();
  }

  private static UseCaseException invalid(String message) {
    return new UseCaseException(ErrorCode.INVALID_REQUEST, message);
  }

  private record IssuedImageUpload(
      String tenantId,
      String serviceId,
      String objectKey,
      String mimeType,
      long contentLengthBytes,
      Instant expiresAt,
      Instant retentionExpiresAt,
      boolean confirmed) {}
}
