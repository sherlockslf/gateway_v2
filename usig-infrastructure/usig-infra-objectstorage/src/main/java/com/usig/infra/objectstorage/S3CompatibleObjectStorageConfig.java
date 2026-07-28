package com.usig.infra.objectstorage;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

public record S3CompatibleObjectStorageConfig(
    URI endpoint,
    URI publicBaseUri,
    String bucket,
    String region,
    String accessKeyId,
    String secretAccessKey,
    String sessionToken,
    String callbackSigningSecret,
    Duration uploadTtl,
    Duration retentionDuration) {

  public S3CompatibleObjectStorageConfig {
    endpoint = Objects.requireNonNull(endpoint, "endpoint must not be null");
    publicBaseUri = Objects.requireNonNullElse(publicBaseUri, endpoint);
    bucket = requireText(bucket, "bucket");
    region = requireText(region, "region");
    accessKeyId = requireText(accessKeyId, "accessKeyId");
    secretAccessKey = requireText(secretAccessKey, "secretAccessKey");
    sessionToken = normalizeOptional(sessionToken);
    callbackSigningSecret = normalizeOptional(callbackSigningSecret);
    uploadTtl = requirePositive(uploadTtl, "uploadTtl");
    retentionDuration = requirePositive(retentionDuration, "retentionDuration");
  }

  public Optional<String> sessionTokenOptional() {
    return Optional.ofNullable(sessionToken);
  }

  public Optional<String> callbackSigningSecretOptional() {
    return Optional.ofNullable(callbackSigningSecret);
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static String normalizeOptional(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }

  private static Duration requirePositive(Duration value, String name) {
    if (value == null || value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return value;
  }
}
