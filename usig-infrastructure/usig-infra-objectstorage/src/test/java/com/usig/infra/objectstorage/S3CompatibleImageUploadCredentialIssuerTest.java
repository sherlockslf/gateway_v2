package com.usig.infra.objectstorage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.usig.application.result.ConfirmedImageUpload;
import com.usig.application.result.ImageUploadCredential;
import com.usig.application.usecase.UseCaseException;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class S3CompatibleImageUploadCredentialIssuerTest {

  private static final Instant NOW = Instant.parse("2026-07-27T12:00:00Z");

  @Test
  void issuesS3CompatiblePresignedPutCredential() {
    S3CompatibleImageUploadCredentialIssuer issuer =
        new S3CompatibleImageUploadCredentialIssuer(config(null), () -> NOW, () -> "object_1");

    ImageUploadCredential credential =
        issuer
            .issue(new TenantId("tenant_1"), service(Set.of()), "avatar.png", "image/png", 1024)
            .toCompletableFuture()
            .join();
    URI uploadUri = URI.create(credential.uploadUrl());
    Map<String, String> query = query(uploadUri);

    assertEquals("PUT", credential.method());
    assertEquals(Map.of("Content-Type", "image/png"), credential.headers());
    assertEquals("https", uploadUri.getScheme());
    assertEquals("s3.example.com", uploadUri.getHost());
    assertEquals(
        "/usig-images/uploads/tenant_1/model-chat/object_1-avatar.png", uploadUri.getPath());
    assertEquals("AWS4-HMAC-SHA256", query.get("X-Amz-Algorithm"));
    assertEquals("AKIDEXAMPLE/20260727/us-east-1/s3/aws4_request", query.get("X-Amz-Credential"));
    assertEquals("20260727T120000Z", query.get("X-Amz-Date"));
    assertEquals("900", query.get("X-Amz-Expires"));
    assertEquals("content-type;host", query.get("X-Amz-SignedHeaders"));
    assertTrue(query.get("X-Amz-Signature").matches("[0-9a-f]{64}"));
    assertEquals(
        "https://cdn.example.com/usig-images/uploads/tenant_1/model-chat/object_1-avatar.png",
        credential.objectUrl());
    assertEquals(NOW.plus(Duration.ofMinutes(15)), credential.expiresAt());
    assertEquals(NOW.plus(Duration.ofHours(24)), credential.retentionExpiresAt());
    assertEquals(1024, credential.maxBytes());
  }

  @Test
  void usesServiceAllowedImageHostForObjectUrlWhenConfigured() {
    S3CompatibleImageUploadCredentialIssuer issuer =
        new S3CompatibleImageUploadCredentialIssuer(config(null), () -> NOW, () -> "object_1");

    ImageUploadCredential credential =
        issuer
            .issue(
                new TenantId("tenant_1"),
                service(Set.of("images.example.com")),
                "avatar.png",
                "image/png",
                1024)
            .toCompletableFuture()
            .join();

    assertEquals(
        "https://images.example.com/usig-images/uploads/tenant_1/model-chat/object_1-avatar.png",
        credential.objectUrl());
  }

  @Test
  void includesSessionTokenInPresignedQueryWhenConfigured() {
    S3CompatibleImageUploadCredentialIssuer issuer =
        new S3CompatibleImageUploadCredentialIssuer(
            config("session-token"), () -> NOW, () -> "object_1");

    ImageUploadCredential credential =
        issuer
            .issue(new TenantId("tenant_1"), service(Set.of()), "avatar.png", "image/png", 1024)
            .toCompletableFuture()
            .join();

    assertEquals(
        "session-token", query(URI.create(credential.uploadUrl())).get("X-Amz-Security-Token"));
  }

  @Test
  void confirmsIssuedS3CompatibleUploadWithLocalRecord() {
    S3CompatibleImageUploadCredentialIssuer issuer =
        new S3CompatibleImageUploadCredentialIssuer(config(null), () -> NOW, () -> "object_1");
    ImageUploadCredential credential =
        issuer
            .issue(new TenantId("tenant_1"), service(Set.of()), "avatar.png", "image/png", 1024)
            .toCompletableFuture()
            .join();

    ConfirmedImageUpload confirmed =
        issuer
            .confirm(
                new TenantId("tenant_1"),
                service(Set.of()),
                credential.objectUrl(),
                "image/png",
                1024)
            .toCompletableFuture()
            .join();

    assertEquals(credential.objectUrl(), confirmed.objectUrl());
    assertEquals("image/png", confirmed.mimeType());
    assertEquals(1024, confirmed.contentLengthBytes());
    assertEquals(credential.retentionExpiresAt(), confirmed.retentionExpiresAt());
  }

  @Test
  void confirmsIssuedS3CompatibleUploadWithCallbackSignatureWhenConfigured() {
    S3CompatibleImageUploadCredentialIssuer issuer =
        new S3CompatibleImageUploadCredentialIssuer(
            config(null, "callback-secret"), () -> NOW, () -> "object_1");
    ServiceDescriptor service = service(Set.of());
    TenantId tenantId = new TenantId("tenant_1");
    ImageUploadCredential credential =
        issuer
            .issue(tenantId, service, "avatar.png", "image/png", 1024)
            .toCompletableFuture()
            .join();
    String signature =
        S3CompatibleImageUploadCredentialIssuer.callbackSignature(
            "callback-secret", tenantId, service, credential.objectUrl(), "image/png", 1024);

    ConfirmedImageUpload confirmed =
        issuer
            .confirm(
                tenantId, service, credential.objectUrl(), "image/png", 1024, "sha256=" + signature)
            .toCompletableFuture()
            .join();

    assertEquals(credential.objectUrl(), confirmed.objectUrl());
  }

  @Test
  void rejectsS3CompatibleUploadConfirmationWhenCallbackSignatureIsMissingOrInvalid() {
    S3CompatibleImageUploadCredentialIssuer issuer =
        new S3CompatibleImageUploadCredentialIssuer(
            config(null, "callback-secret"), () -> NOW, () -> "object_1");
    ServiceDescriptor service = service(Set.of());
    TenantId tenantId = new TenantId("tenant_1");
    ImageUploadCredential credential =
        issuer
            .issue(tenantId, service, "avatar.png", "image/png", 1024)
            .toCompletableFuture()
            .join();

    UseCaseException missing =
        assertThrows(
            UseCaseException.class,
            () -> issuer.confirm(tenantId, service, credential.objectUrl(), "image/png", 1024));
    UseCaseException invalid =
        assertThrows(
            UseCaseException.class,
            () ->
                issuer.confirm(
                    tenantId,
                    service,
                    credential.objectUrl(),
                    "image/png",
                    1024,
                    "sha256=0000000000000000000000000000000000000000000000000000000000000000"));

    assertEquals("image upload callback signature is required", missing.getMessage());
    assertEquals("image upload callback signature is invalid", invalid.getMessage());
  }

  @Test
  void rejectsS3CompatibleUploadConfirmationWhenLocalRecordDoesNotMatch() {
    S3CompatibleImageUploadCredentialIssuer issuer =
        new S3CompatibleImageUploadCredentialIssuer(config(null), () -> NOW, () -> "object_1");
    ImageUploadCredential credential =
        issuer
            .issue(new TenantId("tenant_1"), service(Set.of()), "avatar.png", "image/png", 1024)
            .toCompletableFuture()
            .join();

    UseCaseException exception =
        assertThrows(
            UseCaseException.class,
            () ->
                issuer
                    .confirm(
                        new TenantId("tenant_1"),
                        service(Set.of()),
                        credential.objectUrl(),
                        "image/jpeg",
                        1024)
                    .toCompletableFuture()
                    .join());

    assertEquals("image MIME type does not match issued credential", exception.getMessage());
  }

  @Test
  void cleansExpiredS3CompatibleLocalUploadRecords() {
    Instant[] now = {NOW};
    S3CompatibleImageUploadCredentialIssuer issuer =
        new S3CompatibleImageUploadCredentialIssuer(config(null), () -> now[0], () -> "object_1");
    ImageUploadCredential credential =
        issuer
            .issue(new TenantId("tenant_1"), service(Set.of()), "avatar.png", "image/png", 1024)
            .toCompletableFuture()
            .join();

    now[0] = NOW.plus(Duration.ofMinutes(16));

    assertEquals(
        1, issuer.cleanupExpiredUploads().toCompletableFuture().join().removedUploadRecords());
    assertThrows(
        UseCaseException.class,
        () ->
            issuer
                .confirm(
                    new TenantId("tenant_1"),
                    service(Set.of()),
                    credential.objectUrl(),
                    "image/png",
                    1024)
                .toCompletableFuture()
                .join());
  }

  @Test
  void deletesConfirmedS3CompatibleObjectWhenRetentionExpires() {
    Instant[] now = {NOW};
    RecordingDeletionClient deletionClient = new RecordingDeletionClient();
    S3CompatibleImageUploadCredentialIssuer issuer =
        new S3CompatibleImageUploadCredentialIssuer(
            config(null), () -> now[0], () -> "object_1", deletionClient);
    ServiceDescriptor service = service(Set.of());
    TenantId tenantId = new TenantId("tenant_1");
    ImageUploadCredential credential =
        issuer
            .issue(tenantId, service, "avatar.png", "image/png", 1024)
            .toCompletableFuture()
            .join();
    issuer
        .confirm(tenantId, service, credential.objectUrl(), "image/png", 1024)
        .toCompletableFuture()
        .join();

    now[0] = NOW.plus(Duration.ofMinutes(16));
    assertEquals(
        0, issuer.cleanupExpiredUploads().toCompletableFuture().join().removedUploadRecords());

    now[0] = credential.retentionExpiresAt();
    com.usig.application.result.ImageUploadCleanupResult result =
        issuer.cleanupExpiredUploads().toCompletableFuture().join();

    assertEquals(1, result.removedUploadRecords());
    assertEquals(1, result.deletedObjects());
    assertEquals(0, result.failedObjectDeletes());
    assertEquals(List.of("uploads/tenant_1/model-chat/object_1-avatar.png"), deletionClient.keys);
  }

  @Test
  void keepsConfirmedS3CompatibleRecordWhenObjectDeletionFails() {
    Instant[] now = {NOW};
    RecordingDeletionClient deletionClient = new RecordingDeletionClient();
    deletionClient.failures.set(1);
    S3CompatibleImageUploadCredentialIssuer issuer =
        new S3CompatibleImageUploadCredentialIssuer(
            config(null), () -> now[0], () -> "object_1", deletionClient);
    ServiceDescriptor service = service(Set.of());
    TenantId tenantId = new TenantId("tenant_1");
    ImageUploadCredential credential =
        issuer
            .issue(tenantId, service, "avatar.png", "image/png", 1024)
            .toCompletableFuture()
            .join();
    issuer
        .confirm(tenantId, service, credential.objectUrl(), "image/png", 1024)
        .toCompletableFuture()
        .join();
    now[0] = credential.retentionExpiresAt();

    com.usig.application.result.ImageUploadCleanupResult failed =
        issuer.cleanupExpiredUploads().toCompletableFuture().join();
    com.usig.application.result.ImageUploadCleanupResult retried =
        issuer.cleanupExpiredUploads().toCompletableFuture().join();

    assertEquals(0, failed.removedUploadRecords());
    assertEquals(0, failed.deletedObjects());
    assertEquals(1, failed.failedObjectDeletes());
    assertEquals(1, retried.removedUploadRecords());
    assertEquals(1, retried.deletedObjects());
  }

  @Test
  void buildsSignedS3CompatibleDeleteObjectRequest() {
    JavaHttpS3CompatibleObjectDeletionClient client =
        new JavaHttpS3CompatibleObjectDeletionClient(config("session-token"));

    java.net.http.HttpRequest request =
        client.deleteRequest("uploads/tenant_1/model-chat/object_1-avatar.png", NOW);

    assertEquals("DELETE", request.method());
    assertEquals(
        "https://s3.example.com/usig-images/uploads/tenant_1/model-chat/object_1-avatar.png",
        request.uri().toString());
    assertEquals("20260727T120000Z", request.headers().firstValue("x-amz-date").orElseThrow());
    assertEquals(
        "UNSIGNED-PAYLOAD", request.headers().firstValue("x-amz-content-sha256").orElseThrow());
    assertEquals(
        "session-token", request.headers().firstValue("x-amz-security-token").orElseThrow());
    String authorization = request.headers().firstValue("Authorization").orElseThrow();
    assertTrue(authorization.contains("Credential=AKIDEXAMPLE/20260727/us-east-1/s3/aws4_request"));
    assertTrue(
        authorization.contains(
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token"));
    assertTrue(authorization.matches(".*Signature=[0-9a-f]{64}"));
  }

  @Test
  void rejectsInvalidObjectStorageConfig() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new S3CompatibleObjectStorageConfig(
                URI.create("https://s3.example.com"),
                URI.create("https://cdn.example.com"),
                " ",
                "us-east-1",
                "AKIDEXAMPLE",
                "SECRET",
                null,
                null,
                Duration.ofMinutes(15),
                Duration.ofHours(24)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new S3CompatibleObjectStorageConfig(
                URI.create("https://s3.example.com"),
                URI.create("https://cdn.example.com"),
                "usig-images",
                "us-east-1",
                "AKIDEXAMPLE",
                "SECRET",
                null,
                null,
                Duration.ZERO,
                Duration.ofHours(24)));
  }

  private static S3CompatibleObjectStorageConfig config(String sessionToken) {
    return config(sessionToken, null);
  }

  private static S3CompatibleObjectStorageConfig config(
      String sessionToken, String callbackSigningSecret) {
    return new S3CompatibleObjectStorageConfig(
        URI.create("https://s3.example.com"),
        URI.create("https://cdn.example.com"),
        "usig-images",
        "us-east-1",
        "AKIDEXAMPLE",
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        sessionToken,
        callbackSigningSecret,
        Duration.ofMinutes(15),
        Duration.ofHours(24));
  }

  private static ServiceDescriptor service(Set<String> allowedImageHosts) {
    return new ServiceDescriptor(
        "model-chat",
        ServiceType.MODEL,
        URI.create("https://downstream.example/v1/responses"),
        "openai-compatible",
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, true, false, false, false, true, false),
        null,
        com.usig.domain.service.ServiceLimits.defaults(),
        com.usig.domain.service.ServiceCircuitBreakerPolicy.defaults(),
        allowedImageHosts,
        1);
  }

  private static Map<String, String> query(URI uri) {
    return Arrays.stream(uri.getRawQuery().split("&"))
        .map(part -> part.split("=", 2))
        .collect(
            Collectors.toMap(
                part -> decode(part[0]), part -> part.length == 2 ? decode(part[1]) : ""));
  }

  private static String decode(String value) {
    return URLDecoder.decode(value, StandardCharsets.UTF_8);
  }

  private static final class RecordingDeletionClient implements S3CompatibleObjectDeletionClient {
    private final List<String> keys = new ArrayList<>();
    private final AtomicInteger failures = new AtomicInteger();

    @Override
    public CompletionStage<Void> deleteObject(String objectKey, Instant now) {
      keys.add(objectKey);
      if (failures.getAndUpdate(value -> Math.max(0, value - 1)) > 0) {
        return CompletableFuture.failedFuture(new IllegalStateException("delete failed"));
      }
      return CompletableFuture.completedFuture(null);
    }
  }
}
