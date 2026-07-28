package com.usig.infra.objectstorage;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public final class JavaHttpS3CompatibleObjectDeletionClient
    implements S3CompatibleObjectDeletionClient {

  private static final DateTimeFormatter AMZ_DATE =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter DATE_STAMP =
      DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
  private static final String ALGORITHM = "AWS4-HMAC-SHA256";
  private static final String PAYLOAD_HASH = "UNSIGNED-PAYLOAD";

  private final S3CompatibleObjectStorageConfig config;
  private final HttpClient httpClient;

  public JavaHttpS3CompatibleObjectDeletionClient(S3CompatibleObjectStorageConfig config) {
    this(config, HttpClient.newHttpClient());
  }

  JavaHttpS3CompatibleObjectDeletionClient(
      S3CompatibleObjectStorageConfig config, HttpClient httpClient) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
  }

  @Override
  public CompletionStage<Void> deleteObject(String objectKey, Instant now) {
    return httpClient
        .sendAsync(deleteRequest(objectKey, now), HttpResponse.BodyHandlers.discarding())
        .thenApply(
            response -> {
              int statusCode = response.statusCode();
              if (statusCode < 200 || statusCode >= 300) {
                throw new IllegalStateException(
                    "S3-compatible object delete failed with status " + statusCode);
              }
              return null;
            });
  }

  HttpRequest deleteRequest(String objectKey, Instant now) {
    String amzDate = AMZ_DATE.format(now);
    String dateStamp = DATE_STAMP.format(now);
    String canonicalPath = "/" + encodePath(config.bucket()) + "/" + encodePath(objectKey);
    String host = host(config.endpoint());
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("host", host);
    headers.put("x-amz-content-sha256", PAYLOAD_HASH);
    headers.put("x-amz-date", amzDate);
    config.sessionTokenOptional().ifPresent(token -> headers.put("x-amz-security-token", token));
    String signedHeaders = String.join(";", headers.keySet());
    String canonicalHeaders =
        headers.entrySet().stream()
            .map(entry -> entry.getKey() + ":" + entry.getValue() + "\n")
            .reduce("", String::concat);
    String canonicalRequest =
        "DELETE\n"
            + canonicalPath
            + "\n\n"
            + canonicalHeaders
            + "\n"
            + signedHeaders
            + "\n"
            + PAYLOAD_HASH;
    String credentialScope = dateStamp + "/" + config.region() + "/s3/aws4_request";
    String stringToSign =
        ALGORITHM + "\n" + amzDate + "\n" + credentialScope + "\n" + sha256Hex(canonicalRequest);
    String authorization =
        ALGORITHM
            + " Credential="
            + config.accessKeyId()
            + "/"
            + credentialScope
            + ", SignedHeaders="
            + signedHeaders
            + ", Signature="
            + signature(config.secretAccessKey(), dateStamp, config.region(), stringToSign);
    HttpRequest.Builder builder =
        HttpRequest.newBuilder()
            .uri(URI.create(config.endpoint().toString().replaceAll("/+$", "") + canonicalPath))
            .DELETE()
            .header("Authorization", authorization)
            .header("x-amz-content-sha256", PAYLOAD_HASH)
            .header("x-amz-date", amzDate);
    config.sessionTokenOptional().ifPresent(token -> builder.header("x-amz-security-token", token));
    return builder.build();
  }

  private static String signature(
      String secretAccessKey, String dateStamp, String region, String stringToSign) {
    byte[] dateKey = hmac(("AWS4" + secretAccessKey).getBytes(StandardCharsets.UTF_8), dateStamp);
    byte[] dateRegionKey = hmac(dateKey, region);
    byte[] dateRegionServiceKey = hmac(dateRegionKey, "s3");
    byte[] signingKey = hmac(dateRegionServiceKey, "aws4_request");
    return HexFormat.of().formatHex(hmac(signingKey, stringToSign));
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

  private static String encodePath(String value) {
    return java.util.Arrays.stream(value.split("/", -1))
        .map(JavaHttpS3CompatibleObjectDeletionClient::encode)
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
}
