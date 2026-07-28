package com.usig.adapter.openai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.service.ServiceDescriptor;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class HttpKmsOpenAiCompatibleDecryptor implements OpenAiCompatibleKmsDecryptor {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(2);
  private static final String DEFAULT_PLAINTEXT_FIELD = "plaintext";

  private final URI decryptUri;
  private final Optional<String> authorizationHeader;
  private final Optional<String> keyId;
  private final String plaintextFieldName;
  private final Duration timeout;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;

  public HttpKmsOpenAiCompatibleDecryptor(URI decryptUri) {
    this(
        decryptUri,
        null,
        null,
        DEFAULT_PLAINTEXT_FIELD,
        DEFAULT_TIMEOUT,
        HttpClient.newHttpClient(),
        new ObjectMapper());
  }

  public HttpKmsOpenAiCompatibleDecryptor(
      URI decryptUri,
      String authorizationHeader,
      String keyId,
      String plaintextFieldName,
      Duration timeout,
      HttpClient httpClient,
      ObjectMapper objectMapper) {
    this.decryptUri = Objects.requireNonNull(decryptUri, "decryptUri must not be null");
    this.authorizationHeader = optionalText(authorizationHeader);
    this.keyId = optionalText(keyId);
    this.plaintextFieldName = normalize(plaintextFieldName, DEFAULT_PLAINTEXT_FIELD);
    this.timeout = Objects.requireNonNullElse(timeout, DEFAULT_TIMEOUT);
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
  }

  @Override
  public Optional<String> decrypt(
      String ciphertext, ServiceDescriptor service, ExecutionContext context) {
    Objects.requireNonNull(service, "service must not be null");
    Objects.requireNonNull(context, "context must not be null");
    if (ciphertext == null || ciphertext.isBlank()) {
      return Optional.empty();
    }
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(decryptUri)
            .timeout(timeout)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody(ciphertext, service, context)));
    authorizationHeader.ifPresent(value -> builder.header("Authorization", value));
    HttpResponse<String> response;
    try {
      response =
          httpClient.send(
              builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    } catch (IOException exception) {
      throw new OpenAiCompatibleAdapterException("KMS decrypt request failed", exception);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new OpenAiCompatibleAdapterException("KMS decrypt request interrupted", exception);
    }
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new OpenAiCompatibleAdapterException(
          "KMS decrypt request returned HTTP " + response.statusCode());
    }
    return plaintext(response.body());
  }

  private String requestBody(
      String ciphertext, ServiceDescriptor service, ExecutionContext context) {
    try {
      Map<String, Object> body =
          Map.of(
              "ciphertext",
              ciphertext,
              "serviceId",
              service.serviceId(),
              "tenantId",
              context.tenantId().value(),
              "keyId",
              keyId.orElse(""));
      return objectMapper.writeValueAsString(body);
    } catch (IOException exception) {
      throw new OpenAiCompatibleAdapterException(
          "KMS decrypt request is not serializable", exception);
    }
  }

  private Optional<String> plaintext(String body) {
    try {
      JsonNode value = objectMapper.readTree(body).path(plaintextFieldName);
      if (!value.isTextual() || value.asText().isBlank()) {
        return Optional.empty();
      }
      return Optional.of(value.asText());
    } catch (IOException exception) {
      throw new OpenAiCompatibleAdapterException(
          "KMS decrypt response is not valid JSON", exception);
    }
  }

  private static Optional<String> optionalText(String value) {
    if (value == null || value.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(value.trim());
  }

  private static String normalize(String value, String defaultValue) {
    String normalized = value == null ? defaultValue : value.trim();
    if (normalized == null || normalized.isBlank()) {
      throw new IllegalArgumentException("plaintextFieldName must not be blank");
    }
    return normalized;
  }
}
