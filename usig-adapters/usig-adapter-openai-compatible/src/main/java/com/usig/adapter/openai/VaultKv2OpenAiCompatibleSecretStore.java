package com.usig.adapter.openai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.service.ServiceDescriptor;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class VaultKv2OpenAiCompatibleSecretStore implements OpenAiCompatibleSecretStore {

  private static final String DEFAULT_MOUNT = "secret";
  private static final String DEFAULT_FIELD = "bearerToken";
  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(2);

  private final URI vaultBaseUri;
  private final String mount;
  private final String vaultToken;
  private final String fieldName;
  private final Duration timeout;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;

  public VaultKv2OpenAiCompatibleSecretStore(URI vaultBaseUri, String vaultToken) {
    this(
        vaultBaseUri,
        DEFAULT_MOUNT,
        vaultToken,
        DEFAULT_FIELD,
        DEFAULT_TIMEOUT,
        HttpClient.newHttpClient(),
        new ObjectMapper());
  }

  public VaultKv2OpenAiCompatibleSecretStore(
      URI vaultBaseUri,
      String mount,
      String vaultToken,
      String fieldName,
      Duration timeout,
      HttpClient httpClient,
      ObjectMapper objectMapper) {
    this.vaultBaseUri = Objects.requireNonNull(vaultBaseUri, "vaultBaseUri must not be null");
    this.mount = normalize(mount, DEFAULT_MOUNT, "mount");
    this.vaultToken = normalize(vaultToken, null, "vaultToken");
    this.fieldName = normalize(fieldName, DEFAULT_FIELD, "fieldName");
    this.timeout = Objects.requireNonNullElse(timeout, DEFAULT_TIMEOUT);
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
  }

  @Override
  public Optional<String> bearerToken(ServiceDescriptor service, ExecutionContext context) {
    Objects.requireNonNull(service, "service must not be null");
    Objects.requireNonNull(context, "context must not be null");
    return firstPresent(
        List.of(
            List.of(context.tenantId().value(), service.serviceId()),
            List.of(service.serviceId())));
  }

  private Optional<String> firstPresent(List<List<String>> paths) {
    for (List<String> path : paths) {
      Optional<String> token = readPath(path);
      if (token.isPresent()) {
        return token;
      }
    }
    return Optional.empty();
  }

  private Optional<String> readPath(List<String> path) {
    HttpRequest request =
        HttpRequest.newBuilder(secretUri(path))
            .timeout(timeout)
            .header("Accept", "application/json")
            .header("X-Vault-Token", vaultToken)
            .GET()
            .build();
    HttpResponse<String> response;
    try {
      response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    } catch (IOException exception) {
      throw new OpenAiCompatibleAdapterException("Vault secret lookup failed", exception);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new OpenAiCompatibleAdapterException("Vault secret lookup interrupted", exception);
    }
    if (response.statusCode() == 404) {
      return Optional.empty();
    }
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new OpenAiCompatibleAdapterException(
          "Vault secret lookup returned HTTP " + response.statusCode());
    }
    return extractToken(response.body());
  }

  private Optional<String> extractToken(String body) {
    try {
      JsonNode value = objectMapper.readTree(body).path("data").path("data").path(fieldName);
      if (!value.isTextual() || value.asText().isBlank()) {
        return Optional.empty();
      }
      return Optional.of(value.asText());
    } catch (IOException exception) {
      throw new OpenAiCompatibleAdapterException(
          "Vault secret response is not valid JSON", exception);
    }
  }

  private URI secretUri(List<String> path) {
    StringBuilder builder = new StringBuilder(normalizedBaseUri());
    builder.append("/v1/").append(segment(mount)).append("/data");
    for (String part : path) {
      builder.append('/').append(segment(part));
    }
    return URI.create(builder.toString());
  }

  private String normalizedBaseUri() {
    String value = vaultBaseUri.toString();
    while (value.endsWith("/")) {
      value = value.substring(0, value.length() - 1);
    }
    return value;
  }

  private static String segment(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }

  private static String normalize(String value, String defaultValue, String name) {
    String normalized = value == null ? defaultValue : value.trim();
    if (normalized == null || normalized.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return normalized;
  }
}
