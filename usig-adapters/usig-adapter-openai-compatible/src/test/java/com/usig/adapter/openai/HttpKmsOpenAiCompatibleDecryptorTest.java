package com.usig.adapter.openai;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.run.RunId;
import com.usig.domain.run.TenantId;
import com.usig.domain.service.ServiceCapabilities;
import com.usig.domain.service.ServiceDescriptor;
import com.usig.domain.service.ServiceType;
import com.usig.domain.service.Transport;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class HttpKmsOpenAiCompatibleDecryptorTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private HttpServer server;

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void decryptsCiphertextWithServiceAndTenantContext() throws Exception {
    AtomicReference<String> authorization = new AtomicReference<>();
    AtomicReference<JsonNode> requestBody = new AtomicReference<>();
    server =
        server(
            exchange -> {
              authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
              requestBody.set(OBJECT_MAPPER.readTree(exchange.getRequestBody()));
              respond(exchange, 200, "{\"plaintext\":\"decrypted-token\"}");
            });
    HttpKmsOpenAiCompatibleDecryptor decryptor = decryptor("Bearer kms-token", "key-main");

    String token = decryptor.decrypt("ciphertext-token", service(), context()).orElseThrow();

    assertEquals("decrypted-token", token);
    assertEquals("Bearer kms-token", authorization.get());
    assertEquals("ciphertext-token", requestBody.get().path("ciphertext").asText());
    assertEquals("model-chat", requestBody.get().path("serviceId").asText());
    assertEquals("tenant_1", requestBody.get().path("tenantId").asText());
    assertEquals("key-main", requestBody.get().path("keyId").asText());
  }

  @Test
  void returnsEmptyWhenKmsPlaintextFieldIsBlank() throws Exception {
    server = server(exchange -> respond(exchange, 200, "{\"plaintext\":\" \"}"));
    HttpKmsOpenAiCompatibleDecryptor decryptor = decryptor(null, null);

    assertEquals(Optional.empty(), decryptor.decrypt("ciphertext-token", service(), context()));
  }

  @Test
  void failsWhenKmsReturnsErrorStatus() throws Exception {
    server = server(exchange -> respond(exchange, 503, "{\"error\":\"unavailable\"}"));
    HttpKmsOpenAiCompatibleDecryptor decryptor = decryptor(null, null);

    OpenAiCompatibleAdapterException exception =
        assertThrows(
            OpenAiCompatibleAdapterException.class,
            () -> decryptor.decrypt("ciphertext-token", service(), context()));

    assertEquals("KMS decrypt request returned HTTP 503", exception.getMessage());
  }

  @Test
  void decryptingSecretStoreWrapsEncryptedStore() {
    OpenAiCompatibleSecretStore encryptedStore =
        (service, context) -> Optional.of("encrypted-token");
    OpenAiCompatibleKmsDecryptor decryptor =
        (ciphertext, service, context) -> Optional.of("plain-token");
    KmsDecryptingOpenAiCompatibleSecretStore store =
        new KmsDecryptingOpenAiCompatibleSecretStore(encryptedStore, decryptor);

    assertEquals("plain-token", store.bearerToken(service(), context()).orElseThrow());
  }

  @Test
  void decryptingSecretStoreSkipsKmsWhenEncryptedSecretIsMissing() {
    OpenAiCompatibleSecretStore encryptedStore = (service, context) -> Optional.empty();
    AtomicReference<String> ciphertext = new AtomicReference<>();
    OpenAiCompatibleKmsDecryptor decryptor =
        (value, service, context) -> {
          ciphertext.set(value);
          return Optional.of("plain-token");
        };
    KmsDecryptingOpenAiCompatibleSecretStore store =
        new KmsDecryptingOpenAiCompatibleSecretStore(encryptedStore, decryptor);

    assertEquals(Optional.empty(), store.bearerToken(service(), context()));
    assertTrue(ciphertext.get() == null);
  }

  private HttpKmsOpenAiCompatibleDecryptor decryptor(String authorizationHeader, String keyId) {
    return new HttpKmsOpenAiCompatibleDecryptor(
        serverUri(),
        authorizationHeader,
        keyId,
        "plaintext",
        Duration.ofSeconds(1),
        HttpClient.newHttpClient(),
        OBJECT_MAPPER);
  }

  private URI serverUri() {
    return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/decrypt");
  }

  private HttpServer server(ExchangeHandler handler) throws IOException {
    HttpServer httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    httpServer.createContext("/decrypt", exchange -> handler.handle(exchange));
    httpServer.setExecutor(Executors.newSingleThreadExecutor());
    httpServer.start();
    return httpServer;
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }

  private static ServiceDescriptor service() {
    return new ServiceDescriptor(
        "model-chat",
        ServiceType.MODEL,
        URI.create("http://downstream.example/v1/responses"),
        "openai-compatible",
        Set.of("tenant_1"),
        Set.of(Transport.SSE),
        new ServiceCapabilities(true, true, true, false, false, false, false, false),
        1);
  }

  private static ExecutionContext context() {
    return new ExecutionContext(new RunId("run_1"), new TenantId("tenant_1"), "trace_1", null);
  }

  @FunctionalInterface
  private interface ExchangeHandler {
    void handle(HttpExchange exchange) throws IOException;
  }
}
