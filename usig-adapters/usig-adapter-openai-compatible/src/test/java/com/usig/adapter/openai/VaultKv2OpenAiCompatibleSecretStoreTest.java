package com.usig.adapter.openai;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class VaultKv2OpenAiCompatibleSecretStoreTest {

  private HttpServer server;

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void readsTenantScopedTokenFromVaultKv2() throws Exception {
    AtomicReference<String> path = new AtomicReference<>();
    AtomicReference<String> token = new AtomicReference<>();
    server =
        server(
            exchange -> {
              path.set(exchange.getRequestURI().getPath());
              token.set(exchange.getRequestHeaders().getFirst("X-Vault-Token"));
              respond(exchange, 200, "{\"data\":{\"data\":{\"bearerToken\":\"tenant-token\"}}}");
            });
    VaultKv2OpenAiCompatibleSecretStore store = store();

    String bearerToken = store.bearerToken(service(), context()).orElseThrow();

    assertEquals("tenant-token", bearerToken);
    assertEquals("/v1/usig/data/tenant_1/model-chat", path.get());
    assertEquals("vault-root-token", token.get());
  }

  @Test
  void fallsBackToServiceScopedTokenWhenTenantSecretIsMissing() throws Exception {
    List<String> paths = new ArrayList<>();
    server =
        server(
            exchange -> {
              paths.add(exchange.getRequestURI().getPath());
              if (paths.size() == 1) {
                respond(exchange, 404, "{\"errors\":[\"missing\"]}");
                return;
              }
              respond(exchange, 200, "{\"data\":{\"data\":{\"bearerToken\":\"service-token\"}}}");
            });
    VaultKv2OpenAiCompatibleSecretStore store = store();

    String bearerToken = store.bearerToken(service(), context()).orElseThrow();

    assertEquals("service-token", bearerToken);
    assertEquals(List.of("/v1/usig/data/tenant_1/model-chat", "/v1/usig/data/model-chat"), paths);
  }

  @Test
  void returnsEmptyWhenVaultKv2TokenFieldIsBlank() throws Exception {
    server =
        server(exchange -> respond(exchange, 200, "{\"data\":{\"data\":{\"bearerToken\":\" \"}}}"));
    VaultKv2OpenAiCompatibleSecretStore store = store();

    assertEquals(java.util.Optional.empty(), store.bearerToken(service(), context()));
  }

  @Test
  void failsWhenVaultReturnsNonMissingErrorStatus() throws Exception {
    server = server(exchange -> respond(exchange, 500, "{\"errors\":[\"boom\"]}"));
    VaultKv2OpenAiCompatibleSecretStore store = store();

    OpenAiCompatibleAdapterException exception =
        assertThrows(
            OpenAiCompatibleAdapterException.class, () -> store.bearerToken(service(), context()));

    assertEquals("Vault secret lookup returned HTTP 500", exception.getMessage());
  }

  private VaultKv2OpenAiCompatibleSecretStore store() {
    return new VaultKv2OpenAiCompatibleSecretStore(
        serverUri(),
        "usig",
        "vault-root-token",
        "bearerToken",
        Duration.ofSeconds(1),
        HttpClient.newHttpClient(),
        new ObjectMapper());
  }

  private URI serverUri() {
    return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
  }

  private HttpServer server(ExchangeHandler handler) throws IOException {
    HttpServer httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    httpServer.createContext("/", exchange -> handler.handle(exchange));
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
