package com.usig.bootstrap;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

@QuarkusTest
class GatewayWebSocketEndpointTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @TestHTTPResource("/v1/ws")
  URI endpoint;

  @Test
  void acceptsCancelCommandsForMultipleRunsOnOneConnection() throws Exception {
    String firstRunId = createRun("it-ws-1", "conv_ws_1");
    String secondRunId = createRun("it-ws-2", "conv_ws_2");
    CollectingWebSocket listener = new CollectingWebSocket();
    WebSocket webSocket =
        HttpClient.newHttpClient()
            .newWebSocketBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .header("Origin", "http://localhost")
            .header("X-Tenant-Id", "tenant_1")
            .subprotocols("usig.events.v1")
            .buildAsync(webSocketUri(), listener)
            .get(5, TimeUnit.SECONDS);

    webSocket
        .sendText(
            """
            {"type":"command.cancel","commandId":"cmd_ws_1","runId":"%s"}
            """
                .formatted(firstRunId),
            true)
        .get(5, TimeUnit.SECONDS);
    webSocket
        .sendText(
            """
            {"type":"command.cancel","commandId":"cmd_ws_2","runId":"%s"}
            """
                .formatted(secondRunId),
            true)
        .get(5, TimeUnit.SECONDS);

    JsonNode first = OBJECT_MAPPER.readTree(listener.nextMessage());
    JsonNode second = OBJECT_MAPPER.readTree(listener.nextMessage());
    assertCommandAccepted(first, "cmd_ws_1", firstRunId);
    assertCommandAccepted(second, "cmd_ws_2", secondRunId);
    assertEquals("usig.events.v1", webSocket.getSubprotocol());
    webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(5, TimeUnit.SECONDS);
  }

  private static void assertCommandAccepted(JsonNode json, String commandId, String runId) {
    assertEquals("command.result", json.get("type").asText());
    assertEquals(commandId, json.get("commandId").asText());
    assertEquals(runId, json.get("runId").asText());
    assertTrue(json.get("accepted").asBoolean());
  }

  private String createRun(String idempotencyKey, String conversationId) {
    return given()
        .contentType("application/json")
        .header("Idempotency-Key", idempotencyKey)
        .body(
            """
            {
              "conversationId": "%s",
              "tenantId": "tenant_1",
              "serviceId": "sample-agent",
              "input": [{"type": "TEXT", "value": "hello"}]
            }
            """
                .formatted(conversationId))
        .when()
        .post("/v1/runs")
        .then()
        .statusCode(202)
        .extract()
        .path("runId");
  }

  private URI webSocketUri() throws Exception {
    return new URI(
        "ws", null, endpoint.getHost(), endpoint.getPort(), endpoint.getPath(), null, null);
  }

  private static final class CollectingWebSocket implements WebSocket.Listener {

    private final LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>();

    @Override
    public void onOpen(WebSocket webSocket) {
      webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
      messages.add(data.toString());
      webSocket.request(1);
      return null;
    }

    private String nextMessage() throws InterruptedException {
      String message = messages.poll(5, TimeUnit.SECONDS);
      if (message == null) {
        throw new AssertionError("expected websocket message");
      }
      return message;
    }
  }
}
