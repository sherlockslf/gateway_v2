package com.usig.bootstrap;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(GatewayWebSocketSessionTtlTest.ShortSessionTtlProfile.class)
class GatewayWebSocketSessionTtlTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @TestHTTPResource("/v1/ws")
  URI endpoint;

  @Test
  void rejectsCommandsAfterSessionTtlExpires() throws Exception {
    String runId = createRun("it-ws-expired", "conv_ws_expired");
    CollectingWebSocket listener = new CollectingWebSocket();
    WebSocket webSocket = connect(listener);

    Thread.sleep(1100);
    webSocket
        .sendText(
            """
            {"type":"command.cancel","commandId":"cmd_ws_expired","runId":"%s"}
            """
                .formatted(runId),
            true)
        .get(5, TimeUnit.SECONDS);

    JsonNode response = OBJECT_MAPPER.readTree(listener.nextMessage());
    assertEquals("command.result", response.get("type").asText());
    assertEquals("cmd_ws_expired", response.get("commandId").asText());
    assertEquals(runId, response.get("runId").asText());
    assertFalse(response.get("accepted").asBoolean());
    assertEquals("UNAUTHORIZED", response.get("result").get("errorCode").asText());
    webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(5, TimeUnit.SECONDS);
  }

  @Test
  void refreshesSessionTtlForLaterCommands() throws Exception {
    String runId = createRun("it-ws-refresh", "conv_ws_refresh");
    CollectingWebSocket listener = new CollectingWebSocket();
    WebSocket webSocket = connect(listener);

    Thread.sleep(700);
    webSocket
        .sendText(
            """
            {"type":"command.auth.refresh","commandId":"cmd_ws_refresh"}
            """,
            true)
        .get(5, TimeUnit.SECONDS);
    JsonNode refreshResponse = OBJECT_MAPPER.readTree(listener.nextMessage());
    assertEquals("command.result", refreshResponse.get("type").asText());
    assertEquals("cmd_ws_refresh", refreshResponse.get("commandId").asText());
    assertTrue(refreshResponse.get("accepted").asBoolean());
    assertTrue(refreshResponse.get("result").get("renewed").asBoolean());

    Thread.sleep(500);
    webSocket
        .sendText(
            """
            {"type":"command.cancel","commandId":"cmd_ws_cancel_after_refresh","runId":"%s"}
            """
                .formatted(runId),
            true)
        .get(5, TimeUnit.SECONDS);

    JsonNode cancelResponse = OBJECT_MAPPER.readTree(listener.nextMessage());
    assertEquals("command.result", cancelResponse.get("type").asText());
    assertEquals("cmd_ws_cancel_after_refresh", cancelResponse.get("commandId").asText());
    assertEquals(runId, cancelResponse.get("runId").asText());
    assertTrue(cancelResponse.get("accepted").asBoolean());
    webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(5, TimeUnit.SECONDS);
  }

  private WebSocket connect(CollectingWebSocket listener) throws Exception {
    return HttpClient.newHttpClient()
        .newWebSocketBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .header("Origin", "http://localhost")
        .header("X-Tenant-Id", "tenant_1")
        .subprotocols("usig.events.v1")
        .buildAsync(webSocketUri(), listener)
        .get(5, TimeUnit.SECONDS);
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

  public static final class ShortSessionTtlProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("usig.websocket.session-ttl", "PT1S");
    }
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
