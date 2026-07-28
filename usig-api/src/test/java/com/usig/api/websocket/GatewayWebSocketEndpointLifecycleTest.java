package com.usig.api.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.websockets.next.WebSocketConnection;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class GatewayWebSocketEndpointLifecycleTest {

  @Test
  void closeRemovesLocalSessionState() throws Exception {
    GatewayWebSocketEndpoint endpoint =
        new GatewayWebSocketEndpoint(null, null, new ObjectMapper(), options());
    WebSocketSessionAuthorizer authorizer = sessionAuthorizer(endpoint);
    WebSocketConnection connection = connection("connection-1");

    authorizer.register("connection-1");
    assertEquals(1, authorizer.sessionCount());

    endpoint.close(connection);

    assertEquals(0, authorizer.sessionCount());
  }

  private static GatewayWebSocketEndpoint.WebSocketOptions options() {
    return new GatewayWebSocketEndpoint.WebSocketOptions() {
      @Override
      public String allowedOrigins() {
        return "http://localhost";
      }

      @Override
      public boolean allowMissingOrigin() {
        return false;
      }

      @Override
      public boolean requireSecure() {
        return false;
      }

      @Override
      public int maxMessageBytes() {
        return 1024;
      }

      @Override
      public Duration sessionTtl() {
        return Duration.ofMinutes(15);
      }
    };
  }

  private static WebSocketSessionAuthorizer sessionAuthorizer(GatewayWebSocketEndpoint endpoint)
      throws Exception {
    Field field = GatewayWebSocketEndpoint.class.getDeclaredField("sessionAuthorizer");
    field.setAccessible(true);
    return (WebSocketSessionAuthorizer) field.get(endpoint);
  }

  private static WebSocketConnection connection(String id) {
    return (WebSocketConnection)
        Proxy.newProxyInstance(
            WebSocketConnection.class.getClassLoader(),
            new Class<?>[] {WebSocketConnection.class},
            (proxy, method, args) -> {
              if ("id".equals(method.getName())) {
                return id;
              }
              if ("toString".equals(method.getName())) {
                return "WebSocketConnection[" + id + "]";
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }
}
