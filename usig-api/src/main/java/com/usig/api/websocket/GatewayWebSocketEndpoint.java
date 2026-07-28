package com.usig.api.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.usig.application.command.SubmitInputCommand;
import com.usig.application.usecase.CancelRunUseCase;
import com.usig.application.usecase.SubmitInputUseCase;
import com.usig.domain.run.RunId;
import com.usig.domain.run.TenantId;
import io.quarkus.websockets.next.CloseReason;
import io.quarkus.websockets.next.HandshakeRequest;
import io.quarkus.websockets.next.OnClose;
import io.quarkus.websockets.next.OnOpen;
import io.quarkus.websockets.next.OnTextMessage;
import io.quarkus.websockets.next.WebSocket;
import io.quarkus.websockets.next.WebSocketConnection;
import io.smallrye.config.ConfigMapping;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@WebSocket(path = "/v1/ws")
public final class GatewayWebSocketEndpoint {

  private static final CloseReason POLICY_VIOLATION = new CloseReason(1008, "policy violation");
  private static final CloseReason PROTOCOL_ERROR = new CloseReason(1002, "protocol error");

  private final CancelRunUseCase cancelRun;
  private final SubmitInputUseCase submitInput;
  private final GatewayWebSocketProtocol protocol;
  private final WebSocketOptions options;
  private final WebSocketSessionAuthorizer sessionAuthorizer;

  @Inject
  public GatewayWebSocketEndpoint(
      CancelRunUseCase cancelRun,
      SubmitInputUseCase submitInput,
      ObjectMapper objectMapper,
      WebSocketOptions options) {
    this.cancelRun = cancelRun;
    this.submitInput = submitInput;
    this.options = options;
    this.protocol = new GatewayWebSocketProtocol(objectMapper, options.maxMessageBytes());
    this.sessionAuthorizer =
        new WebSocketSessionAuthorizer(options.sessionTtl(), Clock.systemUTC());
  }

  @OnOpen
  Uni<Void> open(WebSocketConnection connection) {
    if (!GatewayWebSocketProtocol.SUBPROTOCOL.equals(connection.subprotocol())) {
      return connection.close(PROTOCOL_ERROR);
    }
    if (!originAllowed(connection.handshakeRequest())) {
      return connection.close(POLICY_VIOLATION);
    }
    if (options.requireSecure() && !connection.isSecure()) {
      return connection.close(POLICY_VIOLATION);
    }
    sessionAuthorizer.register(connection.id());
    return Uni.createFrom().voidItem();
  }

  @OnClose
  void close(WebSocketConnection connection) {
    sessionAuthorizer.remove(connection.id());
  }

  @OnTextMessage
  Uni<String> onText(String message, WebSocketConnection connection) {
    if (!originAllowed(connection.handshakeRequest())) {
      return Uni.createFrom()
          .item(
              protocol.rejected(
                  null, null, new IllegalArgumentException("websocket origin is not allowed")));
    }
    WebSocketCommand command;
    try {
      command = protocol.decode(message);
    } catch (RuntimeException exception) {
      return Uni.createFrom().item(protocol.rejected(null, null, exception));
    }
    if (GatewayWebSocketProtocol.COMMAND_AUTH_REFRESH.equals(command.type())) {
      sessionAuthorizer.refresh(connection.id());
      return Uni.createFrom().item(protocol.authRefreshAccepted(command.commandId()));
    }
    if (!sessionAuthorizer.isAuthorized(connection.id())) {
      return Uni.createFrom().item(protocol.authExpired(command.commandId(), command.runId()));
    }
    String tenantId = tenantId(command, connection.handshakeRequest());
    return switch (command.type()) {
      case GatewayWebSocketProtocol.COMMAND_CANCEL -> cancel(command, tenantId);
      case GatewayWebSocketProtocol.COMMAND_INPUT -> input(command, tenantId);
      default ->
          Uni.createFrom()
              .item(
                  protocol.rejected(
                      command.commandId(),
                      command.runId(),
                      new IllegalArgumentException("unsupported websocket command type")));
    };
  }

  private Uni<String> cancel(WebSocketCommand command, String tenantId) {
    return Uni.createFrom()
        .completionStage(cancelRun.cancel(new RunId(command.runId()), new TenantId(tenantId)))
        .map(result -> protocol.cancelAccepted(command.commandId(), result))
        .onFailure()
        .recoverWithItem(
            failure -> protocol.rejected(command.commandId(), command.runId(), failure));
  }

  private Uni<String> input(WebSocketCommand command, String tenantId) {
    SubmitInputCommand inputCommand =
        new SubmitInputCommand(
            new RunId(command.runId()), new TenantId(tenantId), command.payload());
    return Uni.createFrom()
        .completionStage(submitInput.submit(inputCommand))
        .map(ignored -> protocol.inputAccepted(command.commandId(), command.runId()))
        .onFailure()
        .recoverWithItem(
            failure -> protocol.rejected(command.commandId(), command.runId(), failure));
  }

  private String tenantId(WebSocketCommand command, HandshakeRequest request) {
    if (command.tenantId() != null && !command.tenantId().isBlank()) {
      return command.tenantId();
    }
    return request.header("X-Tenant-Id");
  }

  private boolean originAllowed(HandshakeRequest request) {
    String origin = request.header("Origin");
    if ((origin == null || origin.isBlank()) && options.allowMissingOrigin()) {
      return true;
    }
    return allowedOrigins().contains(origin);
  }

  private Set<String> allowedOrigins() {
    return Arrays.stream(options.allowedOrigins().split(","))
        .map(String::trim)
        .filter(origin -> !origin.isBlank())
        .collect(Collectors.toUnmodifiableSet());
  }

  @ConfigMapping(prefix = "usig.websocket")
  public interface WebSocketOptions {

    String allowedOrigins();

    boolean allowMissingOrigin();

    boolean requireSecure();

    int maxMessageBytes();

    Duration sessionTtl();
  }
}
