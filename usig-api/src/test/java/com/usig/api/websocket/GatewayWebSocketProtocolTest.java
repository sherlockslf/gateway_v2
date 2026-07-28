package com.usig.api.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.usig.application.result.CancelRunResult;
import com.usig.domain.run.ConversationId;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunId;
import com.usig.domain.run.RunStatus;
import com.usig.domain.run.TenantId;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class GatewayWebSocketProtocolTest {

  private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
  private final GatewayWebSocketProtocol protocol = new GatewayWebSocketProtocol(objectMapper, 512);

  @Test
  void decodesCancelCommandWithRequiredRunId() {
    WebSocketCommand command =
        protocol.decode(
            """
            {"type":"command.cancel","commandId":"cmd_1","runId":"run_1","tenantId":"tenant_1"}
            """);

    assertEquals(GatewayWebSocketProtocol.COMMAND_CANCEL, command.type());
    assertEquals("cmd_1", command.commandId());
    assertEquals("run_1", command.runId());
    assertEquals("tenant_1", command.tenantId());
  }

  @Test
  void decodesInputPayloadWithoutChangingSemantics() {
    WebSocketCommand command =
        protocol.decode(
            """
            {"type":"command.input","commandId":"cmd_2","runId":"run_1","payload":{"answer":"approved"}}
            """);

    assertEquals("approved", command.payload().get("answer"));
  }

  @Test
  void rejectsOversizedMessageBeforeJsonParsing() {
    GatewayWebSocketProtocol tiny = new GatewayWebSocketProtocol(objectMapper, 8);

    assertThrows(
        IllegalArgumentException.class, () -> tiny.decode("{\"type\":\"command.cancel\"}"));
  }

  @Test
  void encodesCommandResultEnvelope() throws Exception {
    Instant now = Instant.parse("2026-07-27T07:20:00Z");
    Run run =
        new Run(
                new RunId("run_1"),
                new ConversationId("conversation_1"),
                new TenantId("tenant_1"),
                "sample-agent",
                RunStatus.RUNNING,
                null,
                0,
                now,
                now)
            .transitionTo(RunStatus.CANCEL_REQUESTED, Instant.parse("2026-07-27T07:21:00Z"));

    String encoded =
        protocol.cancelAccepted(
            "cmd_1",
            new CancelRunResult(run, com.usig.application.port.DownstreamExecutionState.CANCELED));

    JsonNode json = objectMapper.readTree(encoded);
    assertEquals("command.result", json.get("type").asText());
    assertEquals("cmd_1", json.get("commandId").asText());
    assertEquals("run_1", json.get("runId").asText());
    assertTrue(json.get("accepted").asBoolean());
    assertEquals("CANCEL_REQUESTED", json.get("result").get("status").asText());
  }
}
