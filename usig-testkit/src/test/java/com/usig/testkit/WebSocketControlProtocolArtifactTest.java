package com.usig.testkit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class WebSocketControlProtocolArtifactTest {

  @Test
  void webSocketControlProtocolContractKeepsRequiredSemantics() throws Exception {
    Path root = findRepositoryRoot();
    String contract =
        Files.readString(root.resolve("docs/contracts/websocket-control-protocol.md"));

    assertContains(contract, "/v1/ws");
    assertContains(contract, "usig.events.v1");
    assertContains(contract, "Origin");
    assertContains(contract, "X-Tenant-Id");
    assertContains(contract, "command.cancel");
    assertContains(contract, "command.input");
    assertContains(contract, "command.auth.refresh");
    assertContains(contract, "command.result");
    assertContains(contract, "commandId");
    assertContains(contract, "runId");
    assertContains(contract, "accepted=false");
    assertContains(contract, "UNAUTHORIZED");
    assertContains(contract, "renewed");
    assertContains(contract, "Closing the WebSocket connection removes its local session state");
    assertContains(contract, "usig.websocket.session-ttl=PT15M");
    assertContains(contract, "No raw downstream WebSocket or SSE frames");
    assertContains(contract, "No real identity provider integration");
  }

  private static void assertContains(String text, String expected) {
    assertTrue(text.contains(expected), () -> "missing required marker: " + expected);
  }

  private static Path findRepositoryRoot() {
    Path current = Path.of("").toAbsolutePath();
    while (current != null) {
      if (Files.exists(current.resolve("pom.xml")) && Files.isDirectory(current.resolve("docs"))) {
        return current;
      }
      current = current.getParent();
    }
    throw new IllegalStateException("repository root not found");
  }
}
