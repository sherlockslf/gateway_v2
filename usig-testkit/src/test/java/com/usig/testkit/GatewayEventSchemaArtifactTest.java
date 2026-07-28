package com.usig.testkit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.usig.domain.event.EventType;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class GatewayEventSchemaArtifactTest {

  @Test
  void gatewayEventJsonSchemaKeepsRequiredEnvelopeAndEventTypes() throws Exception {
    Path root = findRepositoryRoot();
    String schema = Files.readString(root.resolve("docs/contracts/gateway-event.schema.json"));

    assertContains(schema, "\"title\": \"GatewayEvent\"");
    assertContains(schema, "\"schemaVersion\"");
    assertContains(schema, "\"eventId\"");
    assertContains(schema, "\"eventType\"");
    assertContains(schema, "\"conversationId\"");
    assertContains(schema, "\"runId\"");
    assertContains(schema, "\"sourceService\"");
    assertContains(schema, "\"sourceEventId\"");
    assertContains(schema, "\"sequence\"");
    assertContains(schema, "\"occurredAt\"");
    assertContains(schema, "\"observedAt\"");
    assertContains(schema, "\"payload\"");
    assertContains(schema, "\"metadata\"");
    assertContains(schema, "\"tenantId\"");
    assertContains(schema, "\"traceId\"");
    assertContains(schema, "\"attributes\"");
    assertContains(schema, "\"pattern\": \"^[0-9]+\\\\.[0-9]+$\"");
    assertContains(schema, "\"additionalProperties\": false");

    for (EventType eventType : EventType.values()) {
      assertContains(schema, "\"" + eventType.wireName() + "\"");
    }
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
