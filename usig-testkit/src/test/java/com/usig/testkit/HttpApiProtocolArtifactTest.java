package com.usig.testkit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class HttpApiProtocolArtifactTest {

  @Test
  void httpApiProtocolContractKeepsRequiredSemantics() throws Exception {
    Path root = findRepositoryRoot();
    String contract = Files.readString(root.resolve("docs/contracts/http-api-protocol.md"));

    assertContains(contract, "POST /v1/runs");
    assertContains(contract, "GET /v1/runs/{runId}");
    assertContains(contract, "GET /v1/runs/{runId}/events");
    assertContains(contract, "POST /v1/runs/{runId}:cancel");
    assertContains(contract, "POST /v1/runs/{runId}:input");
    assertContains(contract, "GET /v1/services");
    assertContains(contract, "GET /v1/services/{serviceId}/capabilities");
    assertContains(contract, "POST /v1/services/{serviceId}/image-upload-credential");
    assertContains(contract, "POST /v1/services/{serviceId}/image-upload-callback");
    assertContains(contract, "X-Tenant-Id");
    assertContains(contract, "Idempotency-Key");
    assertContains(contract, "Last-Event-ID");
    assertContains(contract, "text/event-stream");
    assertContains(contract, "GatewayEventDto");
    assertContains(contract, "schemaVersion");
    assertContains(contract, "eventId");
    assertContains(contract, "sourceEventId");
    assertContains(contract, "retrySafety");
    assertContains(contract, "retentionExpiresAt");
    assertContains(contract, "X-USIG-Upload-Callback-Signature");
    assertContains(contract, "TENANT_LIMIT_EXCEEDED");
    assertContains(contract, "DOWNSTREAM_CIRCUIT_OPEN");
    assertContains(contract, "No real identity provider integration");
    assertContains(contract, "No raw downstream frame exposure");
    assertContains(contract, "No vendor-native object storage callback integration");
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
