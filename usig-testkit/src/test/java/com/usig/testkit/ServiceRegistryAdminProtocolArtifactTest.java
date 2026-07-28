package com.usig.testkit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ServiceRegistryAdminProtocolArtifactTest {

  @Test
  void serviceRegistryAdminProtocolContractKeepsRequiredSemantics() throws Exception {
    Path root = findRepositoryRoot();
    String contract =
        Files.readString(root.resolve("docs/contracts/service-registry-admin-protocol.md"));

    assertContains(contract, "POST /v1/services:reload");
    assertContains(contract, "GET /v1/services:reload/status");
    assertContains(contract, "X-USIG-Admin-Token");
    assertContains(contract, "usig.service-registry.reload.enabled=false");
    assertContains(contract, "UNAUTHORIZED");
    assertContains(contract, "FORBIDDEN");
    assertContains(contract, "ADAPTER_NOT_FOUND");
    assertContains(contract, "serviceCount");
    assertContains(contract, "lastAttemptedReloadAt");
    assertContains(contract, "successfulReloadCount");
    assertContains(contract, "failedReloadCount");
    assertContains(contract, "lastSuccessfulReloadAt");
    assertContains(contract, "lastSuccessfulServiceCount");
    assertContains(contract, "lastFailedReloadAt");
    assertContains(contract, "lastFailureMessage");
    assertContains(contract, "usig.service-registry.reload.source=none|postgres");
    assertContains(contract, "usig.service-registry.reload.worker.enabled=false");
    assertContains(contract, "No config-center listening");
    assertContains(contract, "No cross-instance service hot reload coordination");
    assertContains(contract, "No real PostgreSQL polling deployment verification");
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
