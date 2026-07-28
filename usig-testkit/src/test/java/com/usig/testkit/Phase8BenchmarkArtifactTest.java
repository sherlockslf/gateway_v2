package com.usig.testkit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class Phase8BenchmarkArtifactTest {

  @Test
  void benchmarkArtifactsKeepPhase8ScopeAndRustDecisionGate() throws Exception {
    Path root = findRepositoryRoot();
    String benchmarkDoc =
        Files.readString(root.resolve("docs/performance/phase8-production-benchmark.md"));
    String rustTemplate =
        Files.readString(root.resolve("docs/performance/rust-decision-record-template.md"));
    String sseScript = Files.readString(root.resolve("scripts/performance/k6/usig-phase8-sse.js"));
    String webSocketScript =
        Files.readString(root.resolve("scripts/performance/k6/usig-phase8-websocket.js"));

    assertContains(benchmarkDoc, "10,000 idle SSE connections");
    assertContains(benchmarkDoc, "2,000 active Runs at 10 events/s each");
    assertContains(benchmarkDoc, "Rust/Axum data-plane PoC");
    assertContains(benchmarkDoc, "justified");
    assertContains(benchmarkDoc, "memory or tail latency");
    assertContains(benchmarkDoc, "must not redefine the API or event model");
    assertContains(rustTemplate, "Same `GatewayEvent` JSON Schema");
    assertContains(sseScript, "Last-Event-ID");
    assertContains(sseScript, "usig_added_latency_ms");
    assertContains(webSocketScript, "usig.events.v1");
    assertContains(webSocketScript, "command.cancel");
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
