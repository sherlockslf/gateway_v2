package com.usig.testkit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class AdapterReadmeArtifactTest {

  @Test
  void everyAdapterHasReadmeWithCapabilitiesAndTestContract() throws Exception {
    Path root = findRepositoryRoot();
    List<String> adapterModules =
        List.of(
            "usig-adapter-generic-sse",
            "usig-adapter-agent-a2a",
            "usig-adapter-business-workflow",
            "usig-adapter-openai-compatible",
            "usig-adapter-sample-agent");

    for (String module : adapterModules) {
      String readme =
          Files.readString(root.resolve("usig-adapters").resolve(module).resolve("README.md"));
      assertContains(readme, "## Purpose");
      assertContains(readme, "## Capabilities");
      assertContains(readme, "## Contract");
      assertContains(readme, "## Tests");
      assertContains(readme, "streaming");
      assertContains(readme, "cancel");
      assertContains(readme, "resume");
      assertContains(readme, "replay");
      assertContains(readme, "retrySafety");
      assertContains(readme, "GatewayEvent");
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
