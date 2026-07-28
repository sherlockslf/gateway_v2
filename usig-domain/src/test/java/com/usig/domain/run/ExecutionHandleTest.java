package com.usig.domain.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExecutionHandleTest {

  @Test
  void acceptsBoundedOpaqueAttributes() {
    ExecutionHandle handle =
        new ExecutionHandle(
            "sample-agent", "sample-agent-v1", "task_1", null, null, Map.of("cursor", "abc"));

    assertEquals("abc", handle.opaqueAttributes().get("cursor"));
  }

  @Test
  void rejectsTooManyOpaqueAttributes() {
    Map<String, String> attributes = new HashMap<>();
    for (int i = 0; i < 33; i++) {
      attributes.put("key" + i, "value");
    }

    assertThrows(
        IllegalArgumentException.class,
        () -> new ExecutionHandle("sample-agent", "sample-agent-v1", null, null, null, attributes));
  }
}
