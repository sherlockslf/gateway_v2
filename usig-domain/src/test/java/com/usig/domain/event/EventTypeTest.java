package com.usig.domain.event;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class EventTypeTest {

  @Test
  void mapsEnumNamesToWireNames() {
    assertEquals("text.delta", EventType.TEXT_DELTA.wireName());
    assertEquals("execution.cancel_requested", EventType.EXECUTION_CANCEL_REQUESTED.wireName());
    assertEquals(EventType.TOOL_ARGUMENTS_DELTA, EventType.fromWireName("tool.arguments.delta"));
  }
}
