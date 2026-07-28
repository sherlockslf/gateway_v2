package com.usig.adapter.agent.a2a;

record AgentA2aSseFrame(String id, String event, String data) {

  AgentA2aSseFrame {
    id = normalize(id);
    event = normalize(event);
    data = data == null ? "" : data;
  }

  private static String normalize(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }
}
