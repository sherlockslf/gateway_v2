package com.usig.adapter.generic.sse;

public record SseFrame(String id, String event, String data) {

  public SseFrame {
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
