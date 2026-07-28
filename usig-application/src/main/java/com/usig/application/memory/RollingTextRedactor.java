package com.usig.application.memory;

import java.util.List;

final class RollingTextRedactor {

  private final List<String> literals;

  RollingTextRedactor(List<String> literals) {
    this.literals = List.copyOf(literals);
  }

  String redact(String value) {
    String redacted = value;
    for (String literal : literals) {
      if (!literal.isBlank()) {
        redacted = redacted.replace(literal, "[REDACTED]");
      }
    }
    return redacted;
  }
}
