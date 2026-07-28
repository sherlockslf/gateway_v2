package com.usig.domain.run;

public record RunId(String value) {

  public RunId {
    value = requireText(value, "runId");
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
