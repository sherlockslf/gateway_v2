package com.usig.domain.run;

public record ConversationId(String value) {

  public ConversationId {
    value = requireText(value, "conversationId");
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
