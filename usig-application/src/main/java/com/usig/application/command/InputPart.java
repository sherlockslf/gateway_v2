package com.usig.application.command;

public record InputPart(InputPartType type, String value, String mimeType) {

  public InputPart {
    if (type == null) {
      throw new IllegalArgumentException("type must not be null");
    }
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("value must not be blank");
    }
    if (type == InputPartType.IMAGE_REF && (mimeType == null || mimeType.isBlank())) {
      throw new IllegalArgumentException("image input must include mimeType");
    }
  }
}
