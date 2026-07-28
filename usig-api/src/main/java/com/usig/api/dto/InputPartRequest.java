package com.usig.api.dto;

import com.usig.application.command.InputPart;
import com.usig.application.command.InputPartType;

public record InputPartRequest(String type, String value, String mimeType) {

  public InputPart toCommandPart() {
    return new InputPart(InputPartType.valueOf(type.toUpperCase()), value, mimeType);
  }
}
