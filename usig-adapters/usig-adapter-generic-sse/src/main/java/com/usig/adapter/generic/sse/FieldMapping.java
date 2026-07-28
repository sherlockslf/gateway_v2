package com.usig.adapter.generic.sse;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;

public record FieldMapping(
    String jsonPointer, Object fixedValue, Object defaultValue, String template) {

  public static FieldMapping pointer(String jsonPointer) {
    return new FieldMapping(requirePointer(jsonPointer), null, null, null);
  }

  public static FieldMapping pointerOrDefault(String jsonPointer, Object defaultValue) {
    return new FieldMapping(requirePointer(jsonPointer), null, defaultValue, null);
  }

  public static FieldMapping fixed(Object fixedValue) {
    if (fixedValue == null) {
      throw new IllegalArgumentException("fixedValue must not be null");
    }
    return new FieldMapping(null, fixedValue, null, null);
  }

  public static FieldMapping template(String template) {
    if (template == null || template.isBlank()) {
      throw new IllegalArgumentException("template must not be blank");
    }
    return new FieldMapping(null, null, null, template);
  }

  public Object resolve(JsonNode root, Map<String, String> variables) {
    if (fixedValue != null) {
      return fixedValue;
    }
    if (template != null) {
      return resolveTemplate(template, variables);
    }
    JsonNode node = root.at(jsonPointer);
    if (node.isMissingNode() || node.isNull()) {
      return defaultValue;
    }
    if (node.isNumber()) {
      return node.numberValue();
    }
    if (node.isBoolean()) {
      return node.booleanValue();
    }
    if (node.isTextual()) {
      return node.textValue();
    }
    return node;
  }

  private static String resolveTemplate(String value, Map<String, String> variables) {
    String result = value;
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      result = result.replace("${" + entry.getKey() + "}", entry.getValue());
    }
    if (result.contains("${")) {
      throw new GenericSseAdapterException("Unresolved template variable in mapping");
    }
    return result;
  }

  private static String requirePointer(String jsonPointer) {
    if (jsonPointer == null || jsonPointer.isBlank() || !jsonPointer.startsWith("/")) {
      throw new IllegalArgumentException("jsonPointer must start with /");
    }
    return jsonPointer;
  }
}
