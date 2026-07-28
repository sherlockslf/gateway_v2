package com.usig.adapter.openai;

import com.usig.application.command.CreateRunCommand;
import com.usig.application.command.InputPart;
import com.usig.application.command.InputPartType;
import com.usig.application.port.ExecutionContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class OpenAiCompatibleRequestMapper {

  OpenAiCompatibleRequest map(
      CreateRunCommand command,
      ExecutionContext context,
      OpenAiCompatibleAdapterConfig config,
      OpenAiCompatibleCredentialProvider credentialProvider) {
    Objects.requireNonNull(command, "command must not be null");
    Objects.requireNonNull(context, "context must not be null");
    Objects.requireNonNull(config, "config must not be null");
    Objects.requireNonNull(credentialProvider, "credentialProvider must not be null");
    if (containsImage(command) && !config.service().capabilities().multimodalInput()) {
      throw new OpenAiCompatibleAdapterException(
          "Service " + config.service().serviceId() + " does not support multimodal input");
    }
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("Accept", "text/event-stream");
    headers.put("Content-Type", "application/json");
    if (config.authHeaderName() != null) {
      credentialProvider
          .bearerToken(config.service(), context)
          .ifPresent(token -> headers.put(config.authHeaderName(), "Bearer " + token));
    }
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("model", model(command, config));
    body.put("stream", true);
    if (config.apiMode() == OpenAiCompatibleApiMode.RESPONSES) {
      body.put(
          "input", List.of(Map.of("role", "user", "content", responsesContent(command.input()))));
    } else {
      body.put(
          "messages",
          List.of(Map.of("role", "user", "content", chatCompletionsContent(command.input()))));
    }
    body.put(
        "metadata",
        Map.of(
            "runId",
            context.runId().value(),
            "tenantId",
            context.tenantId().value(),
            "traceId",
            context.traceId() == null ? "" : context.traceId()));
    return new OpenAiCompatibleRequest(config.service().endpoint(), headers, body);
  }

  private static String model(CreateRunCommand command, OpenAiCompatibleAdapterConfig config) {
    Object value = command.metadata().get("model");
    if (value instanceof String text && !text.isBlank()) {
      return text;
    }
    return config.defaultModel();
  }

  private static List<Map<String, Object>> chatCompletionsContent(List<InputPart> input) {
    List<Map<String, Object>> content = new ArrayList<>();
    for (InputPart part : input) {
      if (part.type() == InputPartType.TEXT) {
        content.add(Map.of("type", "text", "text", part.value()));
      } else if (part.type() == InputPartType.IMAGE_REF) {
        content.add(
            Map.of(
                "type",
                "image_url",
                "image_url",
                Map.of("url", part.value(), "mimeType", part.mimeType())));
      }
    }
    return List.copyOf(content);
  }

  private static List<Map<String, Object>> responsesContent(List<InputPart> input) {
    List<Map<String, Object>> content = new ArrayList<>();
    for (InputPart part : input) {
      if (part.type() == InputPartType.TEXT) {
        content.add(Map.of("type", "input_text", "text", part.value()));
      } else if (part.type() == InputPartType.IMAGE_REF) {
        content.add(responsesImagePart(part));
      }
    }
    return List.copyOf(content);
  }

  private static Map<String, Object> responsesImagePart(InputPart part) {
    Map<String, Object> image = new LinkedHashMap<>();
    image.put("type", "input_image");
    image.put("image_url", part.value());
    if (part.mimeType() != null && !part.mimeType().isBlank()) {
      image.put("mime_type", part.mimeType());
    }
    return Map.copyOf(image);
  }

  private static boolean containsImage(CreateRunCommand command) {
    return command.input().stream().anyMatch(part -> part.type() == InputPartType.IMAGE_REF);
  }
}
