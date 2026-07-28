package com.usig.adapter.openai;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.usig.application.command.CreateRunCommand;
import com.usig.application.port.ExecutionContext;
import com.usig.domain.error.ErrorCode;
import com.usig.domain.event.EventMetadata;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

final class OpenAiCompatibleEventPublisher implements Flow.Publisher<GatewayEvent> {

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private final Flow.Publisher<byte[]> body;
  private final ObjectMapper objectMapper;
  private final CreateRunCommand request;
  private final ExecutionContext context;
  private final String sourceService;
  private final String downstreamResponseId;
  private final int maxFrameBytes;
  private final Duration firstEventTimeout;
  private final Duration idleEventTimeout;
  private final Duration totalExecutionTimeout;

  OpenAiCompatibleEventPublisher(
      Flow.Publisher<byte[]> body,
      ObjectMapper objectMapper,
      CreateRunCommand request,
      ExecutionContext context,
      String sourceService,
      String downstreamResponseId,
      int maxFrameBytes,
      Duration firstEventTimeout,
      Duration idleEventTimeout,
      Duration totalExecutionTimeout) {
    this.body = Objects.requireNonNull(body, "body must not be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    this.request = Objects.requireNonNull(request, "request must not be null");
    this.context = Objects.requireNonNull(context, "context must not be null");
    this.sourceService = Objects.requireNonNull(sourceService, "sourceService must not be null");
    this.downstreamResponseId = downstreamResponseId;
    this.maxFrameBytes = maxFrameBytes;
    this.firstEventTimeout =
        Objects.requireNonNull(firstEventTimeout, "firstEventTimeout must not be null");
    this.idleEventTimeout =
        Objects.requireNonNull(idleEventTimeout, "idleEventTimeout must not be null");
    this.totalExecutionTimeout =
        Objects.requireNonNull(totalExecutionTimeout, "totalExecutionTimeout must not be null");
  }

  @Override
  public void subscribe(Flow.Subscriber<? super GatewayEvent> subscriber) {
    body.subscribe(new StreamingSubscriber(subscriber));
  }

  private final class StreamingSubscriber implements Flow.Subscriber<byte[]>, Flow.Subscription {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private final AtomicBoolean once = new AtomicBoolean();
    private final AtomicReference<CompletableFuture<Void>> firstEventTimer =
        new AtomicReference<>();
    private final AtomicLong idleGeneration = new AtomicLong();
    private final AtomicReference<CompletableFuture<Void>> idleTimer = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Void>> totalExecutionTimer =
        new AtomicReference<>();
    private final StringBuilder buffer = new StringBuilder();
    private Flow.Subscription upstream;
    private long demand;
    private long sequence;
    private boolean terminated;
    private boolean firstEventSettled;

    private StreamingSubscriber(Flow.Subscriber<? super GatewayEvent> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      if (!once.compareAndSet(false, true)) {
        subscription.cancel();
        return;
      }
      upstream = subscription;
      downstream.onSubscribe(this);
      scheduleFirstEventTimeout();
      scheduleTotalExecutionTimeout();
    }

    @Override
    public void request(long requested) {
      if (requested <= 0 || terminated) {
        return;
      }
      demand = addDemand(demand, requested);
      upstream.request(1);
    }

    @Override
    public void cancel() {
      settleFirstEventWatch();
      settleIdleWatch();
      settleTotalExecutionWatch();
      terminated = true;
      upstream.cancel();
    }

    @Override
    public void onNext(byte[] item) {
      if (terminated) {
        return;
      }
      try {
        buffer.append(decode(item));
        if (buffer.length() > maxFrameBytes) {
          fail("OpenAI-compatible SSE frame exceeded maxFrameBytes");
          return;
        }
        drainFrames();
        if (!terminated && demand > 0) {
          upstream.request(1);
        }
      } catch (RuntimeException ex) {
        fail(ex);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      if (!terminated) {
        settleFirstEventWatch();
        settleIdleWatch();
        settleTotalExecutionWatch();
        terminated = true;
        downstream.onError(throwable);
      }
    }

    @Override
    public void onComplete() {
      if (!terminated) {
        settleFirstEventWatch();
        settleIdleWatch();
        settleTotalExecutionWatch();
        terminated = true;
        downstream.onComplete();
      }
    }

    private void drainFrames() {
      int delimiterIndex = buffer.indexOf("\n\n");
      while (delimiterIndex >= 0 && !terminated) {
        String frame = buffer.substring(0, delimiterIndex);
        buffer.delete(0, delimiterIndex + 2);
        if (frame.length() > maxFrameBytes) {
          fail("OpenAI-compatible SSE frame exceeded maxFrameBytes");
          return;
        }
        for (GatewayEvent event : parseFrame(frame)) {
          emit(event);
          if (terminated) {
            return;
          }
        }
        delimiterIndex = buffer.indexOf("\n\n");
      }
    }

    private List<GatewayEvent> parseFrame(String frame) {
      StringBuilder data = new StringBuilder();
      for (String line : frame.replace("\r\n", "\n").split("\n")) {
        if (line.startsWith("data:")) {
          if (!data.isEmpty()) {
            data.append('\n');
          }
          data.append(line.substring("data:".length()).stripLeading());
        }
      }
      if (data.isEmpty()) {
        return List.of();
      }
      String payload = data.toString();
      if ("[DONE]".equals(payload)) {
        return List.of(event(EventType.EXECUTION_COMPLETED, Map.of("reason", "done"), null));
      }
      try {
        return parseJsonPayload(objectMapper.readTree(payload));
      } catch (JsonProcessingException ex) {
        throw new OpenAiCompatibleAdapterException("Invalid OpenAI-compatible JSON chunk", ex);
      }
    }

    private List<GatewayEvent> parseJsonPayload(JsonNode root) {
      String type = text(root, "type");
      if (type.startsWith("response.") || "error".equals(type)) {
        return parseResponsesPayload(root, type);
      }
      if ("image_generation.partial_image".equals(type)) {
        return List.of(
            event(
                EventType.ARTIFACT_UPDATED,
                imagesStreamingPartialImagePayload(root, type),
                imagesStreamingSourceEventId(root, type)));
      }
      if (root.hasNonNull("error")) {
        JsonNode error = root.get("error");
        return List.of(
            event(
                EventType.EXECUTION_FAILED,
                Map.of(
                    "code", text(error, "code"),
                    "type", text(error, "type"),
                    "message", text(error, "message")),
                text(root, "id")));
      }
      List<GatewayEvent> events = new ArrayList<>();
      if (root.hasNonNull("usage")) {
        events.add(
            event(
                EventType.USAGE_UPDATED,
                objectMapper.convertValue(root.get("usage"), MAP_TYPE),
                text(root, "id")));
      }
      JsonNode choices = root.get("choices");
      if (choices != null && choices.isArray()) {
        for (JsonNode choice : choices) {
          JsonNode delta = choice.get("delta");
          if (delta != null) {
            appendDeltaEvents(root, choice, delta, events);
          }
          if (choice.hasNonNull("finish_reason")) {
            events.add(
                event(
                    EventType.TEXT_END,
                    Map.of("finishReason", choice.get("finish_reason").asText()),
                    text(root, "id")));
          }
        }
      }
      return events;
    }

    private List<GatewayEvent> parseResponsesPayload(JsonNode root, String type) {
      return switch (type) {
        case "response.queued" ->
            List.of(
                event(
                    EventType.EXECUTION_ACCEPTED,
                    responsesLifecyclePayload(root),
                    responsesSourceEventId(root)));
        case "response.created" ->
            List.of(
                event(
                    EventType.EXECUTION_STARTED,
                    responsesLifecyclePayload(root),
                    responsesSourceEventId(root)));
        case "response.in_progress" ->
            List.of(
                event(
                    EventType.EXECUTION_RUNNING,
                    responsesLifecyclePayload(root),
                    responsesSourceEventId(root)));
        case "response.output_text.delta" ->
            List.of(
                event(
                    EventType.TEXT_DELTA,
                    responsesOutputTextPayload(root, type, "delta", text(root, "delta")),
                    responsesSourceEventId(root)));
        case "response.refusal.delta" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesRefusalPayload(root, type, "refusal.delta", text(root, "delta")),
                    responsesSourceEventId(root)));
        case "response.refusal.done" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesRefusalPayload(root, type, "refusal.done", text(root, "refusal")),
                    responsesSourceEventId(root)));
        case "response.reasoning_text.delta" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesReasoningTextPayload(root, type, "reasoning.delta"),
                    responsesSourceEventId(root)));
        case "response.reasoning_text.done" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesReasoningTextPayload(root, type, "reasoning.done"),
                    responsesSourceEventId(root)));
        case "response.reasoning_summary_text.delta" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesReasoningSummaryPayload(root, type, "reasoning.summary.delta"),
                    responsesSourceEventId(root)));
        case "response.reasoning_summary_text.done" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesReasoningSummaryPayload(root, type, "reasoning.summary.done"),
                    responsesSourceEventId(root)));
        case "response.reasoning_summary_part.added" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesReasoningSummaryPayload(root, type, "reasoning.summary.part.added"),
                    responsesSourceEventId(root)));
        case "response.reasoning_summary_part.done" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesReasoningSummaryPayload(root, type, "reasoning.summary.part.done"),
                    responsesSourceEventId(root)));
        case "response.function_call_arguments.delta" ->
            List.of(
                event(
                    EventType.TOOL_ARGUMENTS_DELTA,
                    responsesFunctionCallArgumentsPayload(root, type, "delta"),
                    responsesSourceEventId(root)));
        case "response.output_item.added" -> responsesOutputItemAdded(root);
        case "response.function_call_arguments.done" ->
            List.of(
                event(
                    EventType.TOOL_END,
                    responsesFunctionCallArgumentsPayload(root, type, "arguments"),
                    responsesSourceEventId(root)));
        case "response.custom_tool_call_input.delta" ->
            List.of(
                event(
                    EventType.TOOL_ARGUMENTS_DELTA,
                    responsesCustomToolInputDeltaPayload(root, type),
                    responsesSourceEventId(root)));
        case "response.custom_tool_call_input.done" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesCustomToolInputDonePayload(root, type),
                    responsesSourceEventId(root)));
        case "response.output_item.done" -> responsesOutputItemDone(root);
        case "response.output_text.done" ->
            List.of(
                event(
                    EventType.TEXT_END,
                    responsesOutputTextPayload(root, type, "done", text(root, "text")),
                    responsesSourceEventId(root)));
        case "response.output_text.annotation.added" ->
            List.of(
                event(
                    EventType.ARTIFACT_COMPLETED,
                    responsesOutputTextAnnotationPayload(root),
                    responsesSourceEventId(root)));
        case "response.content_part.added" -> responsesContentPartAdded(root);
        case "response.content_part.done" -> responsesContentPartDone(root);
        case "response.file_search_call.in_progress", "response.web_search_call.in_progress" ->
            responsesBuiltinToolStatus(root, type);
        case "response.file_search_call.searching", "response.web_search_call.searching" ->
            responsesBuiltinToolStatus(root, type);
        case "response.file_search_call.completed", "response.web_search_call.completed" ->
            responsesBuiltinToolStatus(root, type);
        case "response.file_search_call.failed",
            "response.file_search_call.incomplete",
            "response.web_search_call.failed",
            "response.web_search_call.incomplete" ->
            responsesBuiltinToolStatus(root, type);
        case "response.code_interpreter_call.in_progress",
            "response.code_interpreter_call.interpreting",
            "response.code_interpreter_call.completed",
            "response.code_interpreter_call.incomplete",
            "response.code_interpreter_call.failed" ->
            responsesCodeInterpreterStatus(root, type);
        case "response.code_interpreter_call_code.delta" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesCodeInterpreterCodePayload(root, type, "delta", text(root, "delta")),
                    responsesSourceEventId(root)));
        case "response.code_interpreter_call_code.done" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesCodeInterpreterCodePayload(root, type, "done", text(root, "code")),
                    responsesSourceEventId(root)));
        case "response.mcp_call_arguments.delta" ->
            List.of(
                event(
                    EventType.TOOL_ARGUMENTS_DELTA,
                    responsesMcpArgumentsDeltaPayload(root, type),
                    responsesSourceEventId(root)));
        case "response.mcp_call_arguments.done" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesMcpArgumentsDonePayload(root, type),
                    responsesSourceEventId(root)));
        case "response.mcp_call.in_progress",
            "response.mcp_call.calling",
            "response.mcp_call.completed",
            "response.mcp_call.incomplete",
            "response.mcp_call.failed" ->
            responsesMcpCallStatus(root, type);
        case "response.mcp_list_tools.in_progress",
            "response.mcp_list_tools.completed",
            "response.mcp_list_tools.failed" ->
            responsesMcpListToolsStatus(root, type);
        case "response.image_generation_call.in_progress",
            "response.image_generation_call.generating",
            "response.image_generation_call.completed",
            "response.image_generation_call.failed",
            "response.image_generation_call.incomplete" ->
            responsesImageGenerationStatus(root, type);
        case "response.image_generation_call.partial_image" ->
            List.of(
                event(
                    EventType.ARTIFACT_UPDATED,
                    responsesImageGenerationPartialPayload(root, type),
                    responsesSourceEventId(root)));
        case "response.output_audio.delta" ->
            List.of(
                event(
                    EventType.ARTIFACT_UPDATED,
                    responsesOutputAudioPayload(root, type, "delta"),
                    responsesSourceEventId(root)));
        case "response.output_audio.done" ->
            List.of(
                event(
                    EventType.ARTIFACT_COMPLETED,
                    responsesOutputAudioPayload(root, type, "done"),
                    responsesSourceEventId(root)));
        case "response.output_audio_transcript.delta" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesOutputAudioTranscriptPayload(root, type, "audio.transcript.delta"),
                    responsesSourceEventId(root)));
        case "response.output_audio_transcript.done" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesOutputAudioTranscriptPayload(root, type, "audio.transcript.done"),
                    responsesSourceEventId(root)));
        case "response.completed" -> responsesCompleted(root);
        case "response.failed", "response.incomplete", "error" ->
            List.of(
                event(
                    EventType.EXECUTION_FAILED,
                    responsesErrorPayload(root),
                    responsesSourceEventId(root)));
        default ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesUnknownEventPayload(root, type),
                    responsesSourceEventId(root)));
      };
    }

    private List<GatewayEvent> responsesBuiltinToolStatus(JsonNode root, String type) {
      String status = type.substring(type.lastIndexOf('.') + 1);
      EventType eventType =
          switch (status) {
            case "in_progress" -> EventType.TOOL_START;
            case "completed" -> EventType.TOOL_END;
            case "failed", "incomplete" -> EventType.TOOL_ERROR;
            default -> EventType.PROGRESS_UPDATED;
          };
      return List.of(
          event(eventType, responsesBuiltinToolPayload(root, type), text(root, "item_id")));
    }

    private Map<String, Object> responsesBuiltinToolPayload(JsonNode root, String type) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "toolCallId",
                  text(root, "item_id"),
                  "toolType",
                  responsesBuiltinToolType(type),
                  "status",
                  type.substring(type.lastIndexOf('.') + 1),
                  "index",
                  root.path("output_index").asInt(0),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private String responsesBuiltinToolType(String type) {
      if (type.startsWith("response.file_search_call.")) {
        return "file_search";
      }
      if (type.startsWith("response.web_search_call.")) {
        return "web_search";
      }
      return "unknown";
    }

    private List<GatewayEvent> responsesCodeInterpreterStatus(JsonNode root, String type) {
      String status = type.substring(type.lastIndexOf('.') + 1);
      EventType eventType =
          switch (status) {
            case "in_progress" -> EventType.TOOL_START;
            case "completed" -> EventType.TOOL_END;
            case "failed", "incomplete" -> EventType.TOOL_ERROR;
            default -> EventType.PROGRESS_UPDATED;
          };
      return List.of(
          event(
              eventType,
              responsesCodeInterpreterStatusPayload(root, type, status),
              text(root, "item_id")));
    }

    private Map<String, Object> responsesCodeInterpreterStatusPayload(
        JsonNode root, String type, String status) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "toolCallId",
                  text(root, "item_id"),
                  "toolType",
                  "code_interpreter",
                  "status",
                  status,
                  "index",
                  root.path("output_index").asInt(0),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesCodeInterpreterCodePayload(
        JsonNode root, String type, String phase, String code) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "extensionType",
                  "code_interpreter.code." + phase,
                  "toolCallId",
                  text(root, "item_id"),
                  "toolType",
                  "code_interpreter",
                  "code",
                  code,
                  "index",
                  root.path("output_index").asInt(0),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private List<GatewayEvent> responsesImageGenerationStatus(JsonNode root, String type) {
      String status = type.substring(type.lastIndexOf('.') + 1);
      EventType eventType =
          switch (status) {
            case "in_progress" -> EventType.TOOL_START;
            case "completed" -> EventType.TOOL_END;
            case "failed", "incomplete" -> EventType.TOOL_ERROR;
            default -> EventType.PROGRESS_UPDATED;
          };
      return List.of(
          event(
              eventType,
              responsesImageGenerationStatusPayload(root, type, status),
              text(root, "item_id")));
    }

    private Map<String, Object> responsesImageGenerationStatusPayload(
        JsonNode root, String type, String status) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "toolCallId",
                  text(root, "item_id"),
                  "toolType",
                  "image_generation",
                  "status",
                  status,
                  "index",
                  root.path("output_index").asInt(0),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesImageGenerationPartialPayload(JsonNode root, String type) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "artifactType",
                  "response.image_generation.partial_image",
                  "itemId",
                  text(root, "item_id"),
                  "outputIndex",
                  root.path("output_index").asInt(0),
                  "partialImageIndex",
                  root.path("partial_image_index").asInt(0),
                  "imageB64",
                  text(root, "partial_image_b64"),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> imagesStreamingPartialImagePayload(JsonNode root, String type) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "artifactType",
                  "image_generation.partial_image",
                  "partialImageIndex",
                  root.path("partial_image_index").asInt(0),
                  "imageB64",
                  text(root, "b64_json"),
                  "downstreamEventType",
                  type));
      if (root.has("created_at")) {
        payload.put("createdAt", root.path("created_at").asLong());
      }
      putIfPresent(payload, "background", text(root, "background"));
      putIfPresent(payload, "outputFormat", text(root, "output_format"));
      putIfPresent(payload, "quality", text(root, "quality"));
      putIfPresent(payload, "size", text(root, "size"));
      return payload;
    }

    private String imagesStreamingSourceEventId(JsonNode root, String type) {
      String eventId = text(root, "event_id");
      if (!eventId.isBlank()) {
        return eventId;
      }
      return type
          + ":"
          + root.path("created_at").asLong(0)
          + ":"
          + root.path("partial_image_index").asInt(0);
    }

    private Map<String, Object> responsesOutputAudioPayload(
        JsonNode root, String type, String phase) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "artifactType",
                  "response.output_audio",
                  "phase",
                  phase,
                  "itemId",
                  text(root, "item_id"),
                  "outputIndex",
                  root.path("output_index").asInt(0),
                  "contentIndex",
                  root.path("content_index").asInt(0),
                  "downstreamEventType",
                  type));
      String audioB64 = text(root, "delta");
      if (!audioB64.isBlank()) {
        payload.put("audioB64", audioB64);
      }
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesOutputAudioTranscriptPayload(
        JsonNode root, String type, String extensionType) {
      String content =
          "response.output_audio_transcript.done".equals(type)
              ? text(root, "transcript")
              : text(root, "delta");
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "extensionType",
                  extensionType,
                  "content",
                  content,
                  "itemId",
                  text(root, "item_id"),
                  "outputIndex",
                  root.path("output_index").asInt(0),
                  "contentIndex",
                  root.path("content_index").asInt(0),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesMcpArgumentsDeltaPayload(JsonNode root, String type) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "index",
                  root.path("output_index").asInt(0),
                  "toolCallId",
                  text(root, "item_id"),
                  "toolType",
                  "mcp",
                  "arguments",
                  text(root, "delta"),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesMcpArgumentsDonePayload(JsonNode root, String type) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "extensionType",
                  "mcp.arguments.done",
                  "index",
                  root.path("output_index").asInt(0),
                  "toolCallId",
                  text(root, "item_id"),
                  "toolType",
                  "mcp",
                  "arguments",
                  text(root, "arguments"),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private List<GatewayEvent> responsesMcpCallStatus(JsonNode root, String type) {
      String status = type.substring(type.lastIndexOf('.') + 1);
      EventType eventType =
          switch (status) {
            case "in_progress" -> EventType.TOOL_START;
            case "completed" -> EventType.TOOL_END;
            case "failed", "incomplete" -> EventType.TOOL_ERROR;
            default -> EventType.PROGRESS_UPDATED;
          };
      return List.of(
          event(
              eventType, responsesMcpCallStatusPayload(root, type, status), text(root, "item_id")));
    }

    private Map<String, Object> responsesMcpCallStatusPayload(
        JsonNode root, String type, String status) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "toolCallId",
                  text(root, "item_id"),
                  "toolType",
                  "mcp",
                  "status",
                  status,
                  "index",
                  root.path("output_index").asInt(0),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesMcpListToolsPayload(JsonNode root, String type) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "toolCallId",
                  text(root, "item_id"),
                  "toolType",
                  "mcp_list_tools",
                  "status",
                  type.substring(type.lastIndexOf('.') + 1),
                  "index",
                  root.path("output_index").asInt(0),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private List<GatewayEvent> responsesMcpListToolsStatus(JsonNode root, String type) {
      String status = type.substring(type.lastIndexOf('.') + 1);
      EventType eventType =
          switch (status) {
            case "in_progress" -> EventType.TOOL_START;
            case "completed" -> EventType.TOOL_END;
            case "failed" -> EventType.TOOL_ERROR;
            default -> EventType.PROGRESS_UPDATED;
          };
      return List.of(
          event(eventType, responsesMcpListToolsPayload(root, type), text(root, "item_id")));
    }

    private Map<String, Object> responsesCustomToolInputDeltaPayload(JsonNode root, String type) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "index",
                  root.path("output_index").asInt(0),
                  "toolCallId",
                  text(root, "item_id"),
                  "toolType",
                  "custom_tool",
                  "input",
                  text(root, "delta"),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesFunctionCallArgumentsPayload(
        JsonNode root, String type, String argumentsField) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "index",
                  root.path("output_index").asInt(0),
                  "toolCallId",
                  text(root, "item_id"),
                  "toolType",
                  "function",
                  "name",
                  text(root, "name"),
                  "arguments",
                  text(root, argumentsField),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesCustomToolInputDonePayload(JsonNode root, String type) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "extensionType",
                  "custom_tool.input.done",
                  "index",
                  root.path("output_index").asInt(0),
                  "toolCallId",
                  text(root, "item_id"),
                  "toolType",
                  "custom_tool",
                  "input",
                  text(root, "input"),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesReasoningSummaryPayload(
        JsonNode root, String type, String extensionType) {
      String content =
          type.startsWith("response.reasoning_summary_part.")
              ? firstPartText(root.get("part"))
              : "response.reasoning_summary_text.done".equals(type)
                  ? text(root, "text")
                  : text(root, "delta");
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "extensionType",
                  extensionType,
                  "content",
                  content,
                  "itemId",
                  text(root, "item_id"),
                  "outputIndex",
                  root.path("output_index").asInt(0),
                  "summaryIndex",
                  root.path("summary_index").asInt(0),
                  "downstreamEventType",
                  type));
      if (root.has("part")) {
        payload.put("partType", text(root.get("part"), "type"));
      }
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesRefusalPayload(
        JsonNode root, String type, String extensionType, String content) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "extensionType",
                  extensionType,
                  "content",
                  content,
                  "itemId",
                  text(root, "item_id"),
                  "outputIndex",
                  root.path("output_index").asInt(0),
                  "contentIndex",
                  root.path("content_index").asInt(0),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesReasoningTextPayload(
        JsonNode root, String type, String extensionType) {
      String content =
          "response.reasoning_text.done".equals(type) ? text(root, "text") : text(root, "delta");
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "extensionType",
                  extensionType,
                  "content",
                  content,
                  "itemId",
                  text(root, "item_id"),
                  "outputIndex",
                  root.path("output_index").asInt(0),
                  "contentIndex",
                  root.path("content_index").asInt(0),
                  "downstreamEventType",
                  type));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesLifecyclePayload(JsonNode root) {
      JsonNode response = root.get("response");
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "responseId",
                  text(response, "id"),
                  "downstreamStatus",
                  text(response, "status"),
                  "downstreamEventType",
                  text(root, "type")));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesUnknownEventPayload(JsonNode root, String type) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "extensionType",
                  "response.raw",
                  "downstreamEventType",
                  type,
                  "rawEvent",
                  objectMapper.convertValue(root, MAP_TYPE)));
      String itemId = text(root, "item_id");
      if (!itemId.isBlank()) {
        payload.put("itemId", itemId);
      }
      if (root.has("output_index")) {
        payload.put("outputIndex", root.path("output_index").asInt(0));
      }
      if (root.has("content_index")) {
        payload.put("contentIndex", root.path("content_index").asInt(0));
      }
      if (root.has("sequence_number")) {
        payload.put("downstreamSequenceNumber", root.path("sequence_number").asLong());
      }
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private List<GatewayEvent> responsesContentPartAdded(JsonNode root) {
      JsonNode part = root.get("part");
      String partType = text(part, "type");
      return switch (partType) {
        case "output_text" ->
            List.of(
                event(
                    EventType.TEXT_START,
                    responsesContentPartPayload(root, part, "response.output_text"),
                    responsesSourceEventId(root)));
        case "reasoning_text" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesContentPartPayload(root, part, "reasoning.part.added"),
                    responsesSourceEventId(root)));
        case "refusal" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesContentPartPayload(root, part, "refusal.part.added"),
                    responsesSourceEventId(root)));
        case "audio", "output_audio" ->
            List.of(
                event(
                    EventType.ARTIFACT_UPDATED,
                    responsesAudioContentPartPayload(root, part, "added"),
                    responsesSourceEventId(root)));
        default ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesUnknownContentPartPayload(root, part, "content_part.added"),
                    responsesSourceEventId(root)));
      };
    }

    private List<GatewayEvent> responsesContentPartDone(JsonNode root) {
      JsonNode part = root.get("part");
      String partType = text(part, "type");
      return switch (partType) {
        case "output_text" ->
            List.of(
                event(
                    EventType.TEXT_END,
                    responsesContentPartPayload(root, part, "response.output_text"),
                    responsesSourceEventId(root)));
        case "reasoning_text" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesContentPartPayload(root, part, "reasoning.part.done"),
                    responsesSourceEventId(root)));
        case "refusal" ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesContentPartPayload(root, part, "refusal.part.done"),
                    responsesSourceEventId(root)));
        case "audio", "output_audio" ->
            List.of(
                event(
                    EventType.ARTIFACT_COMPLETED,
                    responsesAudioContentPartPayload(root, part, "done"),
                    responsesSourceEventId(root)));
        default ->
            List.of(
                event(
                    EventType.EXTENSION,
                    responsesUnknownContentPartPayload(root, part, "content_part.done"),
                    responsesSourceEventId(root)));
      };
    }

    private Map<String, Object> responsesUnknownContentPartPayload(
        JsonNode root, JsonNode part, String extensionType) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "extensionType",
                  extensionType,
                  "itemId",
                  text(root, "item_id"),
                  "outputIndex",
                  root.path("output_index").asInt(0),
                  "contentIndex",
                  root.path("content_index").asInt(0),
                  "partType",
                  text(part, "type"),
                  "downstreamEventType",
                  text(root, "type")));
      if (part != null && !part.isNull()) {
        payload.put("part", objectMapper.convertValue(part, MAP_TYPE));
      }
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesContentPartPayload(
        JsonNode root, JsonNode part, String contentType) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "contentType",
                  contentType,
                  "itemId",
                  text(root, "item_id"),
                  "outputIndex",
                  root.path("output_index").asInt(0),
                  "contentIndex",
                  root.path("content_index").asInt(0),
                  "downstreamEventType",
                  text(root, "type")));
      String text = firstPartText(part);
      if (!text.isBlank()) {
        payload.put("content", text);
      }
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesAudioContentPartPayload(
        JsonNode root, JsonNode part, String phase) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "artifactType",
                  "response.output_audio",
                  "phase",
                  phase,
                  "itemId",
                  text(root, "item_id"),
                  "outputIndex",
                  root.path("output_index").asInt(0),
                  "contentIndex",
                  root.path("content_index").asInt(0),
                  "downstreamEventType",
                  text(root, "type")));
      String audioB64 = text(part, "audio");
      if (!audioB64.isBlank()) {
        payload.put("audioB64", audioB64);
      }
      String transcript = text(part, "transcript");
      if (!transcript.isBlank()) {
        payload.put("transcript", transcript);
      }
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesOutputTextPayload(
        JsonNode root, String type, String phase, String content) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "content",
                  content,
                  "contentType",
                  "response.output_text",
                  "phase",
                  phase,
                  "itemId",
                  text(root, "item_id"),
                  "outputIndex",
                  root.path("output_index").asInt(0),
                  "contentIndex",
                  root.path("content_index").asInt(0),
                  "downstreamEventType",
                  type));
      if (root.has("sequence_number")) {
        payload.put("downstreamSequenceNumber", root.path("sequence_number").asLong());
      }
      putObjectIfPresent(payload, "logprobs", root.get("logprobs"));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private String firstPartText(JsonNode part) {
      if (part == null || part.isNull()) {
        return "";
      }
      JsonNode value = firstNonNull(part, "text", "refusal");
      return value == null ? "" : value.asText();
    }

    private Map<String, Object> responsesOutputTextAnnotationPayload(JsonNode root) {
      Map<String, Object> annotation = Map.of();
      JsonNode annotationNode = root.get("annotation");
      if (annotationNode != null && !annotationNode.isNull()) {
        annotation = objectMapper.convertValue(annotationNode, MAP_TYPE);
      }
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "artifactType",
                  "response.output_text.annotation",
                  "itemId",
                  text(root, "item_id"),
                  "outputIndex",
                  root.path("output_index").asInt(0),
                  "contentIndex",
                  root.path("content_index").asInt(0),
                  "annotationIndex",
                  root.path("annotation_index").asInt(0),
                  "annotation",
                  annotation,
                  "downstreamEventType",
                  text(root, "type")));
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private void addResponseEventMetadata(JsonNode root, Map<String, Object> payload) {
      String eventType = text(root, "type");
      if (!eventType.isBlank()) {
        payload.put("downstreamEventType", eventType);
      }
      String responseId = text(root, "response_id");
      if (!responseId.isBlank()) {
        payload.put("responseId", responseId);
      }
      String eventId = text(root, "event_id");
      if (!eventId.isBlank()) {
        payload.put("downstreamEventId", eventId);
      }
      if (root.has("sequence_number")) {
        payload.put("downstreamSequenceNumber", root.path("sequence_number").asLong());
      }
    }

    private List<GatewayEvent> responsesCompleted(JsonNode root) {
      List<GatewayEvent> events = new ArrayList<>();
      JsonNode usage = root.path("response").get("usage");
      if (usage != null && !usage.isNull()) {
        Map<String, Object> usagePayload =
            new java.util.LinkedHashMap<>(objectMapper.convertValue(usage, MAP_TYPE));
        addResponseEventMetadata(root, usagePayload);
        events.add(event(EventType.USAGE_UPDATED, usagePayload, responsesSourceEventId(root)));
      }
      Map<String, Object> completedPayload =
          new java.util.LinkedHashMap<>(responsesLifecyclePayload(root));
      completedPayload.put("reason", "response.completed");
      events.add(
          event(EventType.EXECUTION_COMPLETED, completedPayload, responsesSourceEventId(root)));
      return events;
    }

    private List<GatewayEvent> responsesOutputItemAdded(JsonNode root) {
      JsonNode item = root.get("item");
      if (!isToolCallItem(item)) {
        if (isShellCallOutputItem(item)) {
          return List.of(
              event(
                  EventType.TOOL_RESULT,
                  responsesShellCallOutputPayload(root, item, "shell_call_output.added"),
                  responsesItemSourceEventId(root, item)));
        }
        if (isComputerCallOutputItem(item)) {
          return List.of(
              event(
                  EventType.ARTIFACT_CREATED,
                  responsesComputerCallOutputPayload(root, item, "computer_call_output.added"),
                  responsesItemSourceEventId(root, item)));
        }
        if (isMcpApprovalRequestItem(item)) {
          return List.of(
              event(
                  EventType.APPROVAL_REQUIRED,
                  responsesMcpApprovalRequestPayload(root, item, "mcp.approval_request.added"),
                  responsesItemSourceEventId(root, item)));
        }
        if (isMcpApprovalResponseItem(item)) {
          return List.of(
              event(
                  EventType.EXTENSION,
                  responsesMcpApprovalResponsePayload(root, item, "mcp.approval_response.added"),
                  responsesItemSourceEventId(root, item)));
        }
        return List.of(
            event(
                EventType.EXTENSION,
                responsesOutputItemExtensionPayload(root, item, "output_item.added"),
                responsesItemSourceEventId(root, item)));
      }
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "index",
                  root.path("output_index").asInt(0),
                  "toolCallId",
                  text(item, "id"),
                  "toolType",
                  responsesOutputItemToolType(item),
                  "name",
                  text(item, "name")));
      if (isShellCallItem(item)) {
        putIfPresent(payload, "callId", text(item, "call_id"));
        putIfPresent(payload, "status", text(item, "status"));
        putIfPresent(payload, "createdBy", text(item, "created_by"));
        putObjectIfPresent(payload, "action", item.get("action"));
      } else if (isComputerCallItem(item)) {
        putIfPresent(payload, "callId", text(item, "call_id"));
        putIfPresent(payload, "status", text(item, "status"));
        putObjectIfPresent(payload, "action", item.get("action"));
        putObjectIfPresent(payload, "actions", item.get("actions"));
        putObjectIfPresent(payload, "pendingSafetyChecks", item.get("pending_safety_checks"));
      } else {
        payload.put("callId", text(item, "call_id"));
      }
      addResponseEventMetadata(root, payload);
      return List.of(event(EventType.TOOL_START, payload, responsesItemSourceEventId(root, item)));
    }

    private List<GatewayEvent> responsesOutputItemDone(JsonNode root) {
      JsonNode item = root.get("item");
      if (!isToolCallItem(item)) {
        if (isShellCallOutputItem(item)) {
          return List.of(
              event(
                  EventType.TOOL_RESULT,
                  responsesShellCallOutputPayload(root, item, "shell_call_output.done"),
                  responsesItemSourceEventId(root, item)));
        }
        if (isComputerCallOutputItem(item)) {
          return List.of(
              event(
                  EventType.ARTIFACT_COMPLETED,
                  responsesComputerCallOutputPayload(root, item, "computer_call_output.done"),
                  responsesItemSourceEventId(root, item)));
        }
        if (isMcpApprovalRequestItem(item)) {
          return List.of(
              event(
                  EventType.EXTENSION,
                  responsesMcpApprovalRequestPayload(root, item, "mcp.approval_request.done"),
                  responsesItemSourceEventId(root, item)));
        }
        if (isMcpApprovalResponseItem(item)) {
          return List.of(
              event(
                  EventType.EXTENSION,
                  responsesMcpApprovalResponsePayload(root, item, "mcp.approval_response.done"),
                  responsesItemSourceEventId(root, item)));
        }
        return List.of(
            event(
                EventType.EXTENSION,
                responsesOutputItemExtensionPayload(root, item, "output_item.done"),
                responsesItemSourceEventId(root, item)));
      }
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "index",
                  root.path("output_index").asInt(0),
                  "toolCallId",
                  text(item, "id"),
                  "toolType",
                  responsesOutputItemToolType(item),
                  "name",
                  text(item, "name")));
      if (isCustomToolCallItem(item)) {
        payload.put("callId", text(item, "call_id"));
        payload.put("input", text(item, "input"));
      } else if (isShellCallItem(item)) {
        putIfPresent(payload, "callId", text(item, "call_id"));
        putIfPresent(payload, "status", text(item, "status"));
        putIfPresent(payload, "createdBy", text(item, "created_by"));
        putObjectIfPresent(payload, "action", item.get("action"));
      } else if (isComputerCallItem(item)) {
        putIfPresent(payload, "callId", text(item, "call_id"));
        putIfPresent(payload, "status", text(item, "status"));
        putObjectIfPresent(payload, "action", item.get("action"));
        putObjectIfPresent(payload, "actions", item.get("actions"));
        putObjectIfPresent(payload, "pendingSafetyChecks", item.get("pending_safety_checks"));
      } else {
        payload.put("arguments", text(item, "arguments"));
      }
      addResponseEventMetadata(root, payload);
      return List.of(event(EventType.TOOL_END, payload, responsesItemSourceEventId(root, item)));
    }

    private Map<String, Object> responsesShellCallOutputPayload(
        JsonNode root, JsonNode item, String phase) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "phase",
                  phase,
                  "index",
                  root.path("output_index").asInt(0),
                  "toolCallId",
                  text(item, "id"),
                  "toolType",
                  responsesShellCallOutputToolType(item),
                  "downstreamEventType",
                  text(root, "type")));
      putIfPresent(payload, "callId", text(item, "call_id"));
      putIfPresent(payload, "status", text(item, "status"));
      putIfPresent(payload, "createdBy", text(item, "created_by"));
      if (item != null && item.has("max_output_length")) {
        payload.put("maxOutputLength", item.path("max_output_length").asInt(0));
      }
      putObjectIfPresent(payload, "output", item == null ? null : item.get("output"));
      if (item != null && !item.isNull()) {
        payload.put("item", objectMapper.convertValue(item, MAP_TYPE));
      }
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesComputerCallOutputPayload(
        JsonNode root, JsonNode item, String artifactType) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "artifactType",
                  artifactType,
                  "index",
                  root.path("output_index").asInt(0),
                  "itemId",
                  text(item, "id"),
                  "itemType",
                  text(item, "type"),
                  "toolType",
                  "computer",
                  "callId",
                  text(item, "call_id"),
                  "downstreamEventType",
                  text(root, "type")));
      putIfPresent(payload, "status", text(item, "status"));
      putObjectIfPresent(payload, "output", item == null ? null : item.get("output"));
      putObjectIfPresent(
          payload,
          "acknowledgedSafetyChecks",
          item == null ? null : item.get("acknowledged_safety_checks"));
      if (item != null && !item.isNull()) {
        payload.put("item", objectMapper.convertValue(item, MAP_TYPE));
      }
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesMcpApprovalRequestPayload(
        JsonNode root, JsonNode item, String extensionType) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "extensionType",
                  extensionType,
                  "index",
                  root.path("output_index").asInt(0),
                  "approvalRequestId",
                  text(item, "id"),
                  "toolType",
                  "mcp",
                  "name",
                  text(item, "name"),
                  "serverLabel",
                  text(item, "server_label"),
                  "arguments",
                  text(item, "arguments"),
                  "downstreamEventType",
                  text(root, "type")));
      if (item != null && !item.isNull()) {
        payload.put("item", objectMapper.convertValue(item, MAP_TYPE));
      }
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesMcpApprovalResponsePayload(
        JsonNode root, JsonNode item, String extensionType) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "extensionType",
                  extensionType,
                  "index",
                  root.path("output_index").asInt(0),
                  "approvalResponseId",
                  text(item, "id"),
                  "approvalRequestId",
                  text(item, "approval_request_id"),
                  "toolType",
                  "mcp",
                  "approved",
                  item.path("approve").asBoolean(false),
                  "downstreamEventType",
                  text(root, "type")));
      String reason = text(item, "reason");
      if (!reason.isBlank()) {
        payload.put("reason", reason);
      }
      if (item != null && !item.isNull()) {
        payload.put("item", objectMapper.convertValue(item, MAP_TYPE));
      }
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private Map<String, Object> responsesOutputItemExtensionPayload(
        JsonNode root, JsonNode item, String extensionType) {
      Map<String, Object> payload =
          new java.util.LinkedHashMap<>(
              Map.of(
                  "extensionType",
                  extensionType,
                  "index",
                  root.path("output_index").asInt(0),
                  "itemId",
                  text(item, "id"),
                  "itemType",
                  text(item, "type"),
                  "status",
                  text(item, "status"),
                  "downstreamEventType",
                  text(root, "type")));
      String role = text(item, "role");
      if (!role.isBlank()) {
        payload.put("role", role);
      }
      if (item != null && !item.isNull()) {
        payload.put("item", objectMapper.convertValue(item, MAP_TYPE));
      }
      addResponseEventMetadata(root, payload);
      return payload;
    }

    private boolean isToolCallItem(JsonNode item) {
      return isFunctionCallItem(item)
          || isCustomToolCallItem(item)
          || isComputerCallItem(item)
          || isShellCallItem(item);
    }

    private boolean isFunctionCallItem(JsonNode item) {
      return item != null && "function_call".equals(text(item, "type"));
    }

    private boolean isCustomToolCallItem(JsonNode item) {
      return item != null && "custom_tool_call".equals(text(item, "type"));
    }

    private boolean isComputerCallItem(JsonNode item) {
      return item != null && "computer_call".equals(text(item, "type"));
    }

    private boolean isComputerCallOutputItem(JsonNode item) {
      return item != null && "computer_call_output".equals(text(item, "type"));
    }

    private boolean isShellCallItem(JsonNode item) {
      return item != null
          && ("local_shell_call".equals(text(item, "type"))
              || "shell_call".equals(text(item, "type")));
    }

    private boolean isShellCallOutputItem(JsonNode item) {
      return item != null
          && ("local_shell_call_output".equals(text(item, "type"))
              || "shell_call_output".equals(text(item, "type")));
    }

    private boolean isMcpApprovalRequestItem(JsonNode item) {
      return item != null && "mcp_approval_request".equals(text(item, "type"));
    }

    private boolean isMcpApprovalResponseItem(JsonNode item) {
      return item != null && "mcp_approval_response".equals(text(item, "type"));
    }

    private String responsesOutputItemToolType(JsonNode item) {
      if (isCustomToolCallItem(item)) {
        return "custom_tool";
      }
      if (isComputerCallItem(item)) {
        return "computer";
      }
      if ("local_shell_call".equals(text(item, "type"))) {
        return "local_shell";
      }
      if ("shell_call".equals(text(item, "type"))) {
        return "shell";
      }
      return "function";
    }

    private String responsesShellCallOutputToolType(JsonNode item) {
      if ("local_shell_call_output".equals(text(item, "type"))) {
        return "local_shell";
      }
      return "shell";
    }

    private void putIfPresent(Map<String, Object> payload, String key, String value) {
      if (!value.isBlank()) {
        payload.put(key, value);
      }
    }

    private void putObjectIfPresent(Map<String, Object> payload, String key, JsonNode value) {
      if (value != null && !value.isNull()) {
        payload.put(key, objectMapper.convertValue(value, Object.class));
      }
    }

    private Map<String, Object> responsesErrorPayload(JsonNode root) {
      Map<String, Object> payload = new java.util.LinkedHashMap<>();
      String downstreamEventType = text(root, "type");
      if (!downstreamEventType.isBlank()) {
        payload.put("type", downstreamEventType);
        payload.put("downstreamEventType", downstreamEventType);
      }
      JsonNode response = root.get("response");
      if (response != null && !response.isNull()) {
        payload.put("responseId", text(response, "id"));
        payload.put("downstreamStatus", text(response, "status"));
      }
      addResponseEventMetadata(root, payload);
      JsonNode error = root.get("error");
      if (error == null || error.isNull()) {
        error = root.path("response").get("error");
      }
      if (error != null && !error.isNull()) {
        payload.put("code", text(error, "code"));
        payload.putIfAbsent("type", text(error, "type"));
        payload.put("errorType", text(error, "type"));
        payload.put("message", text(error, "message"));
        return payload;
      }
      JsonNode incompleteDetails = root.path("response").get("incomplete_details");
      if (incompleteDetails != null && !incompleteDetails.isNull()) {
        Map<String, Object> details = objectMapper.convertValue(incompleteDetails, MAP_TYPE);
        payload.put("incompleteDetails", details);
        payload.put("incompleteReason", text(incompleteDetails, "reason"));
        payload.put("message", text(incompleteDetails, "reason"));
        return payload;
      }
      payload.put("message", text(root, "message"));
      return payload;
    }

    private String responsesSourceEventId(JsonNode root) {
      String itemId = text(root, "item_id");
      if (!itemId.isBlank()) {
        return itemId;
      }
      JsonNode response = root.get("response");
      if (response != null && response.hasNonNull("id")) {
        return response.get("id").asText();
      }
      return text(root, "id");
    }

    private String responsesItemSourceEventId(JsonNode root, JsonNode item) {
      String itemId = text(item, "id");
      return itemId.isBlank() ? responsesSourceEventId(root) : itemId;
    }

    private void appendDeltaEvents(
        JsonNode root, JsonNode choice, JsonNode delta, List<GatewayEvent> events) {
      if (delta.hasNonNull("content")) {
        events.add(
            event(
                EventType.TEXT_DELTA,
                Map.of("content", delta.get("content").asText()),
                text(root, "id")));
      }
      JsonNode reasoning = firstNonNull(delta, "reasoning_content", "reasoning");
      if (reasoning != null) {
        events.add(
            event(
                EventType.EXTENSION,
                Map.of("extensionType", "reasoning.delta", "content", reasoning.asText()),
                text(root, "id")));
      }
      JsonNode toolCalls = delta.get("tool_calls");
      if (toolCalls != null && toolCalls.isArray()) {
        for (JsonNode toolCall : toolCalls) {
          JsonNode function = toolCall.get("function");
          if (function != null && function.hasNonNull("arguments")) {
            events.add(
                event(
                    EventType.TOOL_ARGUMENTS_DELTA,
                    Map.of(
                        "index",
                        choice.path("index").asInt(0),
                        "toolCallId",
                        text(toolCall, "id"),
                        "name",
                        text(function, "name"),
                        "arguments",
                        function.get("arguments").asText()),
                    text(root, "id")));
          }
        }
      }
    }

    private GatewayEvent event(EventType type, Map<String, Object> payload, String sourceEventId) {
      Instant now = Instant.now();
      long currentSequence = sequence;
      sequence++;
      return new GatewayEvent(
          GatewayEvent.CURRENT_SCHEMA_VERSION,
          "evt_" + context.runId().value() + "_" + currentSequence,
          type,
          request.conversationId().value(),
          context.runId().value(),
          downstreamResponseId,
          sourceService,
          sourceEventId,
          currentSequence,
          now,
          now,
          payload,
          new EventMetadata(context.tenantId().value(), context.traceId(), context.attributes()));
    }

    private void emit(GatewayEvent event) {
      if (demand <= 0) {
        fail("OpenAI-compatible adapter received more events than requested");
        return;
      }
      demand--;
      settleFirstEventWatch();
      downstream.onNext(event);
      scheduleIdleTimeout();
    }

    private void fail(String message) {
      fail(new OpenAiCompatibleAdapterException(message));
    }

    private void fail(Throwable throwable) {
      if (!terminated) {
        settleFirstEventWatch();
        settleIdleWatch();
        settleTotalExecutionWatch();
        terminated = true;
        upstream.cancel();
        downstream.onError(throwable);
      }
    }

    private void scheduleFirstEventTimeout() {
      CompletableFuture<Void> timer =
          CompletableFuture.runAsync(
              () -> {
                if (!firstEventSettled && !terminated) {
                  fail(
                      new OpenAiCompatibleAdapterException(
                          ErrorCode.DOWNSTREAM_FIRST_EVENT_TIMEOUT,
                          "OpenAI-compatible adapter firstEventTimeout elapsed"));
                }
              },
              CompletableFuture.delayedExecutor(
                  firstEventTimeout.toMillis(), TimeUnit.MILLISECONDS));
      firstEventTimer.set(timer);
    }

    private void scheduleIdleTimeout() {
      long generation = idleGeneration.incrementAndGet();
      CompletableFuture<Void> previous = idleTimer.getAndSet(null);
      if (previous != null) {
        previous.cancel(false);
      }
      CompletableFuture<Void> timer =
          CompletableFuture.runAsync(
              () -> {
                if (idleGeneration.get() == generation && !terminated) {
                  fail(
                      new OpenAiCompatibleAdapterException(
                          ErrorCode.DOWNSTREAM_IDLE_TIMEOUT,
                          "OpenAI-compatible adapter idleEventTimeout elapsed"));
                }
              },
              CompletableFuture.delayedExecutor(
                  idleEventTimeout.toMillis(), TimeUnit.MILLISECONDS));
      idleTimer.set(timer);
    }

    private void scheduleTotalExecutionTimeout() {
      CompletableFuture<Void> timer =
          CompletableFuture.runAsync(
              () -> {
                if (!terminated) {
                  fail(
                      new OpenAiCompatibleAdapterException(
                          ErrorCode.DOWNSTREAM_TOTAL_TIMEOUT,
                          "OpenAI-compatible adapter totalExecutionTimeout elapsed"));
                }
              },
              CompletableFuture.delayedExecutor(
                  totalExecutionTimeout.toMillis(), TimeUnit.MILLISECONDS));
      totalExecutionTimer.set(timer);
    }

    private void settleFirstEventWatch() {
      firstEventSettled = true;
      CompletableFuture<Void> timer = firstEventTimer.getAndSet(null);
      if (timer != null) {
        timer.cancel(false);
      }
    }

    private void settleIdleWatch() {
      idleGeneration.incrementAndGet();
      CompletableFuture<Void> timer = idleTimer.getAndSet(null);
      if (timer != null) {
        timer.cancel(false);
      }
    }

    private void settleTotalExecutionWatch() {
      CompletableFuture<Void> timer = totalExecutionTimer.getAndSet(null);
      if (timer != null) {
        timer.cancel(false);
      }
    }
  }

  private static String decode(byte[] item) {
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(item))
          .toString();
    } catch (CharacterCodingException ex) {
      throw new OpenAiCompatibleAdapterException("Invalid UTF-8 in OpenAI-compatible stream", ex);
    }
  }

  private static JsonNode firstNonNull(JsonNode node, String first, String second) {
    JsonNode value = node.get(first);
    if (value != null && !value.isNull()) {
      return value;
    }
    value = node.get(second);
    return value != null && !value.isNull() ? value : null;
  }

  private static String text(JsonNode node, String field) {
    if (node == null || node.isNull()) {
      return "";
    }
    JsonNode value = node.get(field);
    return value == null || value.isNull() ? "" : value.asText();
  }

  private static long addDemand(long current, long requested) {
    long next = current + requested;
    return next < 0 ? Long.MAX_VALUE : next;
  }
}
