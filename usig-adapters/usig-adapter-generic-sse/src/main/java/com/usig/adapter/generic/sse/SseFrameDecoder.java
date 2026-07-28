package com.usig.adapter.generic.sse;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class SseFrameDecoder {

  private final int maxFrameBytes;
  private final StringBuilder currentLine = new StringBuilder();
  private final StringBuilder data = new StringBuilder();
  private String id;
  private String event;
  private int currentFrameBytes;

  public SseFrameDecoder(int maxFrameBytes) {
    if (maxFrameBytes < 1) {
      throw new IllegalArgumentException("maxFrameBytes must be positive");
    }
    this.maxFrameBytes = maxFrameBytes;
  }

  public List<SseFrame> append(byte[] chunk) {
    String text = decodeUtf8(chunk);
    List<SseFrame> frames = new ArrayList<>();
    for (int i = 0; i < text.length(); i++) {
      char character = text.charAt(i);
      if (character == '\r') {
        continue;
      }
      incrementFrameBytes(character);
      if (character == '\n') {
        onLine(currentLine.toString(), frames);
        currentLine.setLength(0);
      } else {
        currentLine.append(character);
      }
    }
    return frames;
  }

  public List<SseFrame> finish() {
    List<SseFrame> frames = new ArrayList<>();
    if (!currentLine.isEmpty()) {
      onLine(currentLine.toString(), frames);
      currentLine.setLength(0);
    }
    if (hasFrameContent()) {
      frames.add(emitFrame());
    }
    return frames;
  }

  private void onLine(String line, List<SseFrame> frames) {
    if (line.isEmpty()) {
      if (hasFrameContent()) {
        frames.add(emitFrame());
      }
      return;
    }
    if (line.startsWith(":")) {
      return;
    }
    int separator = line.indexOf(':');
    String field = separator < 0 ? line : line.substring(0, separator);
    String value = separator < 0 ? "" : line.substring(separator + 1);
    if (value.startsWith(" ")) {
      value = value.substring(1);
    }
    switch (field) {
      case "id" -> id = value;
      case "event" -> event = value;
      case "data" -> appendData(value);
      default -> {
        // Unknown SSE fields are ignored by the SSE specification.
      }
    }
  }

  private void appendData(String value) {
    if (!data.isEmpty()) {
      data.append('\n');
    }
    data.append(value);
  }

  private boolean hasFrameContent() {
    return id != null || event != null || !data.isEmpty();
  }

  private SseFrame emitFrame() {
    SseFrame frame = new SseFrame(id, event, data.toString());
    id = null;
    event = null;
    data.setLength(0);
    currentFrameBytes = 0;
    return frame;
  }

  private void incrementFrameBytes(char character) {
    currentFrameBytes += String.valueOf(character).getBytes(StandardCharsets.UTF_8).length;
    if (currentFrameBytes > maxFrameBytes) {
      throw new GenericSseAdapterException("SSE frame exceeds maxFrameBytes");
    }
  }

  private static String decodeUtf8(byte[] chunk) {
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(chunk))
          .toString();
    } catch (CharacterCodingException ex) {
      throw new GenericSseAdapterException("SSE frame contains invalid UTF-8", ex);
    }
  }
}
