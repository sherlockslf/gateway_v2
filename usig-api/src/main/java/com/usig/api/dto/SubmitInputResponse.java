package com.usig.api.dto;

public record SubmitInputResponse(String runId, boolean accepted) {

  public static SubmitInputResponse accepted(String runId) {
    return new SubmitInputResponse(runId, true);
  }
}
