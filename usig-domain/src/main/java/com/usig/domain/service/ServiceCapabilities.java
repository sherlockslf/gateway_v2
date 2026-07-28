package com.usig.domain.service;

import java.util.Objects;

public record ServiceCapabilities(
    boolean streaming,
    boolean cancel,
    boolean resume,
    boolean replay,
    boolean taskQuery,
    boolean humanInput,
    boolean multimodalInput,
    boolean structuredOutput,
    RetrySafety retrySafety) {

  public ServiceCapabilities(
      boolean streaming,
      boolean cancel,
      boolean resume,
      boolean replay,
      boolean taskQuery,
      boolean humanInput,
      boolean multimodalInput,
      boolean structuredOutput) {
    this(
        streaming,
        cancel,
        resume,
        replay,
        taskQuery,
        humanInput,
        multimodalInput,
        structuredOutput,
        RetrySafety.NONE);
  }

  public ServiceCapabilities {
    retrySafety = Objects.requireNonNull(retrySafety, "retrySafety must not be null");
  }
}
