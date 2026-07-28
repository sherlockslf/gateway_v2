package com.usig.application.pipeline;

public enum StageErrorStrategy {
  FAIL_STREAM,
  DROP_EVENT,
  EMIT_WARNING
}
