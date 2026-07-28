package com.usig.application.port;

public enum DownstreamExecutionState {
  STARTED,
  RUNNING,
  WAITING_INPUT,
  COMPLETED,
  FAILED,
  CANCEL_REQUESTED,
  CANCELED,
  CANCEL_NOT_SUPPORTED,
  COMPLETED_BEFORE_CANCEL,
  UNKNOWN
}
