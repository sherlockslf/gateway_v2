package com.usig.api.dto;

import com.usig.application.result.CancelRunResult;

public record CancelRunResponse(String runId, String status, String downstreamState) {

  public static CancelRunResponse from(CancelRunResult result) {
    return new CancelRunResponse(
        result.run().id().value(), result.run().status().name(), result.downstreamState().name());
  }
}
