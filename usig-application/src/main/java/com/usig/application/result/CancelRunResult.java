package com.usig.application.result;

import com.usig.application.port.DownstreamExecutionState;
import com.usig.domain.run.Run;

public record CancelRunResult(Run run, DownstreamExecutionState downstreamState) {}
