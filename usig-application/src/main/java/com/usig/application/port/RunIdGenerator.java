package com.usig.application.port;

import com.usig.domain.run.RunId;

public interface RunIdGenerator {

  RunId nextRunId();
}
