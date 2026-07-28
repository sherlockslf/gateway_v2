package com.usig.application.port;

import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.ExecutionHandle;
import java.util.concurrent.Flow;

public record AdapterExecution(ExecutionHandle handle, Flow.Publisher<GatewayEvent> events) {

  public AdapterExecution {
    if (handle == null) {
      throw new IllegalArgumentException("handle must not be null");
    }
    if (events == null) {
      throw new IllegalArgumentException("events must not be null");
    }
  }
}
