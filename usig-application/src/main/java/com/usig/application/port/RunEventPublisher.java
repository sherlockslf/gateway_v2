package com.usig.application.port;

import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.RunId;
import java.util.concurrent.Flow;

public interface RunEventPublisher {

  void attach(RunId runId, Flow.Publisher<GatewayEvent> events);

  Flow.Publisher<GatewayEvent> subscribe(RunId runId, String lastEventId);
}
