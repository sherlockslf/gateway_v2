package com.usig.application.port;

import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.RunId;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public interface ReplayStore {

  CompletionStage<Void> append(RunId runId, GatewayEvent event, Duration ttl);

  Flow.Publisher<GatewayEvent> readAfter(RunId runId, String lastEventId);

  CompletionStage<Void> delete(RunId runId);
}
