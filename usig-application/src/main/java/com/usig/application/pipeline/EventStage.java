package com.usig.application.pipeline;

import com.usig.domain.event.GatewayEvent;
import java.util.concurrent.Flow;

public interface EventStage {

  String name();

  StageCharacteristics characteristics();

  StagePolicy policy();

  Flow.Publisher<GatewayEvent> apply(
      Flow.Publisher<GatewayEvent> upstream, PipelineContext context);
}
