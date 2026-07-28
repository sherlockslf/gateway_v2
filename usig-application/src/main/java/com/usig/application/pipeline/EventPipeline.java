package com.usig.application.pipeline;

import com.usig.domain.event.GatewayEvent;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class EventPipeline {

  private final List<EventStage> stages;

  public EventPipeline(List<EventStage> stages) {
    this.stages =
        List.copyOf(Objects.requireNonNullElse(stages, List.of())).stream()
            .map(EventPipeline::requireValidStage)
            .toList();
  }

  public List<EventStage> stages() {
    return stages;
  }

  public Flow.Publisher<GatewayEvent> apply(
      Flow.Publisher<GatewayEvent> source, PipelineContext context) {
    Flow.Publisher<GatewayEvent> current =
        Objects.requireNonNull(source, "source must not be null");
    PipelineContext pipelineContext = Objects.requireNonNull(context, "context must not be null");
    for (EventStage stage : stages) {
      current = stage.apply(current, pipelineContext);
      if (current == null) {
        throw new IllegalStateException("stage returned null publisher: " + stage.name());
      }
    }
    return current;
  }

  private static EventStage requireValidStage(EventStage stage) {
    Objects.requireNonNull(stage, "stage must not be null");
    if (stage.name() == null || stage.name().isBlank()) {
      throw new IllegalArgumentException("stage name must not be blank");
    }
    Objects.requireNonNull(stage.characteristics(), "stage characteristics must not be null");
    Objects.requireNonNull(stage.policy(), "stage policy must not be null");
    return stage;
  }
}
