package com.usig.application.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.usig.application.memory.ListGatewayEventPublisher;
import com.usig.application.port.DeliveryMetric;
import com.usig.application.port.DeliveryMetricsRecorder;
import com.usig.application.port.ReplayStore;
import com.usig.application.sideevent.InMemorySideEventPublisher;
import com.usig.application.sideevent.SideEvent;
import com.usig.application.sideevent.SideEventDispatchPolicy;
import com.usig.application.sideevent.SideEventDispatcher;
import com.usig.domain.event.EventMetadata;
import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import com.usig.domain.run.RunId;
import com.usig.domain.run.TenantId;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class EventPipelineTest {

  private static final Instant NOW = Instant.parse("2026-07-28T04:18:00Z");
  private static final Clock CLOCK = Clock.fixed(NOW.plusMillis(250), ZoneOffset.UTC);
  private static final PipelineContext CONTEXT =
      new PipelineContext(new RunId("run_1"), new TenantId("tenant_1"), "sample-agent", Map.of());
  private static final PipelineContext SSE_CONTEXT =
      new PipelineContext(
          new RunId("run_1"), new TenantId("tenant_1"), "sample-agent", Map.of("transport", "sse"));

  @Test
  void appliesStagesInDeclaredOrder() {
    EventPipeline pipeline =
        new EventPipeline(
            List.of(
                mappingStage("first", content -> content + "-a"),
                mappingStage("second", content -> content + "-b")));

    List<GatewayEvent> events =
        collect(pipeline.apply(new ListGatewayEventPublisher(List.of(event("x"))), CONTEXT));

    assertEquals("x-a-b", events.getFirst().payload().get("content"));
    assertEquals(
        List.of("first", "second"), pipeline.stages().stream().map(EventStage::name).toList());
  }

  @Test
  void stageMustDeclareCharacteristicsAndPolicy() {
    EventStage stage = mappingStage("redaction", content -> "[redacted]");

    assertTrue(stage.characteristics().synchronous());
    assertFalse(stage.characteristics().possiblyBlocking());
    assertTrue(stage.characteristics().preservesOrder());
    assertFalse(stage.characteristics().discardable());
    assertFalse(stage.characteristics().accessesDatabase());
    assertEquals(StageErrorStrategy.FAIL_STREAM, stage.policy().errorStrategy());
    assertEquals(Duration.ofMillis(50), stage.policy().maxExecutionTime());
  }

  @Test
  void rejectsInvalidStagePolicyAndBlankStageName() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new StagePolicy(Duration.ZERO, StageErrorStrategy.FAIL_STREAM));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EventPipeline(List.of(mappingStage(" ", Function.identity()))));
  }

  @Test
  void schemaValidationStagePassesCurrentSchemaEvents() {
    SchemaValidationStage stage = SchemaValidationStage.failStream(Duration.ofMillis(25));

    List<GatewayEvent> events =
        collect(stage.apply(new ListGatewayEventPublisher(List.of(event("x"))), CONTEXT));

    assertEquals("schema-validation", stage.name());
    assertTrue(stage.characteristics().synchronous());
    assertFalse(stage.characteristics().possiblyBlocking());
    assertTrue(stage.characteristics().preservesOrder());
    assertFalse(stage.characteristics().discardable());
    assertFalse(stage.characteristics().accessesDatabase());
    assertEquals(StageErrorStrategy.FAIL_STREAM, stage.policy().errorStrategy());
    assertEquals(Duration.ofMillis(25), stage.policy().maxExecutionTime());
    assertEquals(List.of("evt_1"), events.stream().map(GatewayEvent::eventId).toList());
  }

  @Test
  void schemaValidationStageFailsStreamWhenContextDoesNotMatchEvent() {
    SchemaValidationStage stage = SchemaValidationStage.failStream(Duration.ofMillis(25));
    GatewayEvent mismatchedRun =
        new GatewayEvent(
            GatewayEvent.CURRENT_SCHEMA_VERSION,
            "evt_1",
            EventType.TEXT_DELTA,
            "conv_1",
            "run_other",
            null,
            "sample-agent",
            null,
            0,
            NOW,
            NOW,
            Map.of("content", "x"),
            new EventMetadata("tenant_1", "trace_1", Map.of()));

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                collect(
                    stage.apply(new ListGatewayEventPublisher(List.of(mismatchedRun)), CONTEXT)));

    assertTrue(exception.getCause() instanceof IllegalArgumentException);
    assertEquals(
        "GatewayEvent runId does not match pipeline context", exception.getCause().getMessage());
  }

  @Test
  void timestampNormalizationStageClampsFutureOccurredAtToObservedAt() {
    TimestampNormalizationStage stage =
        TimestampNormalizationStage.failStream(Duration.ofMillis(25));
    GatewayEvent futureOccurredAt =
        new GatewayEvent(
            GatewayEvent.CURRENT_SCHEMA_VERSION,
            "evt_1",
            EventType.TEXT_DELTA,
            "conv_1",
            "run_1",
            null,
            "sample-agent",
            null,
            0,
            NOW.plusSeconds(10),
            NOW,
            Map.of("content", "x"),
            new EventMetadata("tenant_1", "trace_1", Map.of()));

    List<GatewayEvent> events =
        collect(stage.apply(new ListGatewayEventPublisher(List.of(futureOccurredAt)), CONTEXT));

    assertEquals("timestamp-normalization", stage.name());
    assertTrue(stage.characteristics().synchronous());
    assertFalse(stage.characteristics().possiblyBlocking());
    assertTrue(stage.characteristics().preservesOrder());
    assertFalse(stage.characteristics().discardable());
    assertFalse(stage.characteristics().accessesDatabase());
    assertEquals(StageErrorStrategy.FAIL_STREAM, stage.policy().errorStrategy());
    assertEquals(Duration.ofMillis(25), stage.policy().maxExecutionTime());
    assertEquals(NOW, events.getFirst().occurredAt());
    assertEquals(NOW, events.getFirst().observedAt());
    assertEquals("x", events.getFirst().payload().get("content"));
  }

  @Test
  void timestampNormalizationStageKeepsValidOccurredAt() {
    TimestampNormalizationStage stage =
        TimestampNormalizationStage.failStream(Duration.ofMillis(25));
    GatewayEvent valid =
        new GatewayEvent(
            GatewayEvent.CURRENT_SCHEMA_VERSION,
            "evt_1",
            EventType.TEXT_DELTA,
            "conv_1",
            "run_1",
            null,
            "sample-agent",
            null,
            0,
            NOW.minusSeconds(10),
            NOW,
            Map.of("content", "x"),
            new EventMetadata("tenant_1", "trace_1", Map.of()));

    List<GatewayEvent> events =
        collect(stage.apply(new ListGatewayEventPublisher(List.of(valid)), CONTEXT));

    assertEquals(NOW.minusSeconds(10), events.getFirst().occurredAt());
    assertEquals(NOW, events.getFirst().observedAt());
  }

  @Test
  void sequenceStageAssignsMonotonicSequencePerPipelineApplication() {
    SequenceStage stage = SequenceStage.failStream(10, Duration.ofMillis(25));

    List<GatewayEvent> events =
        collect(
            stage.apply(
                new ListGatewayEventPublisher(
                    List.of(eventWithSequence("evt_1", 99), eventWithSequence("evt_2", 1))),
                CONTEXT));
    List<GatewayEvent> secondRun =
        collect(
            stage.apply(
                new ListGatewayEventPublisher(List.of(eventWithSequence("evt_3", 50))), CONTEXT));

    assertEquals("sequence", stage.name());
    assertTrue(stage.characteristics().synchronous());
    assertFalse(stage.characteristics().possiblyBlocking());
    assertTrue(stage.characteristics().preservesOrder());
    assertFalse(stage.characteristics().discardable());
    assertFalse(stage.characteristics().accessesDatabase());
    assertEquals(StageErrorStrategy.FAIL_STREAM, stage.policy().errorStrategy());
    assertEquals(Duration.ofMillis(25), stage.policy().maxExecutionTime());
    assertEquals(10, stage.initialSequence());
    assertEquals(List.of(10L, 11L), events.stream().map(GatewayEvent::sequence).toList());
    assertEquals(10, secondRun.getFirst().sequence());
    assertEquals(List.of("evt_1", "evt_2"), events.stream().map(GatewayEvent::eventId).toList());
  }

  @Test
  void sequenceStageRejectsNegativeInitialSequence() {
    assertThrows(
        IllegalArgumentException.class, () -> SequenceStage.failStream(-1, Duration.ofMillis(25)));
  }

  @Test
  void coalescingStageCombinesAdjacentTextDeltaForSameRunAndSource() {
    CoalescingStage stage = CoalescingStage.failStream(Duration.ofMillis(25));

    List<GatewayEvent> events =
        collect(
            stage.apply(
                new ListGatewayEventPublisher(
                    List.of(
                        textDelta("evt_1", 10, "hel", "sample-agent"),
                        textDelta("evt_2", 11, "lo", "sample-agent"),
                        textDelta("evt_3", 12, "!", "sample-agent"))),
                CONTEXT));

    assertEquals("coalescing", stage.name());
    assertTrue(stage.characteristics().synchronous());
    assertFalse(stage.characteristics().possiblyBlocking());
    assertTrue(stage.characteristics().preservesOrder());
    assertFalse(stage.characteristics().discardable());
    assertFalse(stage.characteristics().accessesDatabase());
    assertEquals(StageErrorStrategy.FAIL_STREAM, stage.policy().errorStrategy());
    assertEquals(Duration.ofMillis(25), stage.policy().maxExecutionTime());
    assertEquals(1, events.size());
    assertEquals("evt_3", events.getFirst().eventId());
    assertEquals(12, events.getFirst().sequence());
    assertEquals("hello!", events.getFirst().payload().get("content"));
  }

  @Test
  void coalescingStageFlushesBeforeNonTextOrDifferentSourceEvent() {
    CoalescingStage stage = CoalescingStage.failStream(Duration.ofMillis(25));
    GatewayEvent toolStart =
        new GatewayEvent(
            GatewayEvent.CURRENT_SCHEMA_VERSION,
            "evt_tool",
            EventType.TOOL_START,
            "conv_1",
            "run_1",
            null,
            "sample-agent",
            null,
            12,
            NOW,
            NOW,
            Map.of("toolCallId", "tool_1"),
            new EventMetadata("tenant_1", "trace_1", Map.of()));

    List<GatewayEvent> events =
        collect(
            stage.apply(
                new ListGatewayEventPublisher(
                    List.of(
                        textDelta("evt_1", 10, "a", "sample-agent"),
                        textDelta("evt_2", 11, "b", "other-agent"),
                        toolStart,
                        textDelta("evt_3", 13, "c", "sample-agent"))),
                CONTEXT));

    assertEquals(
        List.of("evt_1", "evt_2", "evt_tool", "evt_3"),
        events.stream().map(GatewayEvent::eventId).toList());
    assertEquals("a", events.get(0).payload().get("content"));
    assertEquals("b", events.get(1).payload().get("content"));
    assertFalse(events.get(2).payload().containsKey("content"));
    assertEquals("c", events.get(3).payload().get("content"));
  }

  @Test
  void rollingRedactionStageRedactsTextDeltaContentAfterCoalescing() {
    RollingRedactionStage stage =
        RollingRedactionStage.failStream(List.of("SECRET"), Duration.ofMillis(25));
    EventPipeline pipeline =
        new EventPipeline(List.of(CoalescingStage.failStream(Duration.ofMillis(25)), stage));

    List<GatewayEvent> events =
        collect(
            pipeline.apply(
                new ListGatewayEventPublisher(
                    List.of(
                        textDelta("evt_1", 10, "SEC", "sample-agent"),
                        textDelta("evt_2", 11, "RET", "sample-agent"))),
                CONTEXT));

    assertEquals("rolling-redaction", stage.name());
    assertTrue(stage.characteristics().synchronous());
    assertFalse(stage.characteristics().possiblyBlocking());
    assertTrue(stage.characteristics().preservesOrder());
    assertFalse(stage.characteristics().discardable());
    assertFalse(stage.characteristics().accessesDatabase());
    assertEquals(StageErrorStrategy.FAIL_STREAM, stage.policy().errorStrategy());
    assertEquals(Duration.ofMillis(25), stage.policy().maxExecutionTime());
    assertEquals(List.of("SECRET"), stage.redactionLiterals());
    assertEquals(1, events.size());
    assertEquals("evt_2", events.getFirst().eventId());
    assertEquals(11, events.getFirst().sequence());
    assertEquals("[REDACTED]", events.getFirst().payload().get("content"));
  }

  @Test
  void rollingRedactionStageLeavesNonTextAndMissingContentUntouched() {
    RollingRedactionStage stage =
        RollingRedactionStage.failStream(List.of("SECRET"), Duration.ofMillis(25));
    GatewayEvent toolStart =
        new GatewayEvent(
            GatewayEvent.CURRENT_SCHEMA_VERSION,
            "evt_tool",
            EventType.TOOL_START,
            "conv_1",
            "run_1",
            null,
            "sample-agent",
            null,
            12,
            NOW,
            NOW,
            Map.of("toolCallId", "tool_1"),
            new EventMetadata("tenant_1", "trace_1", Map.of()));
    GatewayEvent textWithoutContent =
        new GatewayEvent(
            GatewayEvent.CURRENT_SCHEMA_VERSION,
            "evt_text",
            EventType.TEXT_DELTA,
            "conv_1",
            "run_1",
            null,
            "sample-agent",
            null,
            13,
            NOW,
            NOW,
            Map.of("role", "assistant"),
            new EventMetadata("tenant_1", "trace_1", Map.of()));

    List<GatewayEvent> events =
        collect(
            stage.apply(
                new ListGatewayEventPublisher(List.of(toolStart, textWithoutContent)), CONTEXT));

    assertEquals(Map.of("toolCallId", "tool_1"), events.get(0).payload());
    assertEquals(Map.of("role", "assistant"), events.get(1).payload());
  }

  @Test
  void rollingRedactionStageIgnoresBlankLiterals() {
    RollingRedactionStage stage =
        RollingRedactionStage.failStream(List.of(""), Duration.ofMillis(25));

    List<GatewayEvent> events =
        collect(
            stage.apply(
                new ListGatewayEventPublisher(
                    List.of(textDelta("evt_1", 10, "safe", "sample-agent"))),
                CONTEXT));

    assertEquals("safe", events.getFirst().payload().get("content"));
  }

  @Test
  void tenantPolicyStageAllowsVisibleEventsByDefault() {
    TenantPolicyStage stage =
        TenantPolicyStage.failStream(
            Map.of(), TenantEventVisibilityPolicy.allowAll(), Duration.ofMillis(25));

    List<GatewayEvent> events =
        collect(stage.apply(new ListGatewayEventPublisher(List.of(event("x"))), CONTEXT));

    assertEquals("tenant-policy", stage.name());
    assertTrue(stage.characteristics().synchronous());
    assertFalse(stage.characteristics().possiblyBlocking());
    assertTrue(stage.characteristics().preservesOrder());
    assertFalse(stage.characteristics().discardable());
    assertFalse(stage.characteristics().accessesDatabase());
    assertEquals(StageErrorStrategy.FAIL_STREAM, stage.policy().errorStrategy());
    assertEquals(Duration.ofMillis(25), stage.policy().maxExecutionTime());
    assertEquals(TenantEventVisibilityPolicy.allowAll(), stage.defaultPolicy());
    assertEquals("x", events.getFirst().payload().get("content"));
  }

  @Test
  void tenantPolicyStageFailsReasoningEventsForTenant() {
    TenantPolicyStage stage =
        TenantPolicyStage.failStream(
            Map.of("tenant_1", TenantEventVisibilityPolicy.hideReasoning()),
            TenantEventVisibilityPolicy.allowAll(),
            Duration.ofMillis(25));
    GatewayEvent reasoning =
        gatewayEvent(
            "evt_reasoning",
            EventType.EXTENSION,
            10,
            Map.of("extensionType", "reasoning.delta", "content", "hidden"));

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () -> collect(stage.apply(new ListGatewayEventPublisher(List.of(reasoning)), CONTEXT)));

    assertTrue(exception.getCause() instanceof IllegalArgumentException);
    assertEquals("Tenant policy hides reasoning events", exception.getCause().getMessage());
  }

  @Test
  void tenantPolicyStageFailsToolAndArtifactEventsForTenant() {
    TenantPolicyStage stage =
        TenantPolicyStage.failStream(
            Map.of("tenant_1", new TenantEventVisibilityPolicy(true, false, false)),
            TenantEventVisibilityPolicy.allowAll(),
            Duration.ofMillis(25));
    GatewayEvent tool =
        gatewayEvent("evt_tool", EventType.TOOL_START, 10, Map.of("toolCallId", "tool_1"));
    GatewayEvent artifact =
        gatewayEvent(
            "evt_artifact", EventType.ARTIFACT_CREATED, 11, Map.of("artifactId", "artifact_1"));

    CompletionException toolException =
        assertThrows(
            CompletionException.class,
            () -> collect(stage.apply(new ListGatewayEventPublisher(List.of(tool)), CONTEXT)));
    CompletionException artifactException =
        assertThrows(
            CompletionException.class,
            () -> collect(stage.apply(new ListGatewayEventPublisher(List.of(artifact)), CONTEXT)));

    assertEquals("Tenant policy hides tool events", toolException.getCause().getMessage());
    assertEquals("Tenant policy hides artifact events", artifactException.getCause().getMessage());
  }

  @Test
  void tenantPolicyStageRejectsBlankTenantPolicyKey() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            TenantPolicyStage.failStream(
                Map.of(" ", TenantEventVisibilityPolicy.allowAll()),
                TenantEventVisibilityPolicy.allowAll(),
                Duration.ofMillis(25)));
  }

  @Test
  void criticalEventGuardStageAllowsTerminalAndConnectionDiagnostics() {
    CriticalEventGuardStage stage = CriticalEventGuardStage.failStream(Duration.ofMillis(25));
    GatewayEvent completed = gatewayEvent("evt_done", EventType.EXECUTION_COMPLETED, 10, Map.of());
    GatewayEvent warning =
        gatewayEvent("evt_warning", EventType.WARNING, 11, Map.of("message", "late diagnostic"));
    GatewayEvent heartbeat = gatewayEvent("evt_heartbeat", EventType.HEARTBEAT, 12, Map.of());

    List<GatewayEvent> events =
        collect(
            stage.apply(
                new ListGatewayEventPublisher(List.of(completed, warning, heartbeat)), CONTEXT));

    assertEquals("critical-event-guard", stage.name());
    assertTrue(stage.characteristics().synchronous());
    assertFalse(stage.characteristics().possiblyBlocking());
    assertTrue(stage.characteristics().preservesOrder());
    assertFalse(stage.characteristics().discardable());
    assertFalse(stage.characteristics().accessesDatabase());
    assertEquals(StageErrorStrategy.FAIL_STREAM, stage.policy().errorStrategy());
    assertEquals(Duration.ofMillis(25), stage.policy().maxExecutionTime());
    assertEquals(List.of("evt_done", "evt_warning", "evt_heartbeat"), eventIds(events));
    assertTrue(stage.nonDiscardableEventTypes().containsAll(nonDiscardableEventTypes()));
  }

  @Test
  void criticalEventGuardStageFailsBusinessEventAfterTerminalEvent() {
    CriticalEventGuardStage stage = CriticalEventGuardStage.failStream(Duration.ofMillis(25));
    GatewayEvent completed = gatewayEvent("evt_done", EventType.EXECUTION_COMPLETED, 10, Map.of());
    GatewayEvent lateText = textDelta("evt_late_text", 11, "late", "sample-agent");

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                collect(
                    stage.apply(
                        new ListGatewayEventPublisher(List.of(completed, lateText)), CONTEXT)));

    assertTrue(exception.getCause() instanceof IllegalArgumentException);
    assertEquals(
        "GatewayEvent business event emitted after terminal event",
        exception.getCause().getMessage());
  }

  @Test
  void criticalEventGuardStageFailsDuplicateTerminalEvent() {
    CriticalEventGuardStage stage = CriticalEventGuardStage.failStream(Duration.ofMillis(25));
    GatewayEvent completed = gatewayEvent("evt_done", EventType.EXECUTION_COMPLETED, 10, Map.of());
    GatewayEvent failed = gatewayEvent("evt_failed", EventType.EXECUTION_FAILED, 11, Map.of());

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                collect(
                    stage.apply(
                        new ListGatewayEventPublisher(List.of(completed, failed)), CONTEXT)));

    assertEquals(
        "GatewayEvent business event emitted after terminal event",
        exception.getCause().getMessage());
  }

  @Test
  void replayStageAppendsEventsBeforeForwardingDownstream() {
    RecordingReplayStore replayStore = new RecordingReplayStore();
    ReplayStage stage =
        ReplayStage.failStream(replayStore, Duration.ofMinutes(10), Duration.ofMillis(25));

    List<GatewayEvent> events =
        collect(
            stage.apply(
                new ListGatewayEventPublisher(
                    List.of(eventWithSequence("evt_1", 10), eventWithSequence("evt_2", 11))),
                CONTEXT));

    assertEquals("replay", stage.name());
    assertTrue(stage.characteristics().synchronous());
    assertTrue(stage.characteristics().possiblyBlocking());
    assertTrue(stage.characteristics().preservesOrder());
    assertFalse(stage.characteristics().discardable());
    assertTrue(stage.characteristics().accessesDatabase());
    assertEquals(StageErrorStrategy.FAIL_STREAM, stage.policy().errorStrategy());
    assertEquals(Duration.ofMillis(25), stage.policy().maxExecutionTime());
    assertEquals(Duration.ofMinutes(10), stage.ttl());
    assertEquals(List.of("append:evt_1", "append:evt_2"), replayStore.operations);
    assertEquals(List.of("evt_1", "evt_2"), eventIds(events));
    assertEquals(List.of(new RunId("run_1"), new RunId("run_1")), replayStore.runIds);
    assertEquals(List.of(Duration.ofMinutes(10), Duration.ofMinutes(10)), replayStore.ttls);
  }

  @Test
  void replayStageFailsStreamWhenAppendFails() {
    RecordingReplayStore replayStore = new RecordingReplayStore();
    replayStore.failAppend = true;
    ReplayStage stage =
        ReplayStage.failStream(replayStore, Duration.ofMinutes(10), Duration.ofMillis(25));

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                collect(
                    stage.apply(
                        new ListGatewayEventPublisher(List.of(eventWithSequence("evt_1", 10))),
                        CONTEXT)));

    assertEquals("replay unavailable", exception.getCause().getMessage());
    assertEquals(List.of("append:evt_1"), replayStore.operations);
  }

  @Test
  void replayStageRejectsInvalidTtl() {
    RecordingReplayStore replayStore = new RecordingReplayStore();

    assertThrows(
        IllegalArgumentException.class,
        () -> ReplayStage.failStream(replayStore, Duration.ZERO, Duration.ofMillis(25)));
  }

  @Test
  void sideEffectTapStageDispatchesSideEventsAndForwardsMainEvents() {
    InMemorySideEventPublisher publisher = new InMemorySideEventPublisher(10);
    SideEffectTapStage stage =
        SideEffectTapStage.bestEffort(
            new SideEventDispatcher(publisher, SideEventDispatchPolicy.bestEffort()),
            "usig.message.persist.v1",
            Duration.ofMillis(25));

    List<GatewayEvent> events =
        collect(
            stage.apply(
                new ListGatewayEventPublisher(
                    List.of(eventWithSequence("evt_1", 10), eventWithSequence("evt_2", 11))),
                CONTEXT));

    assertEquals("side-effect-tap", stage.name());
    assertFalse(stage.characteristics().synchronous());
    assertFalse(stage.characteristics().possiblyBlocking());
    assertTrue(stage.characteristics().preservesOrder());
    assertTrue(stage.characteristics().discardable());
    assertFalse(stage.characteristics().accessesDatabase());
    assertEquals(StageErrorStrategy.EMIT_WARNING, stage.policy().errorStrategy());
    assertEquals(Duration.ofMillis(25), stage.policy().maxExecutionTime());
    assertEquals(List.of("evt_1", "evt_2"), eventIds(events));
    assertEquals(
        List.of("evt_1", "evt_2"), publisher.snapshot().stream().map(SideEvent::eventId).toList());
    SideEvent sideEvent = publisher.snapshot().getFirst();
    assertEquals("usig.message.persist.v1", sideEvent.topic());
    assertEquals("run_1", sideEvent.runId());
    assertEquals("tenant_1", sideEvent.tenantId());
    assertEquals("text.delta", sideEvent.payload().get("eventType"));
    assertEquals("sample-agent", sideEvent.payload().get("sourceService"));
    assertEquals(10L, sideEvent.payload().get("sequence"));
    assertEquals(Map.of("content", "evt_1"), sideEvent.payload().get("payload"));
  }

  @Test
  void sideEffectTapStageDoesNotBlockMainStreamWhenDispatchFails() {
    SideEffectTapStage stage =
        SideEffectTapStage.bestEffort(
            new SideEventDispatcher(
                event -> CompletableFuture.failedFuture(new IllegalStateException("kafka down")),
                SideEventDispatchPolicy.bestEffort()),
            "usig.message.persist.v1",
            Duration.ofMillis(25));

    List<GatewayEvent> events =
        collect(
            stage.apply(
                new ListGatewayEventPublisher(List.of(eventWithSequence("evt_1", 10))), CONTEXT));

    assertEquals(List.of("evt_1"), eventIds(events));
  }

  @Test
  void sideEffectTapStageAllowsCustomFactoryToSkipEvents() {
    InMemorySideEventPublisher publisher = new InMemorySideEventPublisher(10);
    SideEffectTapStage stage =
        new SideEffectTapStage(
            new SideEventDispatcher(publisher, SideEventDispatchPolicy.bestEffort()),
            event ->
                event.eventType() == EventType.TEXT_DELTA
                    ? Optional.empty()
                    : Optional.of(SideEvent.fromGatewayEvent("usig.audit.v1", event, Map.of())),
            StagePolicy.failStream(Duration.ofMillis(25)));

    List<GatewayEvent> events =
        collect(
            stage.apply(
                new ListGatewayEventPublisher(
                    List.of(
                        textDelta("evt_text", 10, "x", "sample-agent"),
                        gatewayEvent(
                            "evt_tool", EventType.TOOL_START, 11, Map.of("toolCallId", "t1")))),
                CONTEXT));

    assertEquals(List.of("evt_text", "evt_tool"), eventIds(events));
    assertEquals(
        List.of("evt_tool"), publisher.snapshot().stream().map(SideEvent::eventId).toList());
  }

  @Test
  void clientProjectionStageRemovesHiddenPayloadKeysAndKeepsEventEnvelope() {
    ClientProjectionStage stage =
        ClientProjectionStage.hidePayloadKeys(Set.of("rawFrame", "debug"), Duration.ofMillis(25));
    GatewayEvent internal =
        gatewayEvent(
            "evt_1",
            EventType.TEXT_DELTA,
            10,
            Map.of("content", "hello", "rawFrame", "data: x", "debug", "trace"));

    List<GatewayEvent> events =
        collect(stage.apply(new ListGatewayEventPublisher(List.of(internal)), CONTEXT));

    assertEquals("client-projection", stage.name());
    assertTrue(stage.characteristics().synchronous());
    assertFalse(stage.characteristics().possiblyBlocking());
    assertTrue(stage.characteristics().preservesOrder());
    assertFalse(stage.characteristics().discardable());
    assertFalse(stage.characteristics().accessesDatabase());
    assertEquals(StageErrorStrategy.FAIL_STREAM, stage.policy().errorStrategy());
    assertEquals(Duration.ofMillis(25), stage.policy().maxExecutionTime());
    GatewayEvent projected = events.getFirst();
    assertEquals("evt_1", projected.eventId());
    assertEquals(EventType.TEXT_DELTA, projected.eventType());
    assertEquals("run_1", projected.runId());
    assertEquals(10, projected.sequence());
    assertEquals(new EventMetadata("tenant_1", "trace_1", Map.of()), projected.metadata());
    assertEquals(Map.of("content", "hello"), projected.payload());
  }

  @Test
  void clientProjectionStageCanDropEventsWithCustomProjection() {
    ClientProjectionStage stage =
        new ClientProjectionStage(
            event ->
                event.eventType() == EventType.HEARTBEAT ? Optional.empty() : Optional.of(event),
            StagePolicy.failStream(Duration.ofMillis(25)));

    List<GatewayEvent> events =
        collect(
            stage.apply(
                new ListGatewayEventPublisher(
                    List.of(
                        gatewayEvent("evt_heartbeat", EventType.HEARTBEAT, 10, Map.of()),
                        textDelta("evt_text", 11, "visible", "sample-agent"))),
                CONTEXT));

    assertEquals(List.of("evt_text"), eventIds(events));
  }

  @Test
  void clientProjectionStageFailsStreamWhenProjectionFails() {
    ClientProjectionStage stage =
        new ClientProjectionStage(
            event -> {
              throw new IllegalArgumentException("projection failed");
            },
            StagePolicy.failStream(Duration.ofMillis(25)));

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                collect(
                    stage.apply(
                        new ListGatewayEventPublisher(List.of(eventWithSequence("evt_1", 10))),
                        CONTEXT)));

    assertEquals("projection failed", exception.getCause().getMessage());
  }

  @Test
  void deliveryMetricsStageRecordsEventsOutAndLatencyWithLowCardinalityLabels() {
    RecordingDeliveryMetricsRecorder recorder = new RecordingDeliveryMetricsRecorder();
    DeliveryMetricsStage stage =
        DeliveryMetricsStage.bestEffort(recorder, CLOCK, Duration.ofMillis(25));

    List<GatewayEvent> events =
        collect(
            stage.apply(
                new ListGatewayEventPublisher(List.of(eventWithSequence("evt_1", 10))),
                SSE_CONTEXT));

    assertEquals("delivery-metrics", stage.name());
    assertTrue(stage.characteristics().synchronous());
    assertFalse(stage.characteristics().possiblyBlocking());
    assertTrue(stage.characteristics().preservesOrder());
    assertFalse(stage.characteristics().discardable());
    assertFalse(stage.characteristics().accessesDatabase());
    assertEquals(StageErrorStrategy.EMIT_WARNING, stage.policy().errorStrategy());
    assertEquals(Duration.ofMillis(25), stage.policy().maxExecutionTime());
    assertEquals(List.of("evt_1"), eventIds(events));
    assertEquals(
        new DeliveryMetric("sample-agent", "text.delta", "success", "sse"),
        recorder.eventOutMetrics.getFirst());
    assertEquals(
        new DeliveryMetric("sample-agent", "text.delta", "success", "sse"),
        recorder.latencyMetrics.getFirst());
    assertEquals(Duration.ofMillis(250), recorder.latencies.getFirst());
    assertFalse(recorder.eventOutMetrics.getFirst().attributes().containsKey("runId"));
    assertFalse(recorder.eventOutMetrics.getFirst().attributes().containsKey("tenantId"));
    assertFalse(recorder.eventOutMetrics.getFirst().attributes().containsKey("traceId"));
    assertFalse(recorder.eventOutMetrics.getFirst().attributes().containsKey("payload"));
  }

  @Test
  void deliveryMetricsStageUsesUnknownTransportWhenContextDoesNotDeclareOne() {
    RecordingDeliveryMetricsRecorder recorder = new RecordingDeliveryMetricsRecorder();
    DeliveryMetricsStage stage =
        DeliveryMetricsStage.bestEffort(recorder, CLOCK, Duration.ofMillis(25));

    collect(
        stage.apply(
            new ListGatewayEventPublisher(List.of(eventWithSequence("evt_1", 10))), CONTEXT));

    assertEquals("unknown", recorder.eventOutMetrics.getFirst().transport());
  }

  @Test
  void deliveryMetricsStageDoesNotBlockMainStreamWhenBestEffortRecorderFails() {
    RecordingDeliveryMetricsRecorder recorder = new RecordingDeliveryMetricsRecorder();
    recorder.failEventOut = true;
    DeliveryMetricsStage stage =
        DeliveryMetricsStage.bestEffort(recorder, CLOCK, Duration.ofMillis(25));

    List<GatewayEvent> events =
        collect(
            stage.apply(
                new ListGatewayEventPublisher(List.of(eventWithSequence("evt_1", 10))),
                SSE_CONTEXT));

    assertEquals(List.of("evt_1"), eventIds(events));
  }

  @Test
  void deliveryMetricsStageCanFailStreamWhenConfigured() {
    RecordingDeliveryMetricsRecorder recorder = new RecordingDeliveryMetricsRecorder();
    recorder.failEventOut = true;
    DeliveryMetricsStage stage =
        new DeliveryMetricsStage(recorder, CLOCK, StagePolicy.failStream(Duration.ofMillis(25)));

    CompletionException exception =
        assertThrows(
            CompletionException.class,
            () ->
                collect(
                    stage.apply(
                        new ListGatewayEventPublisher(List.of(eventWithSequence("evt_1", 10))),
                        SSE_CONTEXT)));

    assertEquals("metrics unavailable", exception.getCause().getMessage());
  }

  private static EventStage mappingStage(String name, Function<String, String> mapper) {
    return new EventStage() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public StageCharacteristics characteristics() {
        return StageCharacteristics.synchronousNonBlockingOrdered();
      }

      @Override
      public StagePolicy policy() {
        return StagePolicy.failStream(Duration.ofMillis(50));
      }

      @Override
      public Flow.Publisher<GatewayEvent> apply(
          Flow.Publisher<GatewayEvent> upstream, PipelineContext context) {
        return subscriber ->
            upstream.subscribe(
                new Flow.Subscriber<>() {
                  @Override
                  public void onSubscribe(Flow.Subscription subscription) {
                    subscriber.onSubscribe(subscription);
                  }

                  @Override
                  public void onNext(GatewayEvent item) {
                    subscriber.onNext(copyWithContent(item, mapper.apply(content(item))));
                  }

                  @Override
                  public void onError(Throwable throwable) {
                    subscriber.onError(throwable);
                  }

                  @Override
                  public void onComplete() {
                    subscriber.onComplete();
                  }
                });
      }
    };
  }

  private static GatewayEvent copyWithContent(GatewayEvent event, String content) {
    return new GatewayEvent(
        event.schemaVersion(),
        event.eventId(),
        event.eventType(),
        event.conversationId(),
        event.runId(),
        event.taskId(),
        event.sourceService(),
        event.sourceEventId(),
        event.sequence(),
        event.occurredAt(),
        event.observedAt(),
        Map.of("content", content),
        event.metadata());
  }

  private static String content(GatewayEvent event) {
    return event.payload().get("content").toString();
  }

  private static GatewayEvent event(String content) {
    return GatewayEvent.of(
        "evt_1",
        EventType.TEXT_DELTA,
        "conv_1",
        "run_1",
        "sample-agent",
        0,
        NOW,
        Map.of("content", content),
        new EventMetadata("tenant_1", "trace_1", Map.of()));
  }

  private static GatewayEvent eventWithSequence(String eventId, long sequence) {
    return GatewayEvent.of(
        eventId,
        EventType.TEXT_DELTA,
        "conv_1",
        "run_1",
        "sample-agent",
        sequence,
        NOW,
        Map.of("content", eventId),
        new EventMetadata("tenant_1", "trace_1", Map.of()));
  }

  private static GatewayEvent textDelta(
      String eventId, long sequence, String content, String sourceService) {
    return GatewayEvent.of(
        eventId,
        EventType.TEXT_DELTA,
        "conv_1",
        "run_1",
        sourceService,
        sequence,
        NOW,
        Map.of("content", content),
        new EventMetadata("tenant_1", "trace_1", Map.of()));
  }

  private static GatewayEvent gatewayEvent(
      String eventId, EventType eventType, long sequence, Map<String, Object> payload) {
    return GatewayEvent.of(
        eventId,
        eventType,
        "conv_1",
        "run_1",
        "sample-agent",
        sequence,
        NOW,
        payload,
        new EventMetadata("tenant_1", "trace_1", Map.of()));
  }

  private static List<String> eventIds(List<GatewayEvent> events) {
    return events.stream().map(GatewayEvent::eventId).toList();
  }

  private static Set<EventType> nonDiscardableEventTypes() {
    return Set.of(
        EventType.EXECUTION_STARTED,
        EventType.TEXT_END,
        EventType.TOOL_RESULT,
        EventType.ARTIFACT_COMPLETED,
        EventType.INPUT_REQUIRED,
        EventType.APPROVAL_REQUIRED,
        EventType.EXECUTION_FAILED,
        EventType.EXECUTION_COMPLETED,
        EventType.EXECUTION_CANCELED);
  }

  private static List<GatewayEvent> collect(Flow.Publisher<GatewayEvent> publisher) {
    CollectingSubscriber subscriber = new CollectingSubscriber();
    publisher.subscribe(subscriber);
    return subscriber.result.join();
  }

  private static final class CollectingSubscriber implements Flow.Subscriber<GatewayEvent> {

    private final List<GatewayEvent> events = new ArrayList<>();
    private final CompletableFuture<List<GatewayEvent>> result = new CompletableFuture<>();

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(GatewayEvent item) {
      events.add(item);
    }

    @Override
    public void onError(Throwable throwable) {
      result.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
      result.complete(List.copyOf(events));
    }
  }

  private static final class RecordingDeliveryMetricsRecorder implements DeliveryMetricsRecorder {

    private final List<DeliveryMetric> eventOutMetrics = new ArrayList<>();
    private final List<DeliveryMetric> latencyMetrics = new ArrayList<>();
    private final List<Duration> latencies = new ArrayList<>();
    private boolean failEventOut;

    @Override
    public void eventOut(DeliveryMetric metric) {
      eventOutMetrics.add(metric);
      if (failEventOut) {
        throw new IllegalStateException("metrics unavailable");
      }
    }

    @Override
    public void streamAddedLatency(DeliveryMetric metric, Duration latency) {
      latencyMetrics.add(metric);
      latencies.add(latency);
    }
  }

  private static final class RecordingReplayStore implements ReplayStore {

    private final List<String> operations = new ArrayList<>();
    private final List<RunId> runIds = new ArrayList<>();
    private final List<Duration> ttls = new ArrayList<>();
    private boolean failAppend;

    @Override
    public CompletionStage<Void> append(RunId runId, GatewayEvent event, Duration ttl) {
      operations.add("append:" + event.eventId());
      runIds.add(runId);
      ttls.add(ttl);
      if (failAppend) {
        return CompletableFuture.failedFuture(new IllegalStateException("replay unavailable"));
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public Flow.Publisher<GatewayEvent> readAfter(RunId runId, String lastEventId) {
      return new ListGatewayEventPublisher(List.of());
    }

    @Override
    public CompletionStage<Void> delete(RunId runId) {
      return CompletableFuture.completedFuture(null);
    }
  }
}
