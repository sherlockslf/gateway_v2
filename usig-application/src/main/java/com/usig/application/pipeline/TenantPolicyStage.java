package com.usig.application.pipeline;

import com.usig.domain.event.EventType;
import com.usig.domain.event.GatewayEvent;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;

public final class TenantPolicyStage implements EventStage {

  private static final String NAME = "tenant-policy";

  private final Map<String, TenantEventVisibilityPolicy> tenantPolicies;
  private final TenantEventVisibilityPolicy defaultPolicy;
  private final StagePolicy policy;

  public TenantPolicyStage(
      Map<String, TenantEventVisibilityPolicy> tenantPolicies,
      TenantEventVisibilityPolicy defaultPolicy,
      StagePolicy policy) {
    this.tenantPolicies = Map.copyOf(Objects.requireNonNullElse(tenantPolicies, Map.of()));
    this.defaultPolicy =
        Objects.requireNonNullElse(defaultPolicy, TenantEventVisibilityPolicy.allowAll());
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
    this.tenantPolicies.forEach(TenantPolicyStage::validateTenantPolicy);
  }

  public static TenantPolicyStage failStream(
      Map<String, TenantEventVisibilityPolicy> tenantPolicies,
      TenantEventVisibilityPolicy defaultPolicy,
      Duration maxExecutionTime) {
    return new TenantPolicyStage(
        tenantPolicies, defaultPolicy, StagePolicy.failStream(maxExecutionTime));
  }

  public Map<String, TenantEventVisibilityPolicy> tenantPolicies() {
    return tenantPolicies;
  }

  public TenantEventVisibilityPolicy defaultPolicy() {
    return defaultPolicy;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public StageCharacteristics characteristics() {
    return StageCharacteristics.synchronousNonBlockingOrdered();
  }

  @Override
  public StagePolicy policy() {
    return policy;
  }

  @Override
  public Flow.Publisher<GatewayEvent> apply(
      Flow.Publisher<GatewayEvent> upstream, PipelineContext context) {
    Objects.requireNonNull(upstream, "upstream must not be null");
    Objects.requireNonNull(context, "context must not be null");
    TenantEventVisibilityPolicy tenantPolicy =
        tenantPolicies.getOrDefault(context.tenantId().value(), defaultPolicy);
    return subscriber -> upstream.subscribe(new PolicySubscriber(subscriber, tenantPolicy));
  }

  private static void validateTenantPolicy(
      String tenantId, TenantEventVisibilityPolicy tenantPolicy) {
    if (tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("tenantPolicies must not contain blank tenantId");
    }
    Objects.requireNonNull(tenantPolicy, "tenantPolicy must not be null");
  }

  private static void validateVisibility(
      GatewayEvent event, TenantEventVisibilityPolicy tenantPolicy) {
    Objects.requireNonNull(event, "event must not be null");
    if (!tenantPolicy.reasoningVisible() && reasoningEvent(event)) {
      throw new IllegalArgumentException("Tenant policy hides reasoning events");
    }
    if (!tenantPolicy.toolVisible() && toolEvent(event)) {
      throw new IllegalArgumentException("Tenant policy hides tool events");
    }
    if (!tenantPolicy.artifactVisible() && artifactEvent(event)) {
      throw new IllegalArgumentException("Tenant policy hides artifact events");
    }
  }

  private static boolean reasoningEvent(GatewayEvent event) {
    return event.eventType() == EventType.EXTENSION
        && (reasoningMarker(event.payload().get("extensionType"))
            || reasoningMarker(event.payload().get("contentType"))
            || reasoningMarker(event.payload().get("downstreamEventType")));
  }

  private static boolean reasoningMarker(Object value) {
    if (!(value instanceof String marker)) {
      return false;
    }
    return marker.startsWith("reasoning.") || marker.startsWith("response.reasoning");
  }

  private static boolean toolEvent(GatewayEvent event) {
    return switch (event.eventType()) {
      case TOOL_START, TOOL_ARGUMENTS_DELTA, TOOL_RESULT, TOOL_ERROR, TOOL_END -> true;
      default -> false;
    };
  }

  private static boolean artifactEvent(GatewayEvent event) {
    return switch (event.eventType()) {
      case ARTIFACT_CREATED, ARTIFACT_UPDATED, ARTIFACT_COMPLETED -> true;
      default -> false;
    };
  }

  private static final class PolicySubscriber implements Flow.Subscriber<GatewayEvent> {

    private final Flow.Subscriber<? super GatewayEvent> downstream;
    private final TenantEventVisibilityPolicy tenantPolicy;
    private Flow.Subscription upstream;
    private boolean terminated;

    private PolicySubscriber(
        Flow.Subscriber<? super GatewayEvent> downstream,
        TenantEventVisibilityPolicy tenantPolicy) {
      this.downstream = Objects.requireNonNull(downstream, "downstream must not be null");
      this.tenantPolicy = tenantPolicy;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      upstream = Objects.requireNonNull(subscription, "subscription must not be null");
      downstream.onSubscribe(subscription);
    }

    @Override
    public void onNext(GatewayEvent item) {
      if (terminated) {
        return;
      }
      try {
        validateVisibility(item, tenantPolicy);
        downstream.onNext(item);
      } catch (RuntimeException exception) {
        terminated = true;
        upstream.cancel();
        downstream.onError(exception);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      if (!terminated) {
        terminated = true;
        downstream.onError(throwable);
      }
    }

    @Override
    public void onComplete() {
      if (!terminated) {
        terminated = true;
        downstream.onComplete();
      }
    }
  }
}
