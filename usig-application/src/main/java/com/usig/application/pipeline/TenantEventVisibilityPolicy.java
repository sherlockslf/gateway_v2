package com.usig.application.pipeline;

public record TenantEventVisibilityPolicy(
    boolean reasoningVisible, boolean toolVisible, boolean artifactVisible) {

  public static TenantEventVisibilityPolicy allowAll() {
    return new TenantEventVisibilityPolicy(true, true, true);
  }

  public static TenantEventVisibilityPolicy hideReasoning() {
    return new TenantEventVisibilityPolicy(false, true, true);
  }

  public static TenantEventVisibilityPolicy hideTool() {
    return new TenantEventVisibilityPolicy(true, false, true);
  }

  public static TenantEventVisibilityPolicy hideArtifact() {
    return new TenantEventVisibilityPolicy(true, true, false);
  }
}
