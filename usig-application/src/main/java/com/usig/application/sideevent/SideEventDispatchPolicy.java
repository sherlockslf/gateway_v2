package com.usig.application.sideevent;

import java.util.Objects;
import java.util.Set;

public final class SideEventDispatchPolicy {

  private static final String AUDIT_TOPIC = "usig.audit.v1";

  private final Set<String> failFastAuditTenantIds;

  private SideEventDispatchPolicy(Set<String> failFastAuditTenantIds) {
    this.failFastAuditTenantIds = Set.copyOf(failFastAuditTenantIds);
  }

  public static SideEventDispatchPolicy bestEffort() {
    return new SideEventDispatchPolicy(Set.of());
  }

  public static SideEventDispatchPolicy failFastAuditForTenants(Set<String> tenantIds) {
    Objects.requireNonNull(tenantIds, "tenantIds must not be null");
    return new SideEventDispatchPolicy(tenantIds);
  }

  public boolean shouldFailFast(SideEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    return AUDIT_TOPIC.equals(event.topic()) && failFastAuditTenantIds.contains(event.tenantId());
  }
}
