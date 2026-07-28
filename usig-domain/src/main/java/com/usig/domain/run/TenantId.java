package com.usig.domain.run;

public record TenantId(String value) {

  public TenantId {
    value = requireText(value, "tenantId");
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
