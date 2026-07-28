package com.usig.domain.service;

public enum RetrySafety {
  NONE,
  IDEMPOTENT_CREATE_ONLY,
  RESUME_SAFE
}
