package com.usig.bootstrap;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.usig.application.port.RunConcurrencyLimiter;
import com.usig.infra.redis.RedisRunConcurrencyLimiter;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(RedisConcurrencyLimiterBackendTest.RedisConcurrencyProfile.class)
class RedisConcurrencyLimiterBackendTest {

  @Inject RunConcurrencyLimiter limiter;

  @Test
  void canSelectRedisConcurrencyLimiterBackend() {
    assertInstanceOf(RedisRunConcurrencyLimiter.class, limiter);
  }

  public static final class RedisConcurrencyProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "usig.concurrency.backend", "redis",
          "usig.redis.key-prefix", "usig-test");
    }
  }
}
