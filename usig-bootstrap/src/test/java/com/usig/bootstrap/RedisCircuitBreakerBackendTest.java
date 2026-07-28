package com.usig.bootstrap;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.usig.application.port.RunCircuitBreaker;
import com.usig.infra.redis.RedisRunCircuitBreaker;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(RedisCircuitBreakerBackendTest.RedisCircuitBreakerProfile.class)
class RedisCircuitBreakerBackendTest {

  @Inject RunCircuitBreaker circuitBreaker;

  @Test
  void canSelectRedisCircuitBreakerBackend() {
    assertInstanceOf(RedisRunCircuitBreaker.class, circuitBreaker);
  }

  public static final class RedisCircuitBreakerProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "usig.circuit-breaker.backend", "redis",
          "usig.redis.key-prefix", "usig-test");
    }
  }
}
