package com.usig.bootstrap;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.usig.application.port.ServiceRegistry;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(ServiceRegistryPostgresReloadWorkerResourceTest.PostgresReloadSourceProfile.class)
class ServiceRegistryPostgresReloadWorkerResourceTest {

  @Inject ServiceRegistry serviceRegistry;
  @Inject ServiceRegistryReloadWorkerConfig reloadWorkerConfig;
  @Inject ServiceRegistryReloadWorker reloadWorker;

  @Test
  void reloadsMemoryRegistryFromPostgresSnapshotSourceOnDemand() {
    assertFalse(reloadWorkerConfig.enabled());
    assertEquals(Duration.ofSeconds(19), reloadWorkerConfig.interval());
    assertFalse(reloadWorker.isStarted());
    assertEquals("sample-agent", serviceRegistry.find("sample-agent").orElseThrow().serviceId());

    assertEquals(1, reloadWorker.reloadNow().toCompletableFuture().join().serviceCount());

    assertEquals(
        "postgres-agent", serviceRegistry.find("postgres-agent").orElseThrow().serviceId());
    assertEquals(java.util.Optional.empty(), serviceRegistry.find("sample-agent"));
    given()
        .header("X-Tenant-Id", "tenant_pg")
        .when()
        .get("/v1/services")
        .then()
        .statusCode(200)
        .body("size()", equalTo(1))
        .body("[0].serviceId", equalTo("postgres-agent"));

    given()
        .urlEncodingEnabled(false)
        .header("X-USIG-Admin-Token", "reload-secret")
        .when()
        .get("/v1/services:reload/status")
        .then()
        .statusCode(200)
        .body("reloading", equalTo(false))
        .body("lastAttemptedReloadAt", notNullValue())
        .body("successfulReloadCount", equalTo(1))
        .body("failedReloadCount", equalTo(0))
        .body("lastSuccessfulReloadAt", notNullValue())
        .body("lastSuccessfulServiceCount", equalTo(1))
        .body("lastFailureMessage", equalTo(null));
  }

  public static final class PostgresReloadSourceProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "usig.service-registry.backend", "memory",
          "usig.service-registry.reload.enabled", "true",
          "usig.service-registry.reload.admin-token", "reload-secret",
          "usig.service-registry.reload.source", "postgres",
          "usig.service-registry.reload.worker.enabled", "false",
          "usig.service-registry.reload.worker.interval", "PT19S");
    }
  }
}
