package com.usig.bootstrap;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.usig.application.port.ServiceRegistry;
import com.usig.infra.postgres.PostgresServiceRegistry;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PostgresServiceRegistryBackendTest.PostgresServiceRegistryProfile.class)
class PostgresServiceRegistryBackendTest {

  @Inject ServiceRegistry serviceRegistry;

  @Test
  void canSelectPostgresServiceRegistryBackend() {
    assertInstanceOf(PostgresServiceRegistry.class, serviceRegistry);
    assertEquals(
        "postgres-agent", serviceRegistry.find("postgres-agent").orElseThrow().serviceId());
  }

  @Test
  void serviceApiUsesPostgresServiceRegistryBackend() {
    given()
        .header("X-Tenant-Id", "tenant_pg")
        .when()
        .get("/v1/services")
        .then()
        .statusCode(200)
        .body("size()", equalTo(1))
        .body("[0].serviceId", equalTo("postgres-agent"))
        .body("[0].capabilities.retrySafety", equalTo("NONE"));

    given()
        .header("X-Tenant-Id", "tenant_pg")
        .when()
        .get("/v1/services/configured-agent/capabilities")
        .then()
        .statusCode(404)
        .body("code", equalTo("SERVICE_NOT_FOUND"));
  }

  public static final class PostgresServiceRegistryProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("usig.service-registry.backend", "postgres");
    }
  }
}
