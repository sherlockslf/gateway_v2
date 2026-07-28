package com.usig.bootstrap;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(ServiceRegistryReloadResourceTest.ReloadEnabledProfile.class)
class ServiceRegistryReloadResourceTest {

  @Test
  void rejectsReloadWithoutAdminToken() {
    given()
        .urlEncodingEnabled(false)
        .contentType("application/json")
        .body(
            """
            {"services":[]}
            """)
        .when()
        .post("/v1/services:reload")
        .then()
        .statusCode(401)
        .body("code", equalTo("UNAUTHORIZED"));
  }

  @Test
  void reloadsServiceRegistrySnapshotThroughAdminApi() {
    given()
        .urlEncodingEnabled(false)
        .contentType("application/json")
        .header("X-USIG-Admin-Token", "reload-secret")
        .body(
            """
            {
              "services": [
                {
                  "serviceId": "reloaded-agent",
                  "serviceType": "AGENT",
                  "endpoint": "https://internal.example/reloaded-agent",
                  "adapterId": "sample-agent-v1",
                  "visibleTenantIds": ["tenant_reload"],
                  "transports": ["SSE"],
                  "capabilities": {
                    "streaming": true,
                    "cancel": true,
                    "resume": false,
                    "replay": false,
                    "taskQuery": true,
                    "humanInput": false,
                    "multimodalInput": false,
                    "structuredOutput": true,
                    "retrySafety": "NONE"
                  },
                  "version": 3
                }
              ]
            }
            """)
        .when()
        .post("/v1/services:reload")
        .then()
        .statusCode(200)
        .body("serviceCount", equalTo(1))
        .body("services[0].serviceId", equalTo("reloaded-agent"))
        .body("services[0].version", equalTo(3))
        .body("services[0].capabilities.retrySafety", equalTo("NONE"))
        .body("services[0].endpoint", nullValue());

    given()
        .header("X-Tenant-Id", "tenant_reload")
        .when()
        .get("/v1/services")
        .then()
        .statusCode(200)
        .body("size()", equalTo(1))
        .body("[0].serviceId", equalTo("reloaded-agent"));

    given()
        .header("X-Tenant-Id", "tenant_1")
        .when()
        .get("/v1/services/sample-agent/capabilities")
        .then()
        .statusCode(404)
        .body("code", equalTo("SERVICE_NOT_FOUND"));
  }

  @Test
  void rejectsReloadWhenAdapterIdIsUnknown() {
    given()
        .urlEncodingEnabled(false)
        .contentType("application/json")
        .header("X-USIG-Admin-Token", "reload-secret")
        .body(
            """
            {
              "services": [
                {
                  "serviceId": "bad-agent",
                  "serviceType": "AGENT",
                  "endpoint": "https://internal.example/bad-agent",
                  "adapterId": "missing-adapter",
                  "visibleTenantIds": ["tenant_reload"],
                  "transports": ["SSE"],
                  "version": 1
                }
              ]
            }
            """)
        .when()
        .post("/v1/services:reload")
        .then()
        .statusCode(400)
        .body("code", equalTo("ADAPTER_NOT_FOUND"));
  }

  public static final class ReloadEnabledProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "usig.service-registry.reload.enabled", "true",
          "usig.service-registry.reload.admin-token", "reload-secret");
    }
  }
}
