package com.usig.bootstrap;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ServiceRegistryReloadDisabledResourceTest {

  @Test
  void rejectsReloadByDefault() {
    given()
        .urlEncodingEnabled(false)
        .contentType("application/json")
        .header("X-USIG-Admin-Token", "any-token")
        .body(
            """
            {"services":[]}
            """)
        .when()
        .post("/v1/services:reload")
        .then()
        .statusCode(403)
        .body("code", equalTo("FORBIDDEN"));
  }

  @Test
  void rejectsReloadStatusByDefault() {
    given()
        .urlEncodingEnabled(false)
        .header("X-USIG-Admin-Token", "any-token")
        .when()
        .get("/v1/services:reload/status")
        .then()
        .statusCode(403)
        .body("code", equalTo("FORBIDDEN"));
  }
}
