package com.usig.bootstrap;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import com.usig.application.port.RunRepository;
import com.usig.domain.run.ConversationId;
import com.usig.domain.run.Run;
import com.usig.domain.run.RunId;
import com.usig.domain.run.RunStatus;
import com.usig.domain.run.TenantId;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Instant;
import org.junit.jupiter.api.Test;

@QuarkusTest
class RunResourceTest {

  @Inject RunRepository runRepository;

  @Test
  void createsRunAndReturnsEventsUrl() {
    given()
        .contentType("application/json")
        .header("Idempotency-Key", "it-create-1")
        .body(
            """
            {
              "conversationId": "conv_1",
              "tenantId": "tenant_1",
              "serviceId": "sample-agent",
              "input": [{"type": "TEXT", "value": "hello"}]
            }
            """)
        .when()
        .post("/v1/runs")
        .then()
        .statusCode(202)
        .body("runId", notNullValue())
        .body("status", equalTo("STARTING"))
        .body("eventsUrl", containsString("/v1/runs/"));
  }

  @Test
  void streamsGatewayEventsAsSseWithIdAndEventName() {
    String runId =
        given()
            .contentType("application/json")
            .header("Idempotency-Key", "it-sse-1")
            .body(
                """
                {
                  "conversationId": "conv_2",
                  "tenantId": "tenant_1",
                  "serviceId": "sample-agent",
                  "input": [{"type": "TEXT", "value": "hello"}]
                }
                """)
            .when()
            .post("/v1/runs")
            .then()
            .statusCode(202)
            .extract()
            .path("runId");

    given()
        .accept("text/event-stream")
        .header("X-Tenant-Id", "tenant_1")
        .when()
        .get("/v1/runs/" + runId + "/events")
        .then()
        .statusCode(200)
        .contentType(containsString("text/event-stream"))
        .body(containsString("event:execution.started"))
        .body(containsString("id:evt_" + runId + "_001"))
        .body(containsString("\"eventType\":\"text.delta\""))
        .body(containsString("\"content\":\"sample response\""))
        .body(containsString("event:execution.completed"));
  }

  @Test
  void replaysEventsAfterLastEventId() {
    String runId =
        given()
            .contentType("application/json")
            .header("Idempotency-Key", "it-replay-1")
            .body(
                """
                {
                  "conversationId": "conv_3",
                  "tenantId": "tenant_1",
                  "serviceId": "sample-agent",
                  "input": [{"type": "TEXT", "value": "hello"}]
                }
                """)
            .when()
            .post("/v1/runs")
            .then()
            .statusCode(202)
            .extract()
            .path("runId");

    given()
        .accept("text/event-stream")
        .header("X-Tenant-Id", "tenant_1")
        .when()
        .get("/v1/runs/" + runId + "/events")
        .then()
        .statusCode(200)
        .body(containsString("event:execution.completed"));

    given()
        .accept("text/event-stream")
        .header("X-Tenant-Id", "tenant_1")
        .header("Last-Event-ID", "evt_" + runId + "_001")
        .when()
        .get("/v1/runs/" + runId + "/events")
        .then()
        .statusCode(200)
        .body(containsString("\"eventId\":\"evt_" + runId + "_002\""))
        .body(containsString("event:execution.completed"));
  }

  @Test
  void returnsServiceCapabilitiesForVisibleTenant() {
    given()
        .header("X-Tenant-Id", "tenant_1")
        .when()
        .get("/v1/services/sample-agent/capabilities")
        .then()
        .statusCode(200)
        .body("serviceId", equalTo("sample-agent"))
        .body("serviceType", equalTo("AGENT"))
        .body("adapterId", equalTo("sample-agent-v1"))
        .body("capabilities.streaming", equalTo(true))
        .body("capabilities.cancel", equalTo(true))
        .body("capabilities.taskQuery", equalTo(true))
        .body("capabilities.humanInput", equalTo(true))
        .body("capabilities.multimodalInput", equalTo(true))
        .body("capabilities.retrySafety", equalTo("RESUME_SAFE"));
  }

  @Test
  void listsVisibleServicesWithoutInternalEndpoint() {
    given()
        .header("X-Tenant-Id", "tenant_1")
        .when()
        .get("/v1/services")
        .then()
        .statusCode(200)
        .body("size()", equalTo(1))
        .body("[0].serviceId", equalTo("sample-agent"))
        .body("[0].serviceType", equalTo("AGENT"))
        .body("[0].adapterId", equalTo("sample-agent-v1"))
        .body("[0].capabilities.streaming", equalTo(true))
        .body("[0].capabilities.retrySafety", equalTo("RESUME_SAFE"))
        .body("[0].endpoint", equalTo(null));
  }

  @Test
  void returnsImageUploadCredentialForVisibleMultimodalService() {
    given()
        .contentType("application/json")
        .body(
            """
            {
              "tenantId": "tenant_1",
              "filename": "photo.png",
              "mimeType": "image/png",
              "contentLengthBytes": 1024
            }
            """)
        .when()
        .post("/v1/services/sample-agent/image-upload-credential")
        .then()
        .statusCode(200)
        .body("method", equalTo("PUT"))
        .body(
            "uploadUrl", containsString("https://upload.usig.local/uploads/tenant_1/sample-agent/"))
        .body("objectUrl", containsString("https://cdn.example.com/uploads/tenant_1/sample-agent/"))
        .body("headers.'Content-Type'", equalTo("image/png"))
        .body("retentionExpiresAt", notNullValue())
        .body("maxBytes", equalTo(1024));
  }

  @Test
  void rejectsImageUploadCredentialWhenRequestIsTooLarge() {
    given()
        .contentType("application/json")
        .body(
            """
            {
              "tenantId": "tenant_1",
              "filename": "photo.png",
              "mimeType": "image/png",
              "contentLengthBytes": 10485761
            }
            """)
        .when()
        .post("/v1/services/sample-agent/image-upload-credential")
        .then()
        .statusCode(400)
        .body("code", equalTo("INVALID_REQUEST"));
  }

  @Test
  void confirmsImageUploadCallbackAfterCredentialWasIssued() {
    String objectUrl =
        given()
            .contentType("application/json")
            .body(
                """
                {
                  "tenantId": "tenant_1",
                  "filename": "photo.png",
                  "mimeType": "image/png",
                  "contentLengthBytes": 1024
                }
                """)
            .when()
            .post("/v1/services/sample-agent/image-upload-credential")
            .then()
            .statusCode(200)
            .extract()
            .path("objectUrl");

    given()
        .contentType("application/json")
        .body(
            """
            {
              "tenantId": "tenant_1",
              "objectUrl": "%s",
              "mimeType": "image/png",
              "contentLengthBytes": 1024
            }
            """
                .formatted(objectUrl))
        .when()
        .post("/v1/services/sample-agent/image-upload-callback")
        .then()
        .statusCode(200)
        .body("objectUrl", equalTo(objectUrl))
        .body("mimeType", equalTo("image/png"))
        .body("contentLengthBytes", equalTo(1024))
        .body("retentionExpiresAt", notNullValue());
  }

  @Test
  void rejectsImageUploadCallbackForUnknownObjectUrl() {
    given()
        .contentType("application/json")
        .body(
            """
            {
              "tenantId": "tenant_1",
              "objectUrl": "https://cdn.example.com/uploads/tenant_1/sample-agent/missing",
              "mimeType": "image/png",
              "contentLengthBytes": 1024
            }
            """)
        .when()
        .post("/v1/services/sample-agent/image-upload-callback")
        .then()
        .statusCode(400)
        .body("code", equalTo("INVALID_REQUEST"));
  }

  @Test
  void hidesServiceCapabilitiesFromInvisibleTenant() {
    given()
        .header("X-Tenant-Id", "tenant_2")
        .when()
        .get("/v1/services/sample-agent/capabilities")
        .then()
        .statusCode(403)
        .body("code", equalTo("FORBIDDEN"));
  }

  @Test
  void returnsNotFoundForMissingServiceCapabilities() {
    given()
        .header("X-Tenant-Id", "tenant_1")
        .when()
        .get("/v1/services/missing-service/capabilities")
        .then()
        .statusCode(404)
        .body("code", equalTo("SERVICE_NOT_FOUND"));
  }

  @Test
  void submitsInputForWaitingRun() {
    Run run = waitingRun("run_input_1", "tenant_1", RunStatus.WAITING_INPUT);
    runRepository.create(run, null).toCompletableFuture().join();

    given()
        .urlEncodingEnabled(false)
        .contentType("application/json")
        .header("X-Tenant-Id", "tenant_1")
        .body(
            """
            {
              "payload": {"answer": "approved"}
            }
            """)
        .when()
        .post("/v1/runs/run_input_1:input")
        .then()
        .statusCode(200)
        .body("runId", equalTo("run_input_1"))
        .body("accepted", equalTo(true));
  }

  @Test
  void rejectsInputForRunThatIsNotWaiting() {
    Run run = waitingRun("run_input_2", "tenant_1", RunStatus.RUNNING);
    runRepository.create(run, null).toCompletableFuture().join();

    given()
        .urlEncodingEnabled(false)
        .contentType("application/json")
        .header("X-Tenant-Id", "tenant_1")
        .body(
            """
            {
              "payload": {"answer": "approved"}
            }
            """)
        .when()
        .post("/v1/runs/run_input_2:input")
        .then()
        .statusCode(400)
        .body("code", equalTo("INVALID_REQUEST"));
  }

  private static Run waitingRun(String runId, String tenantId, RunStatus status) {
    Instant now = Instant.parse("2026-07-27T07:00:00Z");
    return new Run(
        new RunId(runId),
        new ConversationId("conv_input"),
        new TenantId(tenantId),
        "sample-agent",
        status,
        null,
        0,
        now,
        now);
  }
}
