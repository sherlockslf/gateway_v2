package com.usig.infra.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class QuarkusPostgresRunCommandsTest {

  private static final Instant NOW = Instant.parse("2026-07-27T07:30:00Z");

  @Test
  void insertOrFindChecksIdempotencyBeforeInsert() {
    RecordingPgRunClient client = new RecordingPgRunClient();
    client.enqueue(List.of());
    client.enqueue(List.of(row("run_1", null, 0)));
    QuarkusPostgresRunCommands commands =
        new QuarkusPostgresRunCommands(client, new ObjectMapper());

    PostgresRunRecord inserted =
        commands.insertOrFind(record("run_1", null, 0), "idem_1").toCompletableFuture().join();

    assertEquals("run_1", inserted.runId());
    assertTrue(client.requests.get(0).sql().contains("idempotency_key = $3"));
    assertEquals(List.of("tenant_1", "sample-agent", "idem_1"), client.requests.get(0).params());
    assertTrue(client.requests.get(1).sql().contains("insert into runs"));
    assertEquals("unknown", client.requests.get(1).params().get(4));
    assertEquals("idem_1", client.requests.get(1).params().get(7));
  }

  @Test
  void insertOrFindReturnsExistingIdempotentRunWithoutInsert() {
    RecordingPgRunClient client = new RecordingPgRunClient();
    client.enqueue(List.of(row("run_existing", null, 2)));
    QuarkusPostgresRunCommands commands =
        new QuarkusPostgresRunCommands(client, new ObjectMapper());

    PostgresRunRecord existing =
        commands.insertOrFind(record("run_new", null, 0), "idem_1").toCompletableFuture().join();

    assertEquals("run_existing", existing.runId());
    assertEquals(1, client.requests.size());
  }

  @Test
  void savesExecutionHandleAsJsonbAndAdapterId() {
    RecordingPgRunClient client = new RecordingPgRunClient();
    PostgresExecutionHandleRecord handle =
        new PostgresExecutionHandleRecord(
            "sample-agent",
            "sample-adapter",
            "task_1",
            "thread_1",
            "downstream_run_1",
            Map.of("k", "v"));
    client.enqueue(List.of(row("run_1", handle, 1)));
    QuarkusPostgresRunCommands commands =
        new QuarkusPostgresRunCommands(client, new ObjectMapper());

    PostgresRunRecord updated =
        commands
            .saveExecutionHandle("run_1", handle, NOW)
            .toCompletableFuture()
            .join()
            .orElseThrow();

    assertTrue(client.requests.getFirst().sql().contains("execution_handle = $2::jsonb"));
    assertTrue(
        client
            .requests
            .getFirst()
            .params()
            .get(1)
            .toString()
            .contains("\"adapterId\":\"sample-adapter\""));
    assertEquals("sample-adapter", client.requests.getFirst().params().get(2));
    assertEquals(handle, updated.executionHandle());
  }

  @Test
  void compareAndSetStatusUsesVersionPredicate() {
    RecordingPgRunClient client = new RecordingPgRunClient();
    client.enqueue(List.of(row("run_1", null, 3)));
    QuarkusPostgresRunCommands commands =
        new QuarkusPostgresRunCommands(client, new ObjectMapper());

    commands
        .compareAndSetStatus("run_1", 2, "RUNNING", NOW)
        .toCompletableFuture()
        .join()
        .orElseThrow();

    assertTrue(client.requests.getFirst().sql().contains("and version = $2"));
    assertEquals(List.of("run_1", 2L, "RUNNING", NOW), client.requests.getFirst().params());
  }

  @Test
  void findByIdAndTenantMapsRowsToRecords() {
    RecordingPgRunClient client = new RecordingPgRunClient();
    client.enqueue(List.of(row("run_1", null, 0)));
    QuarkusPostgresRunCommands commands =
        new QuarkusPostgresRunCommands(client, new ObjectMapper());

    PostgresRunRecord found =
        commands.findByIdAndTenant("run_1", "tenant_1").toCompletableFuture().join().orElseThrow();

    assertTrue(client.requests.getFirst().sql().contains("tenant_id = $2"));
    assertEquals(List.of("run_1", "tenant_1"), client.requests.getFirst().params());
    assertEquals("conv_1", found.conversationId());
  }

  private static PostgresRunRecord record(
      String runId, PostgresExecutionHandleRecord handle, long version) {
    return new PostgresRunRecord(
        runId, "conv_1", "tenant_1", "sample-agent", "ACCEPTED", handle, version, NOW, NOW);
  }

  private static PgRunRow row(String runId, PostgresExecutionHandleRecord handle, long version) {
    return new PgRunRow(
        runId, "conv_1", "tenant_1", "sample-agent", "ACCEPTED", json(handle), version, NOW, NOW);
  }

  private static String json(PostgresExecutionHandleRecord handle) {
    if (handle == null) {
      return null;
    }
    try {
      return new ObjectMapper().writeValueAsString(handle);
    } catch (Exception exception) {
      throw new IllegalStateException(exception);
    }
  }

  private static final class RecordingPgRunClient implements PgRunClient {

    private final List<Request> requests = new ArrayList<>();
    private final ArrayDeque<List<PgRunRow>> responses = new ArrayDeque<>();

    private void enqueue(List<PgRunRow> response) {
      responses.add(response);
    }

    @Override
    public CompletionStage<List<PgRunRow>> query(String sql, List<Object> params) {
      requests.add(new Request(sql, new ArrayList<>(params)));
      return CompletableFuture.completedFuture(responses.removeFirst());
    }
  }

  private record Request(String sql, List<Object> params) {}
}
