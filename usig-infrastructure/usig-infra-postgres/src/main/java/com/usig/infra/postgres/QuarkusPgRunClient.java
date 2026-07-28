package com.usig.infra.postgres;

import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.Tuple;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

public final class QuarkusPgRunClient implements PgRunClient {

  private final PgPool pgPool;

  public QuarkusPgRunClient(PgPool pgPool) {
    this.pgPool = Objects.requireNonNull(pgPool, "pgPool must not be null");
  }

  @Override
  public CompletionStage<List<PgRunRow>> query(String sql, List<Object> params) {
    return pgPool
        .preparedQuery(sql)
        .execute(Tuple.from(params))
        .onItem()
        .transform(
            rows -> {
              List<PgRunRow> result = new ArrayList<>();
              for (Row row : rows) {
                result.add(toRow(row));
              }
              return result;
            })
        .subscribeAsCompletionStage();
  }

  private static PgRunRow toRow(Row row) {
    return new PgRunRow(
        row.getString("run_id"),
        row.getString("conversation_id"),
        row.getString("tenant_id"),
        row.getString("service_id"),
        row.getString("status"),
        row.getString("execution_handle"),
        row.getLong("version"),
        row.getOffsetDateTime("created_at").toInstant(),
        row.getOffsetDateTime("updated_at").toInstant());
  }
}
