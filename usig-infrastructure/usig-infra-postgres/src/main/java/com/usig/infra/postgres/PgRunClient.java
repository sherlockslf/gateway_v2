package com.usig.infra.postgres;

import java.util.List;
import java.util.concurrent.CompletionStage;

interface PgRunClient {

  CompletionStage<List<PgRunRow>> query(String sql, List<Object> params);
}
