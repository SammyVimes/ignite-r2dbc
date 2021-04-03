package org.apache.ignite.r2dbc;

import io.r2dbc.spi.Batch;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class IgniteBatch implements Batch {

    private final ClientWrapper client;

    private final List<String> statements = new ArrayList<>();

    IgniteBatch(ClientWrapper client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public IgniteBatch add(String sql) {
        Objects.requireNonNull(sql, "sql must not be null");

        this.statements.add(sql);
        return this;
    }

    @Override
    public Flux<IgniteResult> execute() {
        return Flux.fromIterable(this.statements)
                .flatMapIterable(statement -> () -> this.client.prepareCommand(statement, Collections.emptyList()))
                .map(client::execute)
                .map(command -> {
                    QueryCursorImpl<List<?>> cursor = (QueryCursorImpl<List<?>>) command;
                    if (cursor.isQuery()) {
                        return IgniteResult.toResult(cursor);
                    } else {
                        List<List<?>> items = cursor.getAll();

                        Integer updCnt = (Integer) items.get(0).get(0);

                        return IgniteResult.toResult(updCnt);
                    }
                });
    }

}
