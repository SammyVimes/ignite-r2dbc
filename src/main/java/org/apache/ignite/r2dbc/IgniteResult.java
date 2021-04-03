package org.apache.ignite.r2dbc;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

public class IgniteResult implements Result {

    private final IgniteRowMetadata rowMetadata;

    private final Flux<IgniteRow> rows;

    private final Mono<Integer> rowsUpdated;

    private IgniteResult(Mono<Integer> rowsUpdated) {
        this.rowMetadata = null;
        this.rows = Flux.empty();
        this.rowsUpdated = Objects.requireNonNull(rowsUpdated, "rowsUpdated must not be null");
    }

    IgniteResult(IgniteRowMetadata rowMetadata, Flux<IgniteRow> rows, Mono<Integer> rowsUpdated) {
        this.rowMetadata = Objects.requireNonNull(rowMetadata, "rowMetadata must not be null");
        this.rows = Objects.requireNonNull(rows, "rows must not be null");
        this.rowsUpdated = Objects.requireNonNull(rowsUpdated, "rowsUpdated must not be null");
    }

    @Override
    public Mono<Integer> getRowsUpdated() {
        return this.rowsUpdated;
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
        Objects.requireNonNull(f, "f must not be null");

        return this.rows
                .map(row -> f.apply(row, this.rowMetadata));
    }

    @Override
    public String toString() {
        return "IgniteResult{" +
                ", rowMetadata=" + this.rowMetadata +
                ", rows=" + this.rows +
                ", rowsUpdated=" + this.rowsUpdated +
                '}';
    }

    static IgniteResult toResult(@Nullable Integer rowsUpdated) {
        return new IgniteResult(Mono.justOrEmpty(rowsUpdated));
    }

    static IgniteResult toResult(QueryCursorImpl<List<?>> result) {
        Objects.requireNonNull(result, "result must not be null");

        Iterator<List<?>> iterator = result.iterator();

        IgniteRowMetadata rowMetadata = IgniteRowMetadata.toRowMetadata(result);

        Iterable<List<Object>> iterable = () -> new Iterator<List<Object>>() {

            @Override
            public boolean hasNext() {
                boolean b = iterator.hasNext();

                if (!b) {
                    result.close();
                }

                return b;
            }

            @Override
            public List<Object> next() {
                //noinspection unchecked
                return (List<Object>) iterator.next();
            }
        };

        Flux<IgniteRow> rows = Flux.fromIterable(iterable)
                .map(values -> IgniteRow.toRow(values, rowMetadata))
                .onErrorMap(IgniteException.class, IgniteExceptionFactory::convert);

        return new IgniteResult(rowMetadata, rows, Mono.empty());
    }
}
