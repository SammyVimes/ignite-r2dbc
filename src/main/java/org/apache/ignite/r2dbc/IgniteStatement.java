package org.apache.ignite.r2dbc;

import io.r2dbc.spi.Statement;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IgniteStatement implements Statement {

    // search for $ or ? in the statement.
    private static final Pattern PARAMETER_SYMBOLS = Pattern.compile(".*([$?])([\\d]+).*");

    // the value of the binding will be on the second group
    private static final int BIND_POSITION_NUMBER_GROUP = 2;

    private final Bindings bindings = new Bindings();

    private final ClientWrapper client;

    private final String sql;

    private String[] generatedColumns;

    private boolean allGeneratedColumns = false;

    IgniteStatement(ClientWrapper client, String sql) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.sql = Objects.requireNonNull(sql, "sql must not be null");
    }

    @Override
    public IgniteStatement add() {
        this.bindings.finish();
        return this;
    }

    @Override
    public IgniteStatement bind(String name, Object value) {
        Objects.requireNonNull(name, "name must not be null");

        return addIndex(getIndex(name), value);
    }

    @Override
    public IgniteStatement bind(int index, Object value) {
        return addIndex(index, value);
    }

    @Override
    public IgniteStatement bindNull(String name, Class<?> type) {
        Objects.requireNonNull(name, "name must not be null");

        bindNull(getIndex(name), type);

        return this;
    }

    @Override
    public IgniteStatement bindNull(int index, @Nullable Class<?> type) {
        this.bindings.getCurrent().add(index, null);

        return this;
    }

    @Override
    public Flux<IgniteResult> execute() {
        return Flux.fromArray(this.sql.split(";"))
                .flatMap(sql -> {
                    if (this.generatedColumns == null) {
                        return execute(this.client, sql.trim(), this.bindings, this.allGeneratedColumns);
                    }
                    return execute(this.client, sql.trim(), this.bindings, this.generatedColumns);
                });
    }

    @Override
    public IgniteStatement returnGeneratedValues(String... columns) {
        Objects.requireNonNull(columns, "columns must not be null");

        if (columns.length == 0) {
            this.allGeneratedColumns = true;
        } else {
            this.generatedColumns = columns;
        }

        return this;
    }

    Binding getCurrentBinding() {
        return this.bindings.getCurrent();
    }

    private IgniteStatement addIndex(int index, Object value) {
        Objects.requireNonNull(value, "value must not be null");

        this.bindings.getCurrent().add(index, value);

        return this;
    }

    private static Flux<IgniteResult> execute(ClientWrapper client, String sql, Bindings bindings, Object generatedColumns) {
        return Flux.fromIterable(() -> client.prepareCommand(sql, bindings.bindings))
                .map(client::execute)
                .map(command -> {
                    QueryCursorImpl<List<?>> cursor = (QueryCursorImpl<List<?>>) command;
                    if (cursor.isQuery()) {
                        return IgniteResult.toResult(cursor);
                    } else {
                        List<List<?>> items = cursor.getAll();

                        Long updCnt = (Long) items.get(0).get(0);

                        return IgniteResult.toResult(updCnt.intValue());
                    }
                });
    }

    private int getIndex(String identifier) {
        Matcher matcher = PARAMETER_SYMBOLS.matcher(identifier);

        if (!matcher.find()) {
            throw new IllegalArgumentException(String.format("Identifier '%s' is not a valid identifier. Should be of the pattern '%s'.", identifier, PARAMETER_SYMBOLS.pattern()));
        }

        return Integer.parseInt(matcher.group(BIND_POSITION_NUMBER_GROUP)) - 1;
    }

    private static final class Bindings {

        private final List<Binding> bindings = new ArrayList<>();

        private Binding current;

        @Override
        public String toString() {
            return "Bindings{" +
                    "bindings=" + bindings +
                    ", current=" + current +
                    '}';
        }

        private void finish() {
            this.current = null;
        }

        private Binding getCurrent() {
            if (this.current == null) {
                this.current = new Binding();
                this.bindings.add(this.current);
            }

            return this.current;
        }
    }
}
