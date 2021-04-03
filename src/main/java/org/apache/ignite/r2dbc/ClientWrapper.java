package org.apache.ignite.r2dbc;

import io.r2dbc.spi.IsolationLevel;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.*;

import static io.r2dbc.spi.IsolationLevel.*;

public class ClientWrapper {

    private final Logger logger = Loggers.getLogger(this.getClass());

    private final Ignite ignite;

    private final Collection<Binding> emptyBinding = Collections.singleton(Binding.EMPTY);

    private final TransactionConcurrency concurrency = TransactionConcurrency.PESSIMISTIC;

    public ClientWrapper(final Ignite ignite) {
        this.ignite = ignite;
    }

    public void beginTransaction(IsolationLevel isolation) throws IllegalStateException {
        ignite.transactions().txStart(concurrency, fromIsolationLevel(isolation));
    }

    private static TransactionIsolation fromIsolationLevel(IsolationLevel level) {
        if (READ_COMMITTED == level) {
            return TransactionIsolation.READ_COMMITTED;
        } else if (READ_UNCOMMITTED == level) {
            return TransactionIsolation.READ_COMMITTED;
        } else if (REPEATABLE_READ == level) {
            return TransactionIsolation.REPEATABLE_READ;
        } else if (SERIALIZABLE == level) {
            return TransactionIsolation.SERIALIZABLE;
        } else {
            throw new IllegalArgumentException(String.format("Invalid isolation level %s", level));
        }
    }

    public boolean inTransaction() {
        return this.ignite.transactions().tx() != null;
    }

    public Mono<Void> close() {
        return Mono.defer(() -> {
            try {
                this.ignite.close();
            }
            catch (IgniteException e) {
                return Mono.error(IgniteExceptionFactory.convert(e));
            }
            return Mono.empty();
        });
    }

    public Mono<Void> commit() {
        IgniteFutureImpl<Void> commitAsync = (IgniteFutureImpl<Void>) this.ignite.transactions().tx().commitAsync();
        return Mono.create(sink -> {
            commitAsync.listen(aVoid -> {
                try {
                    aVoid.get();
                    sink.success();
                }
                catch (IgniteException e) {
                    sink.error(e);
                }
            });
        });
    }

    public Mono<Void> rollback() {
        IgniteFuture<Void> rollbackAsync = this.ignite.transactions().tx().rollbackAsync();
        return Mono.create(sink -> {
            rollbackAsync.listen(aVoid -> {
                try {
                    aVoid.get();
                    sink.success();
                }
                catch (IgniteException e) {
                    sink.error(e);
                }
            });
        });
    }

    public FieldsQueryCursor<List<?>> execute(SqlFieldsQuery query) {
        String cacheName = this.ignite.cacheNames().stream().findFirst().get();
        FieldsQueryCursor<List<?>> cursor = this.ignite.cache(cacheName).query(query);
        return cursor;
    }

    public Iterator<SqlFieldsQuery> prepareCommand(final String sql, final List<Binding> bindings) {
        Objects.requireNonNull(sql, "sql must not be null");
        Objects.requireNonNull(bindings, "bindings must not be null");

        Iterator<Binding> bindingIterator = bindings.isEmpty() ? emptyBinding.iterator() : bindings.iterator();

        return new Iterator<SqlFieldsQuery>() {

            @Override
            public boolean hasNext() {
                return bindingIterator.hasNext();
            }

            @Override
            public SqlFieldsQuery next() {
                Binding binding = bindingIterator.next();

                SqlFieldsQuery command = createCommand(sql, binding);
                logger.debug("Request:  {}", command);
                return command;
            }
        };
    }

    private SqlFieldsQuery createCommand(String sql, Binding binding) {
        SqlFieldsQuery query = new SqlFieldsQuery(sql);

        query.setArgs(binding.getParameters().values().toArray(new Object[0]));

        return query;
    }
}
