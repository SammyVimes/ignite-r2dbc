package org.apache.ignite.r2dbc;

import io.r2dbc.spi.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.Objects;
import java.util.function.Function;

import static io.r2dbc.spi.IsolationLevel.*;

public class IgniteConnection implements Connection {

    private final Logger logger = Loggers.getLogger(this.getClass());

    private final ClientWrapper client;

    private IsolationLevel isolationLevel = READ_UNCOMMITTED;

    IgniteConnection(ClientWrapper client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public Mono<Void> beginTransaction() {
        return useTransactionStatus(inTransaction -> {
            if (inTransaction) {
                this.logger.debug("Skipping begin transaction because already in one");
            }
            else {
                this.client.beginTransaction(isolationLevel);
            }
            return Mono.empty();
        }).onErrorMap(IgniteException.class, IgniteExceptionFactory::convert);
    }

    @Override
    public Mono<Void> close() {
        return this.client.close();
    }

    @Override
    public Mono<Void> commitTransaction() {
        return useTransactionStatus(inTransaction -> {
            if (inTransaction) {
                return this.client.commit();
            } else {
                this.logger.debug("Skipping commit transaction because no transaction in progress.");
            }

            return Mono.empty();
        }).onErrorMap(IgniteException.class, IgniteExceptionFactory::convert);
    }

    @Override
    public IgniteBatch createBatch() {
        return new IgniteBatch(this.client);
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        throw new UnsupportedOperationException("");
//        Assert.requireNonNull(name, "name must not be null");
//
//        return beginTransaction()
//                .then(Mono.<Void>fromRunnable(() -> this.client.execute(String.format("SAVEPOINT %s", name))))
//                .onErrorMap(DbException.class, H2DatabaseExceptionFactory::convert);
    }

    @Override
    public IgniteStatement createStatement(String sql) {
        return new IgniteStatement(this.client, sql);
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return this.isolationLevel;
    }

    @Override
    public ConnectionMetadata getMetadata() {
        return null;
    }

    @Override
    public boolean isAutoCommit() {
        return false;
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        throw new UnsupportedOperationException("");
//        Assert.requireNonNull(name, "name must not be null");
//
//        return useTransactionStatus(inTransaction -> {
//            if (inTransaction) {
//                this.client.execute(String.format("RELEASE SAVEPOINT %s", name));
//            } else {
//                this.logger.debug("Skipping release savepoint because no transaction in progress.");
//            }
//
//            return Mono.empty();
//        })
//                .onErrorMap(DbException.class, H2DatabaseExceptionFactory::convert);
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return useTransactionStatus(inTransaction -> {
            if (inTransaction) {
                return this.client.rollback();
            } else {
                this.logger.debug("Skipping rollback because no transaction in progress.");
            }
            return Mono.empty();
        }).onErrorMap(IgniteException.class, IgniteExceptionFactory::convert);
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        throw new UnsupportedOperationException("");
//        Objects.requireNonNull(name, "name must not be null");
//
//        return useTransactionStatus(inTransaction -> {
//            if (inTransaction) {
//                this.client.execute(String.format("ROLLBACK TO SAVEPOINT %s", name));
//            } else {
//                this.logger.debug("Skipping rollback to savepoint because no transaction in progress.");
//            }
//
//            return Mono.empty();
//        })
//                .onErrorMap(DbException.class, H2DatabaseExceptionFactory::convert);
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {
        return Mono.empty();
//        return Mono.fromRunnable(() -> this.client.getSession().setAutoCommit(autoCommit));
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel must not be null");
        this.isolationLevel = isolationLevel;
        return Mono.empty();
    }

    /**
     * Validates the connection according to the given {@link ValidationDepth}.
     *
     * @param depth the validation depth
     * @return a {@link Publisher} that indicates whether the validation was successful
     * @throws IllegalArgumentException if {@code depth} is {@code null}
     */
    @Override
    public Mono<Boolean> validate(ValidationDepth depth) {
        Objects.requireNonNull(depth, "depth must not be null");

        throw new UnsupportedOperationException("");

//        return Mono.fromCallable(() -> {
//            if (this.client.getSession().isClosed()) {
//                return false;
//            }
//
//            this.client.query(this.client.prepareCommand("SELECT CURRENT_TIMESTAMP", Collections.emptyList()).next());
//
//            return true;
//        })
//                .switchIfEmpty(Mono.just(false));
    }

    private Mono<Void> useTransactionStatus(Function<Boolean, Publisher<?>> f) {
        return Flux.defer(() -> f.apply(this.client.inTransaction()))
                .onErrorMap(IgniteException.class, IgniteExceptionFactory::convert)
                .then();
    }
}
