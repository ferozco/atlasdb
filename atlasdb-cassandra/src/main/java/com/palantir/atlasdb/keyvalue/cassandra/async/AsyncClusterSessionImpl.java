/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.keyvalue.cassandra.async;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Metric;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.async.AsyncQueryExecutors.AsyncQueryExecutor;
import com.palantir.atlasdb.keyvalue.cassandra.async.AsyncQueryExecutors.GetQueryVisitor;
import com.palantir.atlasdb.keyvalue.cassandra.async.AsyncSessionManager.CassandraClusterSessionPair;
import com.palantir.atlasdb.keyvalue.cassandra.async.StatementPreparations.StatementPreparation;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.util.Pair;

public final class AsyncClusterSessionImpl implements AsyncClusterSession {

    private static final Logger log = LoggerFactory.getLogger(AsyncClusterSessionImpl.class);

    private final CassandraClusterSessionPair pair;
    private final String clusterSessionName;
    private final AsyncQueryExecutor asyncQueryExecutor;
    private final StatementPreparation statementPreparation;

    private final ScheduledExecutorService service = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory(sessionName() + "-healtcheck", true /* daemon */));


    public static AsyncClusterSessionImpl create(String clusterName, CassandraClusterSessionPair pair,
            ThreadFactory threadFactory) {
        return create(clusterName, pair, Executors.newCachedThreadPool(threadFactory));
    }

    public static AsyncClusterSessionImpl create(String clusterName, CassandraClusterSessionPair pair,
            Executor executor) {
        return create(clusterName, pair, executor,
                StatementPreparations.UnifiedCacheStatementPreparation.create(pair.session(), 100));
    }

    public static AsyncClusterSessionImpl create(String clusterName, CassandraClusterSessionPair pair,
            Executor executor, StatementPreparation statementPreparation) {
        return create(clusterName, pair, new AsyncQueryExecutor(executor, pair.session()), statementPreparation);
    }

    public static AsyncClusterSessionImpl create(String clusterName, CassandraClusterSessionPair pair,
            AsyncQueryExecutor executor, StatementPreparation statementPreparation) {
        return new AsyncClusterSessionImpl(clusterName, pair, executor, statementPreparation);
    }

    private AsyncClusterSessionImpl(String clusterSessionName, CassandraClusterSessionPair pair,
            AsyncQueryExecutor executor,
            StatementPreparation statementPreparation) {
        this.clusterSessionName = clusterSessionName;
        this.pair = pair;
        this.asyncQueryExecutor = executor;
        this.statementPreparation = statementPreparation;
    }

    @Override
    public Map<MetricName, Metric> usedCqlClusterMetrics() {
        return KeyedStream.stream(
                pair.cluster().getMetrics()
                        .getRegistry()
                        .getMetrics())
                .mapKeys(name -> MetricName.builder().safeName(name).build())
                .collectToMap();
    }

    @Nonnull
    @Override
    public String sessionName() {
        return clusterSessionName;
    }

    @Override
    public void close() {
        service.shutdownNow();
        log.info("Shutting down health checker for cluster session {}",
                SafeArg.of("clusterSession", clusterSessionName));
        boolean shutdown = false;
        try {
            shutdown = service.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted while shutting down health checker. Should not happen");
            Thread.currentThread().interrupt();
        } finally {
            AsyncSessionManager.getOrInitializeAsyncSessionManager().closeClusterSession(this);
        }

        if (!shutdown) {
            log.error("Failed to shutdown health checker in a timely manner for {}",
                    SafeArg.of("clusterSession", clusterSessionName));
        } else {
            log.info("Shut down health checker for cluster session {}",
                    SafeArg.of("clusterSession", clusterSessionName));
        }
    }

    @Override
    public void start() {
        service.scheduleAtFixedRate(() -> {
            ListenableFuture<String> time = getTimeAsync();
            try {
                log.info("Current cluster time is: {}", SafeArg.of("clusterTime", time.get()));
            } catch (Exception e) {
                log.info("Cluster session health check failed");
            }
        }, 0, 1, TimeUnit.MINUTES);
    }

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(String keySpace, TableReference tableRef,
            Map<Cell, Long> timestampByCell) {
        try {
            PreparedStatement preparedStatement = statementPreparation.prepareGetStatement(keySpace, tableRef);


            Stream<Pair<Cell, Statement>> cellStatementStream = timestampByCell.entrySet().parallelStream().map(
                    entry ->
                            new Pair<>(entry.getKey(),
                                    preparedStatement.bind()
                                            .setBytes(StatementPreparations.FieldNameProvider.row,
                                                    ByteBuffer.wrap(entry.getKey().getRowName()))
                                            .setBytes(StatementPreparations.FieldNameProvider.column,
                                                    ByteBuffer.wrap(entry.getKey().getColumnName()))
                                            .setLong(StatementPreparations.FieldNameProvider.timestamp,
                                                    entry.getValue())
                            )
            );

            return asyncQueryExecutor.executeQueries(cellStatementStream, GetQueryVisitor::new,
                    results -> {
                        ImmutableMap.Builder<Cell, Value> builder = ImmutableMap.builder();
                        results.stream().map(AsyncQueryExecutors.Visitor::result).forEach(builder::putAll);
                        return builder.build();
                    });

        } catch (Exception e) {
            return Futures.immediateFailedFuture(Throwables.unwrapAndThrowAtlasDbDependencyException(e));
        }
    }

    private ListenableFuture<String> getTimeAsync() {
        Statement currentTimeStatement = statementPreparation.prepareCurrentTimeStatement().bind();

        return asyncQueryExecutor.executeQuery(currentTimeStatement, new AsyncQueryExecutors.Visitor<String, String>() {
            private String result;

            @Override
            public void visit(String value) {
                if (result == null) {
                    result = value;
                } else {
                    throw Throwables.throwUncheckedException(
                            new RuntimeException("Health check query returned multiple lines"));
                }
            }

            @Override
            public String result() {
                return result;
            }

            @Override
            public String visitRow(Row row) {
                return row.getString(0);
            }
        }, AsyncQueryExecutors.Visitor::result);
    }
}
