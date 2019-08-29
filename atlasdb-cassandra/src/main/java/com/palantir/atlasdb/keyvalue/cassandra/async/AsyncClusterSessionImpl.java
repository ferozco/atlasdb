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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.codahale.metrics.Metric;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.async.AsyncSessionManager.CassandraClusterSessionPair;
import com.palantir.common.base.Throwables;
import com.palantir.common.streams.KeyedStream;
import com.palantir.tracing.AsyncTracer;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;


public final class AsyncClusterSessionImpl implements AsyncClusterSession {

    private final StatementPreparation statementPreparation;
    private final CassandraClusterSessionPair pair;
    private final String sessionName;
    private final Executor executor;

    @Nonnull
    public static AsyncClusterSessionImpl create(String clusterName, CassandraClusterSessionPair pair,
            TaggedMetricRegistry taggedMetricRegistry, ThreadFactory threadFactory) {
        // TODO (OStevan): profile usage and see what value for cache size makes sense
        StatementPreparation statementPreparation = PerOperationStatementPreparation.create(pair.session(),
                taggedMetricRegistry, 100);
        return create(clusterName, pair, statementPreparation,
                Executors.newCachedThreadPool(threadFactory));
    }

    public static AsyncClusterSessionImpl create(String clusterName, CassandraClusterSessionPair pair,
            StatementPreparation statementPreparation, Executor executor) {
        return new AsyncClusterSessionImpl(clusterName, pair, statementPreparation, executor);
    }

    private AsyncClusterSessionImpl(String sessionName, CassandraClusterSessionPair pair,
            StatementPreparation statementPreparation, Executor executor) {
        this.sessionName = sessionName;
        this.pair = pair;
        this.statementPreparation = statementPreparation;
        this.executor = executor;
    }

    @Nonnull
    @Override
    public String getSessionName() {
        return sessionName;
    }

    @Override
    public Map<MetricName, Metric> getMetricsSet() {
        return KeyedStream.stream(
                pair.cluster().getMetrics()
                        .getRegistry()
                        .getMetrics())
                .mapKeys(name -> MetricName.builder().safeName(name).build())
                .collectToMap();
    }

    @Override
    public ListenableFuture<String> getCurrentTimeAsync() {
        PreparedStatement preparedStatement = statementPreparation.prepareCurrentTimeStatement();

        return Futures.transform(pair.session().executeAsync(preparedStatement.bind()),
                result -> {
                    Row row;
                    StringBuilder builder = new StringBuilder();
                    while ((row = result.one()) != null) {
                        builder.append(row.getTimestamp(0));
                    }
                    return builder.toString();
                }, executor);
    }

    /**
     * Not using in clause as per blog post in the link listed below, two reasons, makes the implementation less usage
     * aware and code is also simple.
     * <p>
     * https://lostechies.com/ryansvihla/2014/09/22/cassandra-query-patterns-not-using-the-in-query-for-multiple-partitions/
     *
     * @param keySpace of the table
     * @param tableRef where to look for values
     * @param timestampByCell information to use for query creation
     * @return future with the requested data
     */
    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(String keySpace, TableReference tableRef,
            Map<Cell, Long> timestampByCell) {
        try {
            PreparedStatement preparedStatement = statementPreparation.prepareGetStatement(keySpace, tableRef);
            // maybe something smarter then per cell query is smarter but we are counting that cassandra can optimise
            // the sending of queries
            List<ListenableFuture<Map<Cell, Value>>> allResults = timestampByCell.entrySet().parallelStream().map(
                    entry -> {
                        BoundStatement boundStatement = preparedStatement.bind()
                                .setBytes(StatementPreparation.FieldNameProvider.row,
                                        ByteBuffer.wrap(entry.getKey().getRowName()))
                                .setBytes(StatementPreparation.FieldNameProvider.column,
                                        ByteBuffer.wrap(entry.getKey().getColumnName()))
                                .setLong(StatementPreparation.FieldNameProvider.timestamp, entry.getValue());
                        VisitorWithState visitor = new VisitorWithState(entry.getKey());
                        return Futures.transformAsync(pair.session().executeAsync(boundStatement), iterate(visitor),
                                executor);
                    }).collect(Collectors.toList());

            return transform(Futures.allAsList(allResults), results -> {
                ImmutableMap.Builder<Cell, Value> builder = ImmutableMap.builder();
                results.forEach(builder::putAll);
                return builder.build();
            });
        } catch (Exception e) {
            return Futures.immediateFailedFuture(Throwables.unwrapAndThrowAtlasDbDependencyException(e));
        }
    }

    private <I, O> ListenableFuture<O> transform(ListenableFuture<I> input, Function<? super I, ? extends O> function) {
        AsyncTracer asyncTracer = new AsyncTracer();
        return Futures.transform(input, i -> asyncTracer.withTrace(() -> function.apply(i)),
                executor);
    }

    private AsyncFunction<ResultSet, Map<Cell, Value>> iterate(final VisitorWithState visitor) {
        return rs -> {
            // How far we can go without triggering the blocking fetch:
            int remainingInPage = rs.getAvailableWithoutFetching();

            visitor.visitResultSet(rs, remainingInPage);

            boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
            if (wasLastPage) {
                return Futures.immediateFuture(visitor.result());
            } else {
                ListenableFuture<ResultSet> future = rs.fetchMoreResults();
                return Futures.transformAsync(future, iterate(visitor), executor);
            }
        };
    }


    private static class VisitorWithState {
        private final AtomicReference<Value> maxValue = new AtomicReference<>();
        private final Cell associatedCell;

        VisitorWithState(Cell associatedCell) {
            this.associatedCell = associatedCell;
        }

        public void visit(Value value) {
            maxValue.updateAndGet(previous -> {
                if (previous == null) {
                    return value;
                }
                if (previous.getTimestamp() < value.getTimestamp()) {
                    return value;
                }
                return previous;
            });
        }

        public Map<Cell, Value> result() {
            if (maxValue.get() == null) {
                return ImmutableMap.of();
            }
            return ImmutableMap.of(associatedCell, maxValue.get());
        }

        public void visitResultSet(ResultSet rs, int remainingInPage) {
            int remaining = remainingInPage;
            if (remaining <= 0) {
                return;
            }
            for (Row row : rs) {
                visit(Value.create(row.getBytes(0).array(), row.getLong(1)));
                if (--remaining == 0) {
                    return;
                }
            }
        }
    }

    // TODO (OStevan): might make sense to keep a reference to unique ImmutableUniqueCassandraCluster and use it to
    //  close cluster connections one all sessions of the cluster are closed
    @Override
    public void close() {
        AsyncSessionManager.getAsyncSessionFactory().closeClusterSession(this);
    }
}
