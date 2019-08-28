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

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;

import com.codahale.metrics.Metric;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.async.AsyncSessionManager.CassandraClusterSessionPair;
import com.palantir.common.streams.KeyedStream;
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

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(String keySpace, TableReference tableRef,
            Map<Cell, Long> timestampByCell) {
//        PreparedStatement preparedStatement = statementPreparation.prepareGetStatement(keySpace, tableRef);
//
//
//         listOfFutures = Futures.allAsList(timestampByCell.entrySet().parallelStream().map(entry -> {
//            Cell key = entry.getKey();
//            Long value = entry.getValue();
//            BoundStatement boundStatement = preparedStatement.bind()
//                    .setBytes(StatementPreparation.FieldNameProvider.row, ByteBuffer.wrap(key.getRowName()))
//                    .setBytes(StatementPreparation.FieldNameProvider.column, ByteBuffer.wrap(key.getColumnName()))
//                    .setLong(StatementPreparation.FieldNameProvider.timestamp, value);
//            return pair.executeAsync(boundStatement);
//        }).collect(Collectors.toList()));
//
//        ListenableFuture<Map<Cell, Value>> result;
//        return result;
        throw new UnsupportedOperationException("Get is currently not supported");
    }

    // TODO (OStevan): might make sense to keep a reference to unique ImmutableUniqueCassandraCluster and use it to
    //  close cluster connections one all sessions of the cluster are closed
    @Override
    public void close() {
        AsyncSessionManager.getAsyncSessionFactory().closeClusterSession(this);
    }
}
