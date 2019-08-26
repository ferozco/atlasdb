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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;

import com.codahale.metrics.Metric;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.streams.KeyedStream;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;


public final class AsyncClusterSessionImpl implements AsyncClusterSession {

    private final StatementPreparation statementPreparation;
    private final Session session;
    private final String sessionName;
    private final Executor executor;

    @Nonnull
    public static AsyncClusterSessionImpl create(String clusterName, Session session,
            TaggedMetricRegistry taggedMetricRegistry, ThreadFactory threadFactory) {
        // TODO (OStevan): profile usage and see what value for cache size makes sense
        StatementPreparation statementPreparation = StatementPreparationImpl.create(session, taggedMetricRegistry,
                100);
        return create(clusterName, session, statementPreparation,
                Executors.newCachedThreadPool(threadFactory));
    }

    public static AsyncClusterSessionImpl create(String clusterName, Session session,
            StatementPreparation statementPreparation, Executor executor) {
        return new AsyncClusterSessionImpl(clusterName, session, statementPreparation, executor);
    }

    private AsyncClusterSessionImpl(String sessionName, Session session, StatementPreparation statementPreparation,
            Executor executor) {
        this.sessionName = sessionName;
        this.session = session;
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
                session.getCluster().getMetrics()
                        .getRegistry()
                        .getMetrics())
                .mapKeys(name -> MetricName.builder().safeName(name).build())
                .collectToMap();
    }

    @Override
    public ListenableFuture<String> getCurrentTimeAsync() {
        PreparedStatement preparedStatement = statementPreparation.prepareCurrentTimeStatement();

        return Futures.transform(session.executeAsync(preparedStatement.bind()),
                result -> {
                    Row row;
                    StringBuilder builder = new StringBuilder();
                    while ((row = result.one()) != null) {
                        builder.append(row.getString(0));
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
//            return session.executeAsync(boundStatement);
//        }).collect(Collectors.toList()));
//
//        ListenableFuture<Map<Cell, Value>> result;
//        return result;
        throw new UnsupportedOperationException("Get is currently not supported");
    }

    @Override
    public void close() throws IOException {
        session.close();
    }
}
