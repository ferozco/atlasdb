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
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;


public final class AsyncClusterSessionImpl implements AsyncClusterSession {

    private final StatementPreparation statementPreparation;

    private final Session session;

    public static AsyncClusterSessionImpl create(Session session,
            TaggedMetricRegistry taggedMetricRegistry) {
        // TODO (OStevan): profile usage and see what value for cache size makes sense
        StatementPreparation statementPreparation = StatementPreparationImpl.create(session, taggedMetricRegistry,
                100);
        return new AsyncClusterSessionImpl(session, statementPreparation);
    }

    public static AsyncClusterSessionImpl create(Session session, StatementPreparation statementPreparation) {
        return new AsyncClusterSessionImpl(session, statementPreparation);
    }

    private AsyncClusterSessionImpl(Session session,
            StatementPreparation statementPreparation) {
        this.session = session;
        this.statementPreparation = statementPreparation;
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
//                    .setBytes(StatementPreparation.FieldNameProvider.ROW, ByteBuffer.wrap(key.getRowName()))
//                    .setBytes(StatementPreparation.FieldNameProvider.COLUMN, ByteBuffer.wrap(key.getColumnName()))
//                    .setLong(StatementPreparation.FieldNameProvider.TIMESTAMP, value);
//            return session.executeAsync(boundStatement);
//        }).collect(Collectors.toList()));
//
//        ListenableFuture<Map<Cell, Value>> result;
//        return result;
        return null;
    }

    @Override
    public ListenableFuture<String> getCurrentTimeAsync() {
        PreparedStatement preparedStatement = statementPreparation.prepareCurrentTimeStatement();

        return Futures.transform(session.executeAsync(preparedStatement.bind()), result -> {
                    Row row;
                    StringBuilder builder = new StringBuilder();
                    while ((row = result.one()) != null) {
                        builder.append(row.getString(0));
                    }
                    return builder.toString();
                },
                MoreExecutors.directExecutor());
    }

    @Override
    public void close() throws IOException {
        session.close();
    }
}
