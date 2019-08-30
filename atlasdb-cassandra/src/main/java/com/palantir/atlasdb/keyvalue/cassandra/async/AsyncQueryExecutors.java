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

import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.tracing.AsyncTracer;
import com.palantir.util.Pair;

public class AsyncQueryExecutors {

    interface Visitor<V, R> {
        void visit(V value);
        R result();
        void visitResultSet(ResultSet rs, int remainingInPage);
    }

    static class AsyncQueryExecutor<I, V, R> {

        private final Executor executor;
        private final Session session;

        protected AsyncQueryExecutor(Executor executor, Session session) {
            this.executor = executor;
            this.session = session;
        }

        private <P, O> ListenableFuture<O> transform(ListenableFuture<P> input,
                Function<? super P, ? extends O> function) {
            AsyncTracer asyncTracer = new AsyncTracer();
            return Futures.transform(input, p -> asyncTracer.withTrace(() -> function.apply(p)),
                    executor);
        }

        private AsyncFunction<ResultSet, AsyncQueryExecutors.Visitor<V, R>> iterate(
                final AsyncQueryExecutors.Visitor<V, R> visitor) {
            return rs -> {
                // How far we can go without triggering the blocking fetch:
                int remainingInPage = rs.getAvailableWithoutFetching();

                visitor.visitResultSet(rs, remainingInPage);

                boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
                if (wasLastPage) {
                    return Futures.immediateFuture(visitor);
                } else {
                    ListenableFuture<ResultSet> future = rs.fetchMoreResults();
                    return Futures.transformAsync(future, iterate(visitor), executor);
                }
            };
        }

        public final ListenableFuture<Visitor<V, R>> executeQuery(BoundStatement boundStatement,
                Visitor<V, R> visitor) {
            return Futures.transformAsync(session.executeAsync(boundStatement), iterate(visitor),
                    executor);
        }

        public final ListenableFuture<R> executeQueries(Stream<Pair<I, BoundStatement>> boundStatementStream,
                Function<I, Visitor<V, R>> visitorSupplier,
                Function<List<Visitor<V, R>>, R> transformer
        ) {

            List<ListenableFuture<Visitor<V, R>>> allResults = boundStatementStream
                    .map(pair -> executeQuery(pair.rhSide, visitorSupplier.apply(pair.lhSide)))
                    .collect(Collectors.toList());

            return transform(Futures.allAsList(allResults), transformer);
        }
    }



}
