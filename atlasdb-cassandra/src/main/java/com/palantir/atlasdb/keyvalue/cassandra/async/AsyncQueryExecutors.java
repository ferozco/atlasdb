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
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.tracing.AsyncTracer;
import com.palantir.util.Pair;

public final class AsyncQueryExecutors {

    private AsyncQueryExecutors() {

    }

    /**
     * Accumulator or results from a ResultSet.
     *
     * @param <V> value of one row
     * @param <R> result of visiting the result set
     */
    interface Visitor<V, R> {

        /**
         * Processes one supplied value.
         *
         * @param value to process
         */
        void visit(V value);

        /**
         * Returns the current result of visiting a result set.
         *
         * @return cumulative result of processing (can be min, max, collection, etc.)
         */
        R result();

        V visitRow(Row row);

        /**
         * Visits numberOfRowsToVisit rows from the given resultSet. Can be blocking if the number supplied is greater
         * then the currently available if the result set without fetching.
         *
         * @param resultSet containing the result of the query
         * @param numberOfRowsToVisit in the given resultSet
         */
        default void visitResultSet(ResultSet resultSet, int numberOfRowsToVisit) {
            int remaining = numberOfRowsToVisit;
            if (remaining <= 0) {
                return;
            }
            for (Row row : resultSet) {
                visit(visitRow(row));
                if (--remaining == 0) {
                    return;
                }
            }
        }
    }

    static class GetQueryVisitor implements Visitor<Value, Map<Cell, Value>> {
        // very likely doesn't need to be atomic
        private final AtomicReference<Value> maxValue = new AtomicReference<>();
        private final Cell associatedCell;

        GetQueryVisitor(Cell associatedCell) {
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

        @Override
        public Value visitRow(Row row) {
            return Value.create(row.getBytes(0).array(), row.getLong(1));
        }
    }

    static class AsyncQueryExecutor {

        private final Executor executor;
        private final Session session;

        protected AsyncQueryExecutor(Executor executor, Session session) {
            this.executor = executor;
            this.session = session;
        }

        /**
         * Transforms the return type of a an input future using the supplied function.
         *
         * @param input future to transform
         * @param function which will do the transformation
         * @param <P> type of the return value of input future
         * @param <O> type of the future return value after transformation
         * @return transformed future
         */
        private <P, O> ListenableFuture<O> transform(ListenableFuture<P> input,
                Function<? super P, ? extends O> function) {
            AsyncTracer asyncTracer = new AsyncTracer();
            return Futures.transform(input, p -> asyncTracer.withTrace(() -> function.apply(p)),
                    executor);
        }

        /**
         * Returns an AsyncFunction which will iterate through the ResultSet while ahe same time paging the results
         * reducing the time spent on a blocked thread.
         *
         * @param visitor which processes the retrieved data
         * @param <V> type of one result set row after executing a query
         * @param <R> result type of processing all rows in a ResultSet
         * @return visitor containing the result of all processing
         */
        private <V, R> AsyncFunction<ResultSet, Visitor<V, R>> iterate(
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

        /**
         * Executes one query and returns a ListenableFuture containing the result.
         *
         * @param statement to be executed
         * @param visitor which will be used to process the result of query execution
         * @param <V> type of one row row returned by a query
         * @param <R> final result of executing all queries
         * @return future containing the result of processing a query
         */
        private <V, R> ListenableFuture<Visitor<V, R>> executeQuery(Statement statement,
                Visitor<V, R> visitor) {
            return Futures.transformAsync(session.executeAsync(statement), iterate(visitor),
                    executor);
        }

        public <V, R> ListenableFuture<R> executeQuery(Statement statement, Visitor<V, R> visitor,
                Function<Visitor<V, R>, R> transformer) {
            return transform(executeQuery(statement, visitor), transformer);
        }

        /**
         * Executes a all queries in a stream asynchronously and returns the result of processing each of them using
         * visitors created by visitorCreator and collecting all results with a transformer.
         *
         * @param inputStatementPairStream stream of input/statement pairs where each statement was created using input
         * to create it
         * @param visitorCreator creates a visitor using the input used to create a statement
         * @param transformer used to transform results of individual queries to a single result
         * @param <I> type of input for which a corresponding query was created
         * @param <V> type of one row row returned by a query
         * @param <R> final result of executing all queries
         * @return future containing the result of processing a query
         */
        public final <I, V, R> ListenableFuture<R> executeQueries(Stream<Pair<I, Statement>> inputStatementPairStream,
                Function<I, Visitor<V, R>> visitorCreator,
                Function<List<Visitor<V, R>>, R> transformer
        ) {

            List<ListenableFuture<Visitor<V, R>>> allResults = inputStatementPairStream
                    .map(pair -> executeQuery(pair.rhSide, visitorCreator.apply(pair.lhSide)))
                    .collect(Collectors.toList());

            return transform(Futures.allAsList(allResults), transformer);
        }
    }
}
