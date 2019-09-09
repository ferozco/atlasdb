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

import java.util.function.Function;
import java.util.stream.Collectors;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.tritium.metrics.caffeine.CaffeineCacheStats;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class StatementPreparations {

    private StatementPreparations() {

    }

    // TODO (OStevan): check if this already exists somewhere
    static class FieldNameProvider {
        public static String row = "key";
        public static String column = "column1";
        public static String timestamp = "column2";
        public static String value = "value";

        protected FieldNameProvider() {}
    }

    static final String ALL = "all";
    static final String TIME = "time";
    static final String GET = "get";

    static final String TIME_PATTERN = "SELECT dateof(now()) FROM %s ;";
    static final String GET_PATTERN =
            "SELECT " + FieldNameProvider.value + ',' + FieldNameProvider.timestamp + " FROM %s "
                    + "WHERE " + FieldNameProvider.row + " = " + ":" + FieldNameProvider.row
                    + " AND " + FieldNameProvider.column + " = " + ":" + FieldNameProvider.column
                    + " AND " + FieldNameProvider.timestamp + " > " + ":" + FieldNameProvider.timestamp + " ;";

    static final ImmutableSet<String> SUPPORTED_OPERATIONS = ImmutableSet.of(ALL, GET, TIME);

    static Cache<String, PreparedStatement> createAndRegisterCache(TaggedMetricRegistry taggedMetricRegistry,
            String operation, int cacheSize) {
        Cache<String, PreparedStatement> cache = Caffeine.newBuilder().maximumSize(cacheSize).build();
        CaffeineCacheStats.registerCache(taggedMetricRegistry, cache,
                "query.async.prepared.statements.cache.metrics." + operation);
        return cache;
    }

    static Cache<String, PreparedStatement> createCache(int cacheSize) {
        return Caffeine.newBuilder().maximumSize(cacheSize).build();
    }

    // TODO (OStevan): prone to injection, fix this with some pattern match checking
    static String normalizeName(String keyspace, TableReference tableReference) {
        return keyspace + "." + tableReference.getQualifiedName();
    }


    public abstract static class StatementPreparation {

        /**
         * Main method to implement statement preparation.
         * @param operation for which the query is being creating
         * @param pattern pattern to used when creating this query
         * @param normalizedName of the table we are targeting (namespace.tableRef)
         * @return prepared statement
         */
        protected abstract PreparedStatement prepareStatement(String operation, String pattern, String normalizedName);


        /**
         * Simple health check query, reads the current time from the system table on cassandra cluster.
         *
         * @return statement to get data from a cluster
         */
        public final PreparedStatement prepareCurrentTimeStatement() {
            return prepareStatement(TIME, TIME_PATTERN, "system.local");
        }


        /**
         * Creates a prepared statement used in get requests. Qualifies the query such that all info is available for
         * orchestration on cassandra cluster as per TokenAwarePolicy for PreparedStatements.
         * <p>
         * https://docs.datastax.com/en/developer/java-driver/3.6/manual/load_balancing/#token-aware-policy
         *
         * @param keyspace where table of the data
         * @param tableReference of the table we are targeting
         * @return prepared statement for get request
         */
        public final PreparedStatement prepareGetStatement(String keyspace, TableReference tableReference) {
            return prepareStatement(GET, GET_PATTERN, normalizeName(keyspace, tableReference));
        }
    }


    public static final class CachePerOperationStatementPreparation extends StatementPreparation {

        // factory functions
        public static StatementPreparation create(Session session, TaggedMetricRegistry taggedMetricRegistry,
                CassandraKeyValueServiceConfig config) {
            throw new UnsupportedOperationException("Config parameters not defined for statement preparation cache");
        }

        public static CachePerOperationStatementPreparation create(Session session,
                TaggedMetricRegistry taggedMetricRegistry, int cacheSize) {
            return new CachePerOperationStatementPreparation(session,
                    SUPPORTED_OPERATIONS.stream().collect(Collectors.collectingAndThen(
                            Collectors.toMap(
                                    Function.identity(),
                                    operation -> {
                                        if (taggedMetricRegistry != null) {
                                            return createAndRegisterCache(taggedMetricRegistry, operation, cacheSize);
                                        } else {
                                            return createCache(cacheSize);
                                        }
                                    }
                            ),
                            ImmutableMap::copyOf
                            )
                    ));
        }

        // instance fields, constructor and methods
        private final ImmutableMap<String, Cache<String, PreparedStatement>> requestToCacheMap;
        private final Session session;

        private CachePerOperationStatementPreparation(
                Session session,
                ImmutableMap<String, Cache<String, PreparedStatement>> requestToCacheMap) {
            this.session = session;
            this.requestToCacheMap = requestToCacheMap;
        }

        protected PreparedStatement prepareStatement(String operation, String pattern, String normalizedName) {
            return requestToCacheMap.get(operation).get(normalizedName,
                    key -> session.prepare(String.format(pattern, key)));
        }
    }

    public static final class UnifiedCacheStatementPreparation extends StatementPreparation {

        private final Cache<String, PreparedStatement> cache;
        private final Session session;

        // factory functions
        public static UnifiedCacheStatementPreparation create(Session session,
                TaggedMetricRegistry taggedMetricRegistry, CassandraKeyValueServiceConfig config) {
            throw new UnsupportedOperationException("Config parameters not defined for statement preparation cache");
        }

        public static UnifiedCacheStatementPreparation create(Session session,
                TaggedMetricRegistry taggedMetricRegistry, int cacheSize) {
            return new UnifiedCacheStatementPreparation(session,
                    createAndRegisterCache(taggedMetricRegistry, ALL, cacheSize));
        }

        public static UnifiedCacheStatementPreparation create(Session session, int cacheSize) {
            return new UnifiedCacheStatementPreparation(session, createCache(cacheSize));
        }

        private UnifiedCacheStatementPreparation(Session session, Cache<String, PreparedStatement> cache) {
            this.session = session;
            this.cache = cache;
        }

        @Override
        protected PreparedStatement prepareStatement(String operation, String pattern, String normalizedName) {
            String cacheKey = operation + "." + normalizedName;
            return cache.get(cacheKey,
                    key -> session.prepare(String.format(pattern, normalizedName)));
        }
    }

}
