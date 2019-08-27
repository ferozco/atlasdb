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

import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.tritium.metrics.caffeine.CaffeineCacheStats;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class StatementPreparationImpl implements StatementPreparation {
    // currently supported operations and query patterns for each operation
    /**
     * The prepared statement is for a single row instead of a more general in statement (implemented like this to help
     * the token aware policy, might be wrong solution to have this implementation specific).
     */
    // TODO (OStevan): possibly wrong and should verify this more in docs and find a solution for this,
    //  might be worthwhile to simplify this and make it more understandable. Also there is probably a better way of
    //  constructing this string
    private static final String GET = "get";
    private static final String GET_PATTERN =
            "SELECT * FROM %s "
                    + "WHERE " + FieldNameProvider.row + " =:" + FieldNameProvider.row
                    + " AND " + FieldNameProvider.column + " =:" + FieldNameProvider.column
                    + " AND " + FieldNameProvider.timestamp + " >:" + FieldNameProvider.timestamp + " ;";

    // all supported operations
    private static final ImmutableSet<String> SUPPORTED_OPERATIONS = ImmutableSet.of(GET);

    private static Cache<String, PreparedStatement> createAndRegisterCache(TaggedMetricRegistry taggedMetricRegistry,
            String operation, int cacheSize) {
        Cache<String, PreparedStatement> cache = Caffeine.newBuilder().maximumSize(cacheSize).build();
        CaffeineCacheStats.registerCache(taggedMetricRegistry, cache,
                "query.async.queries.cache.metrics." + operation);
        return cache;
    }

    // TODO (OStevan): prone to injection, fix this with some pattern match checking
    private static String normalizeName(String keyspace, TableReference tableReference) {
        return keyspace + "." + tableReference.getQualifiedName();
    }

    // factory functions
    public static StatementPreparation create(Session session, TaggedMetricRegistry taggedMetricRegistry,
            CassandraKeyValueServiceConfig config) {
        throw new UnsupportedOperationException("Config parameters not defined for statement preparation cache");
    }

    public static StatementPreparationImpl create(Session session, TaggedMetricRegistry taggedMetricRegistry,
            int cacheSize) {
        return new StatementPreparationImpl(session,
                taggedMetricRegistry,
                SUPPORTED_OPERATIONS.stream().collect(Collectors.collectingAndThen(
                        Collectors.toMap(
                                Functions.identity(),
                                operation -> createAndRegisterCache(taggedMetricRegistry, operation, cacheSize)
                        ),
                        ImmutableMap::copyOf
                        )
                ));
    }

    // instance fields, constructor and methods
    private final ImmutableMap<String, Cache<String, PreparedStatement>> requestToCacheMap;
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final Session session;

    private StatementPreparationImpl(
            Session session,
            TaggedMetricRegistry taggedMetricRegistry,
            ImmutableMap<String, Cache<String, PreparedStatement>> requestToCacheMap) {
        this.session = session;
        this.taggedMetricRegistry = taggedMetricRegistry;
        this.requestToCacheMap = requestToCacheMap;
    }

    private PreparedStatement prepareStatement(String operation, String pattern, String normalizedName) {
        return requestToCacheMap.get(operation).get(normalizedName,
                key -> session.prepare(String.format(pattern, key)));
    }

    /**
     * Returns a dummy prepared statement used to get current time.
     */
    @Override
    public PreparedStatement prepareCurrentTimeStatement() {
        return session.prepare("SELECT dateof(now()) FROM system.local ;");
    }

    /**
     * Prepares a statement for a given table in a specific keyspace.
     *
     * @param keyspace of the table
     * @param tableReference of the table where data is stored
     * @return prepared statement for further use
     */
    @Override
    public PreparedStatement prepareGetStatement(String keyspace, TableReference tableReference) {
        return prepareStatement(GET, GET_PATTERN, normalizeName(keyspace, tableReference));
    }
}
