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

import com.datastax.driver.core.PreparedStatement;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.tritium.metrics.caffeine.CaffeineCacheStats;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public abstract class AbstractStatementPreparation implements StatementPreparation {
    protected static final String ALL = "all";
    protected static final String GET = "get";
    protected static final String GET_PATTERN =
            "SELECT * FROM %s "
                    + "WHERE " + FieldNameProvider.row + " =:" + FieldNameProvider.row
                    + " AND " + FieldNameProvider.column + " =:" + FieldNameProvider.column
                    + " AND " + FieldNameProvider.timestamp + " >:" + FieldNameProvider.timestamp + " ;";

    protected static final ImmutableSet<String> SUPPORTED_OPERATIONS = ImmutableSet.of(ALL, GET);

    protected static Cache<String, PreparedStatement> createAndRegisterCache(TaggedMetricRegistry taggedMetricRegistry,
            String operation, int cacheSize) {
        Cache<String, PreparedStatement> cache = Caffeine.newBuilder().maximumSize(cacheSize).build();
        CaffeineCacheStats.registerCache(taggedMetricRegistry, cache,
                "query.async.prepared.statements.cache.metrics." + operation);
        return cache;
    }

    // TODO (OStevan): prone to injection, fix this with some pattern match checking
    protected static String normalizeName(String keyspace, TableReference tableReference) {
        return keyspace + "." + tableReference.getQualifiedName();
    }

    protected abstract PreparedStatement prepareStatement(String operation, String pattern, String normalizedName);


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
