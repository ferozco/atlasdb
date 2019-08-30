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
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class PerOperationStatementPreparation extends AbstractStatementPreparation {

    // factory functions
    public static StatementPreparation create(Session session, TaggedMetricRegistry taggedMetricRegistry,
            CassandraKeyValueServiceConfig config) {
        throw new UnsupportedOperationException("Config parameters not defined for statement preparation cache");
    }

    public static PerOperationStatementPreparation create(Session session, TaggedMetricRegistry taggedMetricRegistry,
            int cacheSize) {
        return new PerOperationStatementPreparation(session,
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

    private PerOperationStatementPreparation(
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
