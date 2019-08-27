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
import com.datastax.driver.core.Session;
import com.github.benmanes.caffeine.cache.Cache;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class UnifiedStatementPreparation extends AbstractStatementPreparation {
    private final Cache<String, PreparedStatement> cache;
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final Session session;

    // factory functions
    public static UnifiedStatementPreparation create(Session session, TaggedMetricRegistry taggedMetricRegistry,
            CassandraKeyValueServiceConfig config) {
        throw new UnsupportedOperationException("Config parameters not defined for statement preparation cache");
    }

    public static UnifiedStatementPreparation create(Session session, TaggedMetricRegistry taggedMetricRegistry,
            int cacheSize) {
        return new UnifiedStatementPreparation(session,
                taggedMetricRegistry,
                createAndRegisterCache(taggedMetricRegistry, ALL, cacheSize));
    }

    public UnifiedStatementPreparation(Session session, TaggedMetricRegistry taggedMetricRegistry,
            Cache<String, PreparedStatement> cache) {
        this.session = session;
        this.taggedMetricRegistry = taggedMetricRegistry;
        this.cache = cache;
    }

    /**
     * Returns a dummy prepared statement used to get current time.
     */
    @Override
    public PreparedStatement prepareCurrentTimeStatement() {
        return session.prepare("SELECT dateof(now()) FROM system.local ;");
    }

    @Override
    protected PreparedStatement prepareStatement(String operation, String pattern, String normalizedName) {
        String cacheKey = operation + "." + normalizedName;
        return cache.get(cacheKey,
                key -> session.prepare(String.format(pattern, normalizedName)));
    }
}
