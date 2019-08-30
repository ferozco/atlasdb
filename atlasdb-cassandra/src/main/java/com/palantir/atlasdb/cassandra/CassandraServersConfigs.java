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

package com.palantir.atlasdb.cassandra;

import java.net.InetSocketAddress;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableSet;

public class CassandraServersConfigs {

    @JsonDeserialize(as = DeprecatedCassandraServersConfig.class)
    @JsonTypeName(DeprecatedCassandraServersConfig.TYPE)
    public static class DeprecatedCassandraServersConfig implements CassandraServersConfig {
        public static final String TYPE = "deprecated";

        private Set<InetSocketAddress> thriftServers;

        public DeprecatedCassandraServersConfig(InetSocketAddress thriftAddress) {
            this.thriftServers = ImmutableSet.of(thriftAddress);
        }

        public DeprecatedCassandraServersConfig(InetSocketAddress... thriftServers) {
            this.thriftServers = ImmutableSet.copyOf(thriftServers);
        }

        @JsonCreator
        public DeprecatedCassandraServersConfig(Iterable<InetSocketAddress> thriftServers) {
            this.thriftServers = ImmutableSet.copyOf(thriftServers);
        }

        @Override
        public Set<InetSocketAddress> thrift() {
            return thriftServers;
        }

        @Override
        public Set<InetSocketAddress> cql() {
            return ImmutableSet.of();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof DeprecatedCassandraServersConfig) {
                return thriftServers.equals(((DeprecatedCassandraServersConfig) obj).thriftServers);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }
}
