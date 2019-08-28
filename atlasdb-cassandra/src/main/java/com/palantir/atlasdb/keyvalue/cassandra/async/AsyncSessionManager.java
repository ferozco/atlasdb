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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.ThreadingOptions;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class AsyncSessionManager {
    // helper interfaces, automatically generated
    @Value.Immutable
    public interface UniqueCassandraCluster {
        @Value.Parameter
        Set<InetSocketAddress> servers();
    }

    // class fields and methods
    private static final Logger log = LoggerFactory.getLogger(AsyncSessionManager.class);
    private static final AtomicReference<AsyncSessionManager> FACTORY = new AtomicReference<>(null);

    public static void initialize(TaggedMetricRegistry taggedMetricRegistry) {
        Preconditions.checkState(
                null == FACTORY.getAndUpdate(
                        previous ->
                                previous == null ? new AsyncSessionManager(taggedMetricRegistry) : previous
                ),
                "Already initialized");
    }

    public static AsyncSessionManager getAsyncSessionFactory() {
        AsyncSessionManager factory = FACTORY.get();
        Preconditions.checkState(factory != null, "AsyncSessionManager is not initialized");
        return factory;
    }


    // instance fields and methods
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final Cache<UniqueCassandraCluster, Cluster> clusters = Caffeine.newBuilder().build();
    private final AtomicLong cassandraId = new AtomicLong();
    // global for all sessions
    private final AtomicLong sessionId = new AtomicLong();

    private AsyncSessionManager(TaggedMetricRegistry taggedMetricRegistry) {
        this.taggedMetricRegistry = taggedMetricRegistry;
    }

    // TODO (OStevan): probably very much semantically incorrect, might make sense to call it
    //  CassandraClusterConnectionConfig
    public AsyncClusterSession getSession(CassandraKeyValueServiceConfig config)
            throws ExecutionException, InterruptedException {
        return createSession(Objects.requireNonNull(clusters.get(ImmutableUniqueCassandraCluster.of(config.servers()),
                uniqueCassandraCluster -> createCluster(config)
        ))).get();
    }


    private Cluster createCluster(CassandraKeyValueServiceConfig config) {
        long curId = cassandraId.getAndIncrement();
        String clusterName = "cassandra" + (curId == 0 ? "" : "-" + curId);

        log.info("Creating cluster {}", SafeArg.of("clusterId", clusterName));

        Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPointsWithPorts(contactPoints(config))
                .withClusterName(clusterName) // for JMX Metrics
                .withCredentials(config.credentials().username(), config.credentials().password())
                .withCompression(ProtocolOptions.Compression.LZ4)
                .withLoadBalancingPolicy(loadBalancingPolicy(config))
                .withPoolingOptions(poolingOptions(config))
                .withQueryOptions(queryOptions(config))
                .withRetryPolicy(retryPolicy(config))
                .withSSL(sslOptions(config))
                .withThreadingOptions(new ThreadingOptions());


        return buildCluster(clusterBuilder);
    }

    private ListenableFuture<AsyncClusterSession> createSession(Cluster cluster) {
        long curId = sessionId.getAndIncrement();
        String sessionName = cluster.getClusterName() + "-session" + (curId == 0 ? "" : "-" + curId);

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(sessionName + "-%d")
                .build();

        return Futures.transform(cluster.connectAsync(),
                session -> AsyncClusterSessionImpl.create(sessionName, session, taggedMetricRegistry, threadFactory),
                MoreExecutors.directExecutor());
    }

    private static Collection<InetSocketAddress> contactPoints(CassandraKeyValueServiceConfig config) {
        return config.servers().stream().map(
                address -> new InetSocketAddress("localhost", 9042)).collect(
                Collectors.toList());
    }

    private static SSLOptions sslOptions(CassandraKeyValueServiceConfig config) {
        return config.sslConfiguration().map(
                sslConfiguration -> {
                    SSLContext sslContext = SslSocketFactories.createSslContext(sslConfiguration);
                    return RemoteEndpointAwareJdkSSLOptions.builder()
                            .withSSLContext(sslContext)
                            .build();
                }).orElse(
                config.ssl().map(option -> {
                    if (option) {
                        return RemoteEndpointAwareJdkSSLOptions.builder().build();
                    } else {
                        return null;
                    }
                }).orElse(null)
        );
    }

    private static PoolingOptions poolingOptions(CassandraKeyValueServiceConfig config) {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, config.poolSize());
        poolingOptions.setMaxRequestsPerConnection(HostDistance.REMOTE, config.poolSize());
        poolingOptions.setPoolTimeoutMillis(config.cqlPoolTimeoutMillis());
        return poolingOptions;
    }

    private static QueryOptions queryOptions(CassandraKeyValueServiceConfig config) {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setFetchSize(config.fetchBatchCount());
        return queryOptions;
    }

    private static LoadBalancingPolicy loadBalancingPolicy(CassandraKeyValueServiceConfig config) {
        // Refuse to talk to nodes twice as (latency-wise) slow as the best one, over a timescale of 100ms,
        // and every 10s try to re-evaluate ignored nodes performance by giving them queries again.
        // Note we are being purposely datacenter-irreverent here, instead relying on latency alone
        // to approximate what DCAwareRR would do;
        // this is because DCs for Atlas are always quite latency-close and should be used this way,
        // not as if we have some cross-country backup DC.
        LoadBalancingPolicy policy = LatencyAwarePolicy.builder(new RoundRobinPolicy()).build();

        // If user wants, do not automatically add in new nodes to pool (useful during DC migrations / rebuilds)
        if (!config.autoRefreshNodes()) {
            policy = new WhiteListPolicy(policy, config.servers());
        }

        // also try and select coordinators who own the data we're talking about to avoid an extra hop,
        // but also shuffle which replica we talk to for a load balancing that comes at the expense
        // of less effective caching
        return new TokenAwarePolicy(policy, TokenAwarePolicy.ReplicaOrdering.RANDOM);
    }

    private static RetryPolicy retryPolicy(CassandraKeyValueServiceConfig config) {
        return DefaultRetryPolicy.INSTANCE;
    }

    private static Cluster buildCluster(Cluster.Builder clusterBuilder) {
        Cluster cluster;
        //        Metadata metadata;
        try {
            cluster = clusterBuilder.build();
            //            metadata = cluster.getMetadata(); // special; this is the first place we connect to
            // hosts, this is where people will see failures
        } catch (NoHostAvailableException e) {
            if (e.getMessage().contains("Unknown compression algorithm")) {
                clusterBuilder.withCompression(ProtocolOptions.Compression.NONE);
                cluster = clusterBuilder.build();
                //                metadata = cluster.getMetadata();
            } else {
                throw e;
            }
        } catch (IllegalStateException e) {
            // god dammit datastax what did I do to _you_
            if (e.getMessage().contains("requested compression is not available")) {
                clusterBuilder.withCompression(ProtocolOptions.Compression.NONE);
                cluster = clusterBuilder.build();
                //                metadata = cluster.getMetadata();
            } else {
                throw e;
            }
        }
        return cluster;
    }
}
