/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.services.test;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cache.DefaultTimestampCache;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.services.ServicesConfig;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.timestamp.TimestampService;

import dagger.Module;
import dagger.Provides;

@Module
public class TestTransactionManagerModule {

    @Provides
    @Singleton
    public LockClient provideLockClient() {
        return LockClient.of("atlas instance");
    }

    @Provides
    @Singleton
    public Follower provideCleanupFollower(ServicesConfig atlasDbConfig) {
        return CleanupFollower.create(atlasDbConfig.schemas());
    }

    @Provides
    @Singleton
    public Cleaner provideCleaner(ServicesConfig config,
                                  @Named("kvs") KeyValueService kvs,
                                  LockService lock,
                                  TimestampService tss,
                                  LockClient lockClient,
                                  Follower follower,
                                  TransactionService transactionService) {
        AtlasDbConfig atlasDbConfig = config.atlasDbConfig();
        return new DefaultCleanerBuilder(
                kvs,
                lock,
                tss,
                lockClient,
                ImmutableList.of(follower),
                transactionService)
                .setBackgroundScrubAggressively(atlasDbConfig.backgroundScrubAggressively())
                .setBackgroundScrubBatchSize(atlasDbConfig.getBackgroundScrubBatchSize())
                .setBackgroundScrubFrequencyMillis(atlasDbConfig.getBackgroundScrubFrequencyMillis())
                .setBackgroundScrubThreads(atlasDbConfig.getBackgroundScrubThreads())
                .setPunchIntervalMillis(atlasDbConfig.getPunchIntervalMillis())
                .setTransactionReadTimeout(atlasDbConfig.getTransactionReadTimeoutMillis())
                .setInitializeAsync(atlasDbConfig.initializeAsync())
                .buildCleaner();
    }

    @Provides
    @Singleton
    public SerializableTransactionManager provideTransactionManager(MetricsManager metricsManager,
                                                                    ServicesConfig config,
                                                                    @Named("kvs") KeyValueService kvs,
                                                                    TransactionManagers.LockAndTimestampServices lts,
                                                                    LockClient lockClient,
                                                                    TransactionService transactionService,
                                                                    ConflictDetectionManager conflictManager,
                                                                    SweepStrategyManager sweepStrategyManager,
                                                                    Cleaner cleaner) {
        return new SerializableTransactionManager(
                metricsManager,
                kvs,
                lts.timelock(),
                lts.timestampManagement(),
                lts.lock(),
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictManager,
                sweepStrategyManager,
                cleaner,
                new DefaultTimestampCache(
                        metricsManager.getRegistry(), () -> config.atlasDbRuntimeConfig().getTimestampCacheSize()),
                config.allowAccessToHiddenTables(),
                config.atlasDbConfig().keyValueService().concurrentGetRangesThreadPoolSize(),
                config.atlasDbConfig().keyValueService().defaultGetRangesConcurrency(),
                MultiTableSweepQueueWriter.NO_OP,
                PTExecutors.newSingleThreadExecutor(true),
                true,
                () -> config.atlasDbRuntimeConfig().transaction());
    }

}
