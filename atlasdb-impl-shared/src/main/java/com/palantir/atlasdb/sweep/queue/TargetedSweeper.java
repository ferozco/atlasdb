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
package com.palantir.atlasdb.sweep.queue;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.BackgroundSweeper;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.metrics.SweepOutcome;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.config.ImmutableTargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.config.ImmutableTargetedSweepRuntimeConfig;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepRuntimeConfig;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.exception.NotInitializedException;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;

@SuppressWarnings({"FinalClass", "Not final for mocking in tests"})
public class TargetedSweeper implements MultiTableSweepQueueWriter, BackgroundSweeper {
    private static final Logger log = LoggerFactory.getLogger(TargetedSweeper.class);
    private static final Duration MAX_SHARD_DURATION = Duration.ofMinutes(5L);

    private final Supplier<TargetedSweepRuntimeConfig> runtime;
    private final List<Follower> followers;
    private final MetricsManager metricsManager;

    private TargetedSweepMetrics metrics;
    private SweepQueue queue;
    private SpecialTimestampsSupplier timestampsSupplier;
    private TimelockService timeLock;
    private BackgroundSweepScheduler conservativeScheduler;
    private BackgroundSweepScheduler thoroughScheduler;

    private volatile boolean isInitialized = false;

    private TargetedSweeper(MetricsManager metricsManager,
            Supplier<TargetedSweepRuntimeConfig> runtime,
            TargetedSweepInstallConfig install,
            List<Follower> followers) {
        this.metricsManager = metricsManager;
        this.runtime = runtime;
        this.conservativeScheduler = new BackgroundSweepScheduler(install.conservativeThreads(),
                TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
        this.thoroughScheduler = new BackgroundSweepScheduler(install.thoroughThreads(),
                TableMetadataPersistence.SweepStrategy.THOROUGH);
        this.followers = followers;
    }

    /**
     * Creates a targeted sweeper, without initializing any of the necessary resources. You must call the
     * {@link #initializeWithoutRunning(SpecialTimestampsSupplier, TimelockService, KeyValueService,
     * TransactionService, TargetedSweepFollower)} method before any writes can be made to the sweep queue, or before
     * the background sweep job can run.
     *
     * @param runtime live reloadable TargetedSweepRuntimeConfig.
     * @param install TargetedSweepInstallConfig, which is not live reloadable.
     * @param followers follower used for sweeps, as defined by your schema.
     * @return returns an uninitialized targeted sweeper
     */
    public static TargetedSweeper createUninitialized(MetricsManager metrics,
            Supplier<TargetedSweepRuntimeConfig> runtime,
            TargetedSweepInstallConfig install,
            List<Follower> followers) {
        return new TargetedSweeper(metrics, runtime, install, followers);
    }

    public static TargetedSweeper createUninitializedForTest(MetricsManager metricsManager,
            Supplier<TargetedSweepRuntimeConfig> runtime) {
        TargetedSweepInstallConfig install = ImmutableTargetedSweepInstallConfig.builder()
                .conservativeThreads(0)
                .thoroughThreads(0)
                .build();
        return createUninitialized(metricsManager, runtime, install, ImmutableList.of());
    }

    public static TargetedSweeper createUninitializedForTest(Supplier<Integer> shards) {
        Supplier<TargetedSweepRuntimeConfig> runtime = () ->
                ImmutableTargetedSweepRuntimeConfig.builder()
                        .shards(shards.get())
                        .build();
        return createUninitializedForTest(MetricsManagers.createForTests(), runtime);
    }

    @Override
    public void initialize(TransactionManager txManager) {
        initializeWithoutRunning(txManager);
        runInBackground();
    }

    public void initializeWithoutRunning(TransactionManager txManager) {
        initializeWithoutRunning(SpecialTimestampsSupplier.create(txManager),
                txManager.getTimelockService(),
                txManager.getKeyValueService(),
                txManager.getTransactionService(),
                new TargetedSweepFollower(followers, txManager));
    }

    /**
     * This method initializes all the resources necessary for the targeted sweeper. This method should only be called
     * once the kvs is ready.
     * @param timestamps supplier of unreadable and immutable timestamps.
     * @param timelockService TimeLockService to use for synchronizing iterations of sweep on different nodes
     * @param kvs key value service that must be already initialized.
     * @param transaction transaction service for checking if values were committed and rolling back if necessary
     * @param follower followers used for sweeps.
     */
    public void initializeWithoutRunning(
            SpecialTimestampsSupplier timestamps,
            TimelockService timelockService,
            KeyValueService kvs,
            TransactionService transaction,
            TargetedSweepFollower follower) {
        if (isInitialized) {
            return;
        }
        Preconditions.checkState(kvs.isInitialized(),
                "Attempted to initialize targeted sweeper with an uninitialized backing KVS.");
        metrics = TargetedSweepMetrics.create(metricsManager, timelockService, kvs, SweepQueueUtils.REFRESH_TIME);
        queue = SweepQueue.create(
                metrics,
                kvs,
                timelockService,
                Suppliers.compose(TargetedSweepRuntimeConfig::shards, runtime::get),
                transaction,
                follower,
                () -> runtime.get().maximumPartitionsToBatchInSingleRead());
        timestampsSupplier = timestamps;
        timeLock = timelockService;
        isInitialized = true;
    }

    @Override
    public void runInBackground() {
        assertInitialized();
        conservativeScheduler.scheduleBackgroundThreads();
        thoroughScheduler.scheduleBackgroundThreads();
    }

    @Override
    public void enqueue(List<WriteInfo> writes) {
        assertInitialized();
        queue.enqueue(writes);
    }

    /**
     * Sweeps the next batch for the given shard and strategy. If the sweep is successful, we delete the processed
     * writes from the sweep queue and then update the sweep queue progress accordingly.
     *
     * @param shardStrategy shard and strategy to use
     * @return true if we should immediately process another batch for this shard and strategy
     */
    @SuppressWarnings("checkstyle:RegexpMultiline") // Suppress VisibleForTesting warning
    @VisibleForTesting
    public boolean sweepNextBatch(ShardAndStrategy shardStrategy, long maxTsExclusive) {
        assertInitialized();
        return queue.sweepNextBatch(shardStrategy, maxTsExclusive);
    }

    @VisibleForTesting
    void processShard(ShardAndStrategy shardAndStrategy) {
        long maxTsExclusive = Sweeper.of(shardAndStrategy).getSweepTimestamp(timestampsSupplier);
        if (runtime.get().batchShardIterations()) {
            Stopwatch watch = Stopwatch.createStarted();
            boolean processNextBatch = true;
            while (processNextBatch && runtime.get().enabled()
                    && (watch.elapsed().compareTo(MAX_SHARD_DURATION) < 0)) {
                processNextBatch = sweepNextBatch(shardAndStrategy, maxTsExclusive);
            }
        } else {
            sweepNextBatch(shardAndStrategy, maxTsExclusive);
        }
    }

    @Override
    public void close() {
        conservativeScheduler.close();
        thoroughScheduler.close();
    }

    @Override
    public void shutdown() {
        close();
    }

    private void assertInitialized() {
        if (!isInitialized) {
            throw new NotInitializedException("Targeted Sweeper");
        }
    }

    private class BackgroundSweepScheduler implements AutoCloseable {
        private final int numThreads;
        private final TableMetadataPersistence.SweepStrategy sweepStrategy;
        private final AtomicLong counter = new AtomicLong(0);

        private ScheduledExecutorService executorService;

        private BackgroundSweepScheduler(int numThreads, TableMetadataPersistence.SweepStrategy sweepStrategy) {
            this.numThreads = numThreads;
            this.sweepStrategy = sweepStrategy;
        }

        private void scheduleBackgroundThreads() {
            if (numThreads > 0 && executorService == null) {
                executorService = PTExecutors
                        .newScheduledThreadPoolExecutor(numThreads, new NamedThreadFactory("Targeted Sweep", true));
                for (int i = 0; i < numThreads; i++) {
                    executorService.scheduleWithFixedDelay(this::runOneIteration, 1000,
                            Math.max(runtime.get().pauseMillis(), 1L), TimeUnit.MILLISECONDS);
                }
            }
        }

        private void runOneIteration() {
            if (!runtime.get().enabled()) {
                metrics.registerOccurrenceOf(sweepStrategy, SweepOutcome.DISABLED);
                return;
            }

            Optional<TargetedSweeperLock> maybeLock = Optional.empty();
            try {
                maybeLock = tryToAcquireLockForNextShardAndStrategy();
                maybeLock.ifPresent(lock -> processShard(lock.getShardAndStrategy()));
            } catch (InsufficientConsistencyException e) {
                metrics.registerOccurrenceOf(sweepStrategy, SweepOutcome.NOT_ENOUGH_DB_NODES_ONLINE);
                logException(e, maybeLock);
            } catch (Throwable th) {
                metrics.registerOccurrenceOf(sweepStrategy, SweepOutcome.ERROR);
                logException(th, maybeLock);
            } finally {
                try {
                    maybeLock.ifPresent(TargetedSweeperLock::unlock);
                } catch (Throwable th) {
                    logUnlockException(th, maybeLock);
                }
            }
        }

        private Optional<TargetedSweeperLock> tryToAcquireLockForNextShardAndStrategy() {
            return IntStream.range(0, queue.getNumShards())
                    .map(ignore -> getShardAndIncrement())
                    .mapToObj(shard -> TargetedSweeperLock.tryAcquire(shard, sweepStrategy, timeLock))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .findFirst();
        }

        private int getShardAndIncrement() {
            return (int) (counter.getAndIncrement() % queue.getNumShards());
        }

        private void logException(Throwable th, Optional<TargetedSweeperLock> maybeLock) {
            if (maybeLock.isPresent()) {
                log.warn("Targeted sweep for {} failed and will be retried later.",
                        SafeArg.of("shardStrategy", maybeLock.get().getShardAndStrategy().toText()), th);
            } else {
                log.warn("Targeted sweep for sweep strategy {} failed and will be retried later.",
                        SafeArg.of("sweepStrategy", sweepStrategy), th);
            }
        }

        private void logUnlockException(Throwable th, Optional<TargetedSweeperLock> maybeLock) {
            if (maybeLock.isPresent()) {
                log.info("Failed to unlock targeted sweep lock for {}.",
                        SafeArg.of("shardStrategy", maybeLock.get().getShardAndStrategy().toText()), th);
            } else {
                log.info("Failed to unlock targeted sweep lock for sweep strategy {}.",
                        SafeArg.of("sweepStrategy", sweepStrategy), th);
            }
        }

        @Override
        public void close() {
            if (executorService != null) {
                executorService.shutdown();
            }
        }
    }
}
