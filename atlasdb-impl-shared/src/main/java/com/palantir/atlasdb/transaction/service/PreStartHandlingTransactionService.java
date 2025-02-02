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

package com.palantir.atlasdb.transaction.service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.CheckForNull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

/**
 * This service handles queries for timestamps before {@link AtlasDbConstants#STARTING_TS}
 * as follows:
 *
 * - Gets of timestamps before {@link AtlasDbConstants#STARTING_TS} will return
 *   {@link AtlasDbConstants#STARTING_TS - 1}; in an AtlasDB context these correspond to
 *   deletion sentinels that are written non-transactionally and thus always committed.
 * - putUnlessExists to timestamps before {@link AtlasDbConstants#STARTING_TS} will throw an
 *   exception.
 *
 * Queries for legitimate timestamps are routed to the delegate.
 */
public class PreStartHandlingTransactionService implements TransactionService {
    private final TransactionService delegate;

    public PreStartHandlingTransactionService(TransactionService delegate) {
        this.delegate = delegate;
    }

    @CheckForNull
    @Override
    public Long get(long startTimestamp) {
        if (!isTimestampValid(startTimestamp)) {
            return AtlasDbConstants.STARTING_TS - 1;
        }
        return delegate.get(startTimestamp);
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        Map<Boolean, List<Long>> classifiedTimestamps = StreamSupport.stream(startTimestamps.spliterator(), false)
                .collect(Collectors.partitioningBy(PreStartHandlingTransactionService::isTimestampValid));

        Map<Long, Long> result = Maps.newHashMap();
        List<Long> validTimestamps = classifiedTimestamps.get(true);
        if (!validTimestamps.isEmpty()) {
            result.putAll(delegate.get(validTimestamps));
        }
        result.putAll(Maps.asMap(
                ImmutableSet.copyOf(classifiedTimestamps.get(false)), unused -> AtlasDbConstants.STARTING_TS - 1));
        return result;
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        if (!isTimestampValid(startTimestamp)) {
            throw new SafeIllegalStateException("Attempted to putUnlessExists({}, {}) which is disallowed.",
                    SafeArg.of("startTimestamp", startTimestamp),
                    SafeArg.of("commitTimestamp", commitTimestamp));
        }
        delegate.putUnlessExists(startTimestamp, commitTimestamp);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private static boolean isTimestampValid(Long startTimestamp) {
        return startTimestamp >= AtlasDbConstants.STARTING_TS;
    }
}
