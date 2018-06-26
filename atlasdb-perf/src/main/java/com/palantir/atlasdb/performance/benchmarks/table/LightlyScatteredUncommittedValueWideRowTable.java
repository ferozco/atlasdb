/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.performance.benchmarks.table;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import com.palantir.atlasdb.keyvalue.api.TableReference;

@State(Scope.Benchmark)
public class LightlyScatteredUncommittedValueWideRowTable extends UncommittedValueWideRowTable {
    @Override
    public TableReference getTableRef() {
        return Tables.TABLE_REF;
    }

    @Override
    public long numColumnsWithoutUncommittedValues() {
        return 49000;
    }

    @Override
    public long numColumnsWithUncommittedValues() {
        return 1000;
    }

    @Override
    public long numUncommittedValuesPerColumn() {
        return 1;
    }

    @Override
    public boolean isPersistent() {
        return false;
    }
}
