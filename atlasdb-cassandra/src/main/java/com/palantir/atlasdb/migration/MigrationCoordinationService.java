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

package com.palantir.atlasdb.migration;

import com.palantir.atlasdb.keyvalue.api.TableReference;

public interface MigrationCoordinationService {
    boolean startMigration(TableReference startTable, TableReference targetTable);
    boolean endMigration(TableReference startTable);
    boolean endDualWrite(TableReference startTable);
    TableMigrationState getMigrationState(TableReference startTable, long timestamp);

    enum MigrationState {
        WRITE_FIRST_ONLY,
        WRITE_BOTH_READ_FIRST,
        WRITE_BOTH_READ_SECOND,
        WRITE_SECOND_READ_SECOND
    }
}
