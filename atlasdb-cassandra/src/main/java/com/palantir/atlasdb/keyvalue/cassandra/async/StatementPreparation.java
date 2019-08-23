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
import com.palantir.atlasdb.keyvalue.api.TableReference;

public interface StatementPreparation {

    // TODO (OStevan): check if this already exists somewhere
    class FieldNameProvider {
        public static String ROW = "key";
        public static String COLUMN = "column1";
        public static String TIMESTAMP = "column2";
        public static String VALUE = "value";
    }

    PreparedStatement prepareCurrentTimeStatement();

    /**
     * Creates a prepared statement used in get requests qualifying the query such that all info available for query
     * orchestration on cassandra cluster as per TokenAwarePolicy for PreparedStatements at
     * https://docs.datastax.com/en/developer/java-driver/3.6/manual/load_balancing/#token-aware-policy
     * @param keyspace where table of the data
     * @param tableReference of the table we are targeting
     * @return prepared statement for get request
     */
    PreparedStatement prepareGetStatement(String keyspace, TableReference tableReference);
}
