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

package com.palantir.atlasdb.timelock.paxos;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.SetMultimap;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.PaxosValue;

@Path("/" + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE
        + "/learner")
public class BatchPaxosLearnerResource implements BatchPaxosLearner {

    private final PaxosComponents paxosComponents;

    public BatchPaxosLearnerResource(PaxosComponents paxosComponents) {
        this.paxosComponents = paxosComponents;
    }

    @POST
    @Path("learn")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public void learn(SetMultimap<Client, PaxosValue> paxosValuesByClient) {
        paxosValuesByClient.forEach(
                (client, paxosValue) -> paxosComponents.learner(client).learn(paxosValue.getRound(), paxosValue));
    }

    @POST
    @Path("learned-values")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public SetMultimap<Client, PaxosValue> getLearnedValues(Set<WithSeq<Client>> clientAndSeqs) {
        return KeyedStream.of(clientAndSeqs)
                .mapKeys(WithSeq::value)
                .map(WithSeq::seq)
                .map((client, seq) -> paxosComponents.learner(client).getLearnedValue(seq))
                .filter(Objects::nonNull)
                .collectToSetMultimap();
    }

    @POST
    @Path("learned-values-since")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public SetMultimap<Client, PaxosValue> getLearnedValuesSince(Map<Client, Long> seqLowerBoundsByClient) {
        return KeyedStream.stream(seqLowerBoundsByClient)
                .map((client, seq) -> paxosComponents.learner(client).getLearnedValuesSince(seq))
                .flatMap(Collection::stream)
                .collectToSetMultimap();
    }

}
