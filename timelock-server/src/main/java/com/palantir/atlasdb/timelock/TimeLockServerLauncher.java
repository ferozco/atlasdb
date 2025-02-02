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
package com.palantir.atlasdb.timelock;

import java.util.UUID;
import java.util.function.Consumer;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.palantir.atlasdb.timelock.config.CombinedTimeLockServerConfiguration;
import com.palantir.atlasdb.timelock.logging.NonBlockingFileAppenderFactory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.server.jersey.ConjureJerseyFeature;
import com.palantir.timelock.paxos.TimeLockAgent;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;

import io.dropwizard.Application;
import io.dropwizard.jersey.optional.EmptyOptionalException;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

/**
 * Provides a way of launching an embedded TimeLock server using Dropwizard. Should only be used in tests.
 */
public class TimeLockServerLauncher extends Application<CombinedTimeLockServerConfiguration> {
    public static void main(String[] args) throws Exception {
        new TimeLockServerLauncher().run(args);
    }

    @Override
    public void initialize(Bootstrap<CombinedTimeLockServerConfiguration> bootstrap) {
        MetricRegistry metricRegistry = SharedMetricRegistries
                .getOrCreate("AtlasDbTest" + UUID.randomUUID().toString());
        bootstrap.setMetricRegistry(metricRegistry);
        bootstrap.getObjectMapper().registerSubtypes(NonBlockingFileAppenderFactory.class);
        bootstrap.getObjectMapper().registerModule(new Jdk8Module());
        super.initialize(bootstrap);
    }

    @Override
    public void run(CombinedTimeLockServerConfiguration configuration, Environment environment) {
        environment.getObjectMapper()
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule());
        environment.jersey().register(ConjureJerseyFeature.INSTANCE);
        environment.jersey().register(new EmptyOptionalTo204ExceptionMapper());

        MetricsManager metricsManager = MetricsManagers.of(environment.metrics(), new DefaultTaggedMetricRegistry());
        Consumer<Object> registrar = component -> environment.jersey().register(component);

        TimeLockAgent.create(
                metricsManager,
                configuration.install(),
                configuration::runtime, // this won't actually live reload
                CombinedTimeLockServerConfiguration.threadPoolSize(),
                CombinedTimeLockServerConfiguration.blockingTimeoutMs(),
                registrar);
    }

    @Provider
    private static final class EmptyOptionalTo204ExceptionMapper implements ExceptionMapper<EmptyOptionalException> {
        @Override
        public Response toResponse(EmptyOptionalException exception) {
            return Response.noContent().build();
        }
    }
}
