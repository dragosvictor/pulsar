/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.loadbalance.extensions.models;

import static org.apache.pulsar.broker.loadbalance.extensions.models.AssignCounter.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.AssignCounter.Label.Skip;
import static org.apache.pulsar.broker.loadbalance.extensions.models.AssignCounter.Label.Success;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.stats.Metrics;

/**
 * Defines Unload Metrics.
 */
public class AssignCounter {

    public static final AttributeKey<String> LOAD_BALANCER_ASSIGN_RESULT_KEY =
            AttributeKey.stringKey("pulsar.loadbalancer.extension.assign.result");
    private static final Attributes SUCCESS =
            Attributes.of(LOAD_BALANCER_ASSIGN_RESULT_KEY, Success.name().toLowerCase());
    private static final Attributes FAILURE =
            Attributes.of(LOAD_BALANCER_ASSIGN_RESULT_KEY, Failure.name().toLowerCase());
    private static final Attributes SKIP = Attributes.of(LOAD_BALANCER_ASSIGN_RESULT_KEY, Skip.name().toLowerCase());
    private final LongCounter assignCounter;

    enum Label {
        Success,
        Failure,
        Skip,
    }

    final Map<Label, AtomicLong> breakdownCounters;

    public AssignCounter(PulsarService pulsarService) {
        breakdownCounters = Map.of(
                Success, new AtomicLong(),
                Failure, new AtomicLong(),
                Skip, new AtomicLong()
        );
        assignCounter = pulsarService.getOpenTelemetry().getMeter()
                .counterBuilder("pulsar.loadbalancer.assign")
                .build();
    }


    public void incrementSuccess() {
        breakdownCounters.get(Success).incrementAndGet();
        assignCounter.add(1, SUCCESS);
    }

    public void incrementFailure() {
        breakdownCounters.get(Failure).incrementAndGet();
        assignCounter.add(1, FAILURE);
    }

    public void incrementSkip() {
        breakdownCounters.get(Skip).incrementAndGet();
        assignCounter.add(1, SKIP);
    }

    public List<Metrics> toMetrics(String advertisedBrokerAddress) {
        var metrics = new ArrayList<Metrics>();
        var dimensions = new HashMap<String, String>();
        dimensions.put("metric", "assign");
        dimensions.put("broker", advertisedBrokerAddress);

        for (var etr : breakdownCounters.entrySet()) {
            var label = etr.getKey();
            var count = etr.getValue().get();
            var breakdownDims = new HashMap<>(dimensions);
            breakdownDims.put("result", label.toString());
            var breakdownMetric = Metrics.create(breakdownDims);
            breakdownMetric.put("brk_lb_assign_broker_breakdown_total", count);
            metrics.add(breakdownMetric);
        }

        return metrics;
    }
}