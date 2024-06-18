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

import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Skip;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Success;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Admin;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.CoolDown;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.HitCount;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBrokers;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBundles;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoLoadData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.OutDatedData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Overloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Underloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Unknown;
import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.stats.Metrics;

/**
 * Defines Unload Metrics.
 */
public class UnloadCounter implements AutoCloseable {

    public static final AttributeKey<String> LOAD_BALANCER_UNLOAD_DECISION_KEY =
            AttributeKey.stringKey("pulsar.loadbalancer.extension.unload.decision");
    public static final AttributeKey<String> LOAD_BALANCER_UNLOAD_REASON_KEY =
            AttributeKey.stringKey("pulsar.loadbalancer.extension.unload.reason");
    private final LongCounter unloadCounter;
    private final LongCounter unloadBrokerCounter;

    private final ObservableDoubleGauge avgLoadGauge;
    private final ObservableDoubleGauge stdLoadGauge;

    private long unloadBrokerCount = 0;
    private long unloadBundleCount = 0;

    @Getter
    @VisibleForTesting
    private final Map<UnloadDecision.Label, Map<UnloadDecision.Reason, AtomicLong>> breakdownCounters;

    @Getter
    @VisibleForTesting
    private double loadAvg;
    @Getter
    @VisibleForTesting
    private double loadStd;

    private volatile long updatedAt = 0;

    public UnloadCounter(PulsarService pulsarService) {
        breakdownCounters = Map.of(
                Success, Map.of(
                        Overloaded, new AtomicLong(),
                        Underloaded, new AtomicLong(),
                        Admin, new AtomicLong()),
                Skip, Map.of(
                        HitCount, new AtomicLong(),
                        NoBundles, new AtomicLong(),
                        CoolDown, new AtomicLong(),
                        OutDatedData, new AtomicLong(),
                        NoLoadData, new AtomicLong(),
                        NoBrokers, new AtomicLong(),
                        Unknown, new AtomicLong()),
                Failure, Map.of(
                        Unknown, new AtomicLong())
        );

        var meter = pulsarService.getOpenTelemetry().getMeter();
        unloadCounter = meter
                .counterBuilder("pulsar.loadbalancer.extensible.unloads")
                .build();
        unloadBrokerCounter = meter
                .counterBuilder("pulsar.loadbalancer.extensible.unload_brokers")
                .setDescription("Total number of brokers that have been unloaded")
                .setUnit("{brokers}")
                .build();
        avgLoadGauge = meter.gaugeBuilder("pulsar.loadbalancer.extensible.load.avg")
                .setDescription(
                        "Average load of the brokers in the cluster, as calculated by the Extensible Load Balancer")
                .setUnit("1")
                .buildWithCallback(observableDoubleMeasurement -> observableDoubleMeasurement.record(loadAvg));
        stdLoadGauge = meter.gaugeBuilder("pulsar.loadbalancer.extensible.load.std")
                .setDescription("Standard deviation of the load of the brokers in the cluster, "
                        + "as calculated by the Extensible Load Balancer")
                .setUnit("1")
                .buildWithCallback(observableDoubleMeasurement -> observableDoubleMeasurement.record(loadStd));
    }

    public void update(UnloadDecision.Label label, UnloadDecision.Reason reason) {
        if (label == Success) {
            unloadBundleCount++;
        }
        breakdownCounters.get(label).get(reason).incrementAndGet();

        var attributes = Attributes.of(
                LOAD_BALANCER_UNLOAD_DECISION_KEY, label.name().toLowerCase(),
                LOAD_BALANCER_UNLOAD_REASON_KEY, reason.name().toLowerCase());
        unloadCounter.add(1, attributes);

        updatedAt = System.currentTimeMillis();
    }

    public void updateLoadData(double loadAvg, double loadStd) {
        this.loadAvg = loadAvg;
        this.loadStd = loadStd;
        updatedAt = System.currentTimeMillis();
    }

    public void updateUnloadBrokerCount(int unloadBrokerCount) {
        this.unloadBrokerCount += unloadBrokerCount;
        unloadBrokerCounter.add(unloadBrokerCount);
        updatedAt = System.currentTimeMillis();
    }

    public List<Metrics> toMetrics(String advertisedBrokerAddress) {
        var metrics = new ArrayList<Metrics>();
        var dimensions = new HashMap<String, String>();

        dimensions.put("metric", "bundleUnloading");
        dimensions.put("broker", advertisedBrokerAddress);
        var m = Metrics.create(dimensions);
        m.put("brk_lb_unload_broker_total", unloadBrokerCount);
        m.put("brk_lb_unload_bundle_total", unloadBundleCount);
        metrics.add(m);

        for (var etr : breakdownCounters.entrySet()) {
            var result = etr.getKey();
            for (var counter : etr.getValue().entrySet()) {
                var reason = counter.getKey();
                var count = counter.getValue().longValue();
                var dim = new HashMap<>(dimensions);
                dim.put("result", result.toString());
                dim.put("reason", reason.toString());
                var metric = Metrics.create(dim);
                metric.put("brk_lb_unload_broker_breakdown_total", count);
                metrics.add(metric);
            }
        }


        if (loadAvg > 0 && loadStd > 0) {
            {
                var dim = new HashMap<>(dimensions);
                dim.put("feature", "max_ema");
                dim.put("stat", "avg");
                var metric = Metrics.create(dim);
                metric.put("brk_lb_resource_usage_stats", loadAvg);
                metrics.add(metric);
            }
            {
                var dim = new HashMap<>(dimensions);
                dim.put("feature", "max_ema");
                dim.put("stat", "std");
                var metric = Metrics.create(dim);
                metric.put("brk_lb_resource_usage_stats", loadStd);
                metrics.add(metric);
            }
        }

        return metrics;
    }

    public long updatedAt() {
        return updatedAt;
    }

    @Override
    public void close() throws Exception {
        stdLoadGauge.close();
        avgLoadGauge.close();
    }
}
