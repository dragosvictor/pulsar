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
package org.apache.pulsar.broker.stats;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.stats.MetricsUtil;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

public class OpenTelemetryNamespaceStats {

    private final Map<NamespaceName, StatsObject> statsMap;

    private final DoubleHistogram publishLatency;

    public OpenTelemetryNamespaceStats(PulsarService pulsarService) {
        statsMap = new ConcurrentHashMap<>(32);

        var meter = pulsarService.getOpenTelemetry().getMeter();
        publishLatency = meter.histogramBuilder("pulsar.namespace.publish.duration")
                .setDescription("Publish duration")
                .setUnit("s")
                .build();
    }

    public StatsObject getStats(NamespaceName namespace) {
        return statsMap.computeIfAbsent(namespace, StatsObject::new);
    }

    public class StatsObject {
        private final Attributes attributes;

        public StatsObject(NamespaceName namespace) {
            attributes = Attributes.of(
                    OpenTelemetryAttributes.PULSAR_NAMESPACE, namespace.getLocalName(),
                    OpenTelemetryAttributes.PULSAR_TENANT, namespace.getTenant()
            );
        }

        public void recordAddLatency(long latency, TimeUnit timeUnit) {
            publishLatency.record(MetricsUtil.convertToSeconds(latency, timeUnit), attributes);
        }
    }
}
