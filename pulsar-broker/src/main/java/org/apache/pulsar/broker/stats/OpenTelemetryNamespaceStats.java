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
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.stats.MetricsUtil;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

public class OpenTelemetryNamespaceStats {

    public static final String PUBLISH_DURATION_METRIC_NAME = "pulsar.namespace.publish.duration";
    private final DoubleHistogram publishLatency;

    private final Attributes attributes;

    public OpenTelemetryNamespaceStats(PulsarService pulsarService, NamespaceName namespace) {
        attributes = Attributes.of(
                OpenTelemetryAttributes.PULSAR_NAMESPACE, namespace.getLocalName(),
                OpenTelemetryAttributes.PULSAR_TENANT, namespace.getTenant()
        );

        var meter = pulsarService.getOpenTelemetry().getMeter();
        publishLatency = meter.histogramBuilder(PUBLISH_DURATION_METRIC_NAME)
                .setDescription("Publish duration")
                .setUnit("s")
                .build();
    }

    public void recordAddLatency(long latency, TimeUnit timeUnit) {
        publishLatency.record(MetricsUtil.convertToSeconds(latency, timeUnit), attributes);
    }
}
