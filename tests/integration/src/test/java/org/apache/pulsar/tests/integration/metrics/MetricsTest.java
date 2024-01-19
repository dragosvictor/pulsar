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
package org.apache.pulsar.tests.integration.metrics;

import static org.testng.Assert.assertEquals;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.tests.integration.containers.OpenTelemetryCollectorContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Slf4j
public class MetricsTest {

    @Test
    public void testOpenTelemetryMetrics() throws Exception {
        var clusterName = MetricsTest.class.getSimpleName() + "-" + UUID.randomUUID();
        var openTelemetryCollectorContainer = new OpenTelemetryCollectorContainer(clusterName);

        var brokerOtelServiceName = clusterName + "-broker";
        var localCollectorProps = Map.of(
                "OTEL_SDK_DISABLED", "false",
                "OTEL_METRICS_EXPORTER", "otlp",
                "OTEL_METRIC_EXPORT_INTERVAL", "1000",
                "OTEL_EXPORTER_OTLP_ENDPOINT", openTelemetryCollectorContainer.getOtlpEndpoint(),
                "OTEL_SERVICE_NAME", brokerOtelServiceName
        );

        var proxyOtelServiceName = clusterName + "-proxy";
        var proxyCollectorProps = Map.of(
                "OTEL_SDK_DISABLED", "false",
                "OTEL_METRICS_EXPORTER", "otlp",
                "OTEL_METRIC_EXPORT_INTERVAL", "1000",
                "OTEL_EXPORTER_OTLP_ENDPOINT", openTelemetryCollectorContainer.getOtlpEndpoint(),
                "OTEL_SERVICE_NAME", proxyOtelServiceName
        );

        var spec = PulsarClusterSpec.builder()
                .clusterName(clusterName)
                .numBookies(1)
                .numBrokers(1)
                .brokerEnvs(localCollectorProps)
                .numProxies(1)
                .proxyEnvs(proxyCollectorProps)
                .externalService("otel-collector", openTelemetryCollectorContainer)
                .build();
        @Cleanup("stop")
        var pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        var prometheusClient = openTelemetryCollectorContainer.getMetricsClient();

        Awaitility.waitAtMost(Duration.ofMinutes(5)).pollInterval(Duration.ofSeconds(1)).until(() -> {
            var metricName = "queueSize_ratio"; // Sent automatically by the OpenTelemetry SDK.
            var metrics = prometheusClient.getMetrics();
            var brokerMetrics = metrics.findByNameAndLabels(metricName, Pair.of("job", brokerOtelServiceName));
            var proxyMetrics = metrics.findByNameAndLabels(metricName, Pair.of("job", proxyOtelServiceName));
            return !brokerMetrics.isEmpty() && !proxyMetrics.isEmpty();
        });
    }
}
