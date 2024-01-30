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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.tests.integration.containers.ChaosContainer;
import org.apache.pulsar.tests.integration.containers.OpenTelemetryCollectorContainer;
import org.apache.pulsar.tests.integration.containers.WorkerContainer;
import org.apache.pulsar.tests.integration.functions.PulsarFunctionsTest;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator;
import org.apache.pulsar.tests.integration.topologies.FunctionRuntimeType;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarTestBase;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Slf4j
public class MetricsTest {

    // Test with the included Prometheus exporter as well
    // https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md#prometheus-exporter
    @Test(timeOut = 360_000)
    public void testOpenTelemetryMetricsOtlpExport() throws Exception {
        var clusterName = "testOpenTelemetryMetrics-" + UUID.randomUUID();
        var openTelemetryCollectorContainer = new OpenTelemetryCollectorContainer(clusterName);

        var exporter = "otlp";
        var otlpEndpointProp =
                Pair.of("OTEL_EXPORTER_OTLP_ENDPOINT", openTelemetryCollectorContainer.getOtlpEndpoint());

        var brokerOtelServiceName = clusterName + "-broker";
        var brokerCollectorProps = getCollectorProps(brokerOtelServiceName, exporter, otlpEndpointProp);

        var proxyOtelServiceName = clusterName + "-proxy";
        var proxyCollectorProps = getCollectorProps(proxyOtelServiceName, exporter, otlpEndpointProp);

        var functionWorkerServiceNameSuffix = PulsarTestBase.randomName();
        var functionWorkerOtelServiceName = "function-worker-" + functionWorkerServiceNameSuffix;
        var functionWorkerCollectorProps = getCollectorProps(functionWorkerOtelServiceName, exporter, otlpEndpointProp);

        var spec = PulsarClusterSpec.builder()
                .clusterName(clusterName)
                .brokerEnvs(brokerCollectorProps)
                .proxyEnvs(proxyCollectorProps)
                .externalService("otel-collector", openTelemetryCollectorContainer)
                .functionWorkerEnv(functionWorkerServiceNameSuffix, functionWorkerCollectorProps)
                .build();
        @Cleanup("stop")
        var pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        setupFunctionWorker(pulsarCluster, functionWorkerServiceNameSuffix);

        // TODO: Validate cluster name is present once
        // https://github.com/open-telemetry/opentelemetry-java/issues/6108 is solved.
        var metricName = "queueSize_ratio"; // Sent automatically by the OpenTelemetry SDK.
        Awaitility.waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).until(() ->
            hasMetrics(openTelemetryCollectorContainer, OpenTelemetryCollectorContainer.PROMETHEUS_EXPORTER_PORT,
                    metricName, Pair.of("job", brokerOtelServiceName)));
        Awaitility.waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).until(() ->
                hasMetrics(openTelemetryCollectorContainer, OpenTelemetryCollectorContainer.PROMETHEUS_EXPORTER_PORT,
                        metricName, Pair.of("job", proxyOtelServiceName)));
        Awaitility.waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).until(() ->
                hasMetrics(openTelemetryCollectorContainer, OpenTelemetryCollectorContainer.PROMETHEUS_EXPORTER_PORT,
                        metricName, Pair.of("job", functionWorkerOtelServiceName)));
    }

    @Test(timeOut = 360_000)
    public void testOpenTelemetryMetricsPrometheusExport() throws Exception {
        var prometheusExporterPort = 9464;
        var clusterName = "testOpenTelemetryMetrics-" + UUID.randomUUID();

        var exporter = "prometheus";
        var prometheusExporterPortProp =
                Pair.of("OTEL_EXPORTER_PROMETHEUS_PORT", Integer.toString(prometheusExporterPort));

        var brokerOtelServiceName = clusterName + "-broker";
        var brokerCollectorProps = getCollectorProps(brokerOtelServiceName, exporter, prometheusExporterPortProp);

        var proxyOtelServiceName = clusterName + "-proxy";
        var proxyCollectorProps = getCollectorProps(proxyOtelServiceName, exporter, prometheusExporterPortProp);

        var functionWorkerServiceNameSuffix = PulsarTestBase.randomName();
        var functionWorkerOtelServiceName = "function-worker-" + functionWorkerServiceNameSuffix;
        var functionWorkerCollectorProps =
                getCollectorProps(functionWorkerOtelServiceName, exporter, prometheusExporterPortProp);

        var spec = PulsarClusterSpec.builder()
                .clusterName(clusterName)
                .brokerEnvs(brokerCollectorProps)
                .brokerAdditionalPorts(List.of(prometheusExporterPort))
                .proxyEnvs(proxyCollectorProps)
                .proxyAdditionalPorts(List.of(prometheusExporterPort))
                .functionWorkerEnv(functionWorkerServiceNameSuffix, functionWorkerCollectorProps)
                .functionWorkerAdditionalPorts(functionWorkerServiceNameSuffix, List.of(prometheusExporterPort))
                .build();
        @Cleanup("stop")
        var pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        var workerContainer = setupFunctionWorker(pulsarCluster, functionWorkerServiceNameSuffix);

        var metricName = "target_info"; // Sent automatically by the OpenTelemetry SDK.
        Awaitility.waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).until(() ->
                hasMetrics(pulsarCluster.getAnyBroker(), prometheusExporterPort, metricName,
                        Pair.of("pulsar_cluster", clusterName),
                        Pair.of("service_name", brokerOtelServiceName)));
        Awaitility.waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).until(() ->
                hasMetrics(pulsarCluster.getProxy(), prometheusExporterPort, metricName,
                        Pair.of("pulsar_cluster", clusterName),
                        Pair.of("service_name", proxyOtelServiceName)));
        Awaitility.waitAtMost(90, TimeUnit.SECONDS).ignoreExceptions().pollInterval(1, TimeUnit.SECONDS).until(() ->
                hasMetrics(workerContainer, prometheusExporterPort, metricName,
                        Pair.of("pulsar_cluster", clusterName),
                        Pair.of("service_name", functionWorkerOtelServiceName)));
    }

    @SafeVarargs
    private static boolean hasMetrics(ChaosContainer<?> container, int port, String metricName,
                                      Pair<String, String> ... expectedLabels) throws Exception {
        var client = new PrometheusMetricsClient(container.getHost(), container.getMappedPort(port));
        var allMetrics = client.getMetrics();
        var actualMetrics = allMetrics.findByNameAndLabels(metricName, expectedLabels);
        return !actualMetrics.isEmpty();
    }

    @SafeVarargs
    private static Map<String, String> getCollectorProps(String serviceName, String exporter,
                                                         Pair<String, String> ... extraProps) {
        var defaultProps = Map.of(
                "OTEL_SDK_DISABLED", "false",
                "OTEL_METRIC_EXPORT_INTERVAL", "1000",
                "OTEL_SERVICE_NAME", serviceName,
                "OTEL_METRICS_EXPORTER", exporter
        );
        var props = new HashMap<>(defaultProps);
        Arrays.stream(extraProps).forEach(p -> props.put(p.getKey(), p.getValue()));
        return props;
    }

    private static WorkerContainer setupFunctionWorker(PulsarCluster pulsarCluster, String suffix) throws Exception {
        pulsarCluster.setupFunctionWorkers(suffix, FunctionRuntimeType.PROCESS, 1);

        var namespace = NamespaceName.get("public", "default");
        var sourceTopicName = TopicName.get(TopicDomain.persistent.toString(), namespace, "metricTestSource-" + suffix);
        var sinkTopicName = TopicName.get(TopicDomain.persistent.toString(), namespace, "metricTestSink-" + suffix);

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(sourceTopicName.toString());
            admin.topics().createNonPartitionedTopic(sinkTopicName.toString());
        }

        var commandGenerator = new CommandGenerator();
        commandGenerator.setSourceTopic(sourceTopicName.toString());
        commandGenerator.setSinkTopic(sinkTopicName.toString());
        commandGenerator.setRuntime(CommandGenerator.Runtime.JAVA);
        commandGenerator.setFunctionName("metricsTestLocalRunTest-" + suffix);
        commandGenerator.setFunctionClassName(PulsarFunctionsTest.EXCLAMATION_JAVA_CLASS);
        var functionWorkerCommand = commandGenerator.generateCreateFunctionCommand();

        var workerContainer = pulsarCluster.getAnyWorker();
        workerContainer.execCmdAsync("sh", "-c", functionWorkerCommand);
        return workerContainer;
    }
}
