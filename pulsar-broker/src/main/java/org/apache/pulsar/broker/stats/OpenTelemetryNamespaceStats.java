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
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandleAttributes;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.stats.MetricsUtil;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

public class OpenTelemetryNamespaceStats {

    public static final String PUBLISH_DURATION_METRIC_NAME = "pulsar.namespace.publish.duration";
    private final DoubleHistogram publishLatency;

    public static final String TRANSACTION_BUFFER_CLIENT_OPERATION_DURATION_METRIC_NAME =
            "pulsar.broker.transaction.buffer.client.operation.duration";
    private final DoubleHistogram txBufferClientOperationLatency;

    private final Attributes commonAttributes;
    private final Attributes transactionBufferClientAbortFailedAttributes;
    private final Attributes transactionBufferClientAbortSucceededAttributes;
    private final Attributes transactionBufferClientCommitFailedAttributes;
    private final Attributes transactionBufferClientCommitSucceededAttributes;

    @Getter
    private final PendingAckHandleAttributes pendingAckHandleAttributes;

    public OpenTelemetryNamespaceStats(PulsarService pulsarService, NamespaceName namespace) {
        var meter = pulsarService.getOpenTelemetry().getMeter();

        commonAttributes = Attributes.of(
                OpenTelemetryAttributes.PULSAR_NAMESPACE, namespace.getLocalName(),
                OpenTelemetryAttributes.PULSAR_TENANT, namespace.getTenant()
        );

        transactionBufferClientCommitSucceededAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.TransactionStatus.COMMITTED.attributes)
                .putAll(OpenTelemetryAttributes.TransactionBufferClientOperationStatus.SUCCESS.attributes)
                .build();
        transactionBufferClientCommitFailedAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.TransactionStatus.COMMITTED.attributes)
                .putAll(OpenTelemetryAttributes.TransactionBufferClientOperationStatus.FAILURE.attributes)
                .build();
        transactionBufferClientAbortSucceededAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.TransactionStatus.ABORTED.attributes)
                .putAll(OpenTelemetryAttributes.TransactionBufferClientOperationStatus.SUCCESS.attributes)
                .build();
        transactionBufferClientAbortFailedAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.TransactionStatus.ABORTED.attributes)
                .putAll(OpenTelemetryAttributes.TransactionBufferClientOperationStatus.FAILURE.attributes)
                .build();
        txBufferClientOperationLatency = meter
                .histogramBuilder(TRANSACTION_BUFFER_CLIENT_OPERATION_DURATION_METRIC_NAME)
                .setDescription("The duration of a transaction buffer client operation.")
                .setUnit("s")
                .build();

        pendingAckHandleAttributes = new PendingAckHandleAttributes(namespace);

        publishLatency = meter.histogramBuilder(PUBLISH_DURATION_METRIC_NAME)
                .setDescription("Publish duration")
                .setUnit("s")
                .build();
    }

    public void recordAddLatency(long latency, TimeUnit timeUnit) {
        publishLatency.record(MetricsUtil.convertToSeconds(latency, timeUnit), commonAttributes);
    }

    public void recordTransactionBufferClientCommitSucceededLatency(long latencyNs) {
        txBufferClientOperationLatency.record(MetricsUtil.convertToSeconds(latencyNs, TimeUnit.NANOSECONDS),
                transactionBufferClientCommitSucceededAttributes);
    }

    public void recordTransactionBufferClientCommitFailedLatency(long latencyNs) {
        txBufferClientOperationLatency.record(MetricsUtil.convertToSeconds(latencyNs, TimeUnit.NANOSECONDS),
                transactionBufferClientCommitFailedAttributes);
    }

    public void recordTransactionBufferClientAbortSucceededLatency(long latencyNs) {
        txBufferClientOperationLatency.record(MetricsUtil.convertToSeconds(latencyNs, TimeUnit.NANOSECONDS),
                transactionBufferClientAbortSucceededAttributes);
    }

    public void recordTransactionBufferClientAbortFailedLatency(long latencyNs) {
        txBufferClientOperationLatency.record(MetricsUtil.convertToSeconds(latencyNs, TimeUnit.NANOSECONDS),
                transactionBufferClientAbortFailedAttributes);
    }
}
