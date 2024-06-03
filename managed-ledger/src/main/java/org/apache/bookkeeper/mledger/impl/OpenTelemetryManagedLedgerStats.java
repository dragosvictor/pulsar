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
package org.apache.bookkeeper.mledger.impl;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;

public class OpenTelemetryManagedLedgerStats implements AutoCloseable {

    // Replaces pulsar_ml_AddEntryMessagesRate
    public static final String ADD_ENTRY_COUNTER = "pulsar.broker.managed_ledger.message.outgoing.count";
    private final ObservableLongMeasurement addEntryCounter;

    // Replaces pulsar_ml_AddEntryBytesRate
    public static final String BYTES_OUT_COUNTER = "pulsar.broker.managed_ledger.message.outgoing.size";
    private final ObservableLongMeasurement bytesOutCounter;

    // Replaces pulsar_ml_ReadEntriesRate
    public static final String READ_ENTRY_COUNTER = "pulsar.broker.managed_ledger.message.incoming.count";
    private final ObservableLongMeasurement readEntryCounter;

    // Replaces pulsar_ml_ReadEntriesBytesRate
    public static final String BYTES_IN_COUNTER = "pulsar.broker.managed_ledger.message.incoming.size";
    private final ObservableLongMeasurement bytesInCounter;

    // Replaces pulsar_ml_MarkDeleteRate
    public static final String MARK_DELETE_COUNTER = "pulsar.broker.managed_ledger.mark_delete.count";
    private final ObservableLongMeasurement markDeleteCounter;

    private final BatchCallback batchCallback;

    public OpenTelemetryManagedLedgerStats(ManagedLedgerFactoryImpl factory, OpenTelemetry openTelemetry) {
        var meter = openTelemetry.getMeter("pulsar.managed_ledger");

        addEntryCounter = meter
                .upDownCounterBuilder(ADD_ENTRY_COUNTER)
                .setUnit("{operation}")
                .setDescription("The number of write operations to this ledger.")
                .buildObserver();

        bytesOutCounter = meter
                .counterBuilder(BYTES_OUT_COUNTER)
                .setUnit("By")
                .setDescription("The total number of messages bytes written to this ledger.")
                .buildObserver();

        readEntryCounter = meter
                .upDownCounterBuilder(READ_ENTRY_COUNTER)
                .setUnit("{operation}")
                .setDescription("The number of read operations from this ledger.")
                .buildObserver();

        bytesInCounter = meter
                .counterBuilder(BYTES_IN_COUNTER)
                .setUnit("By")
                .setDescription("The total number of messages bytes read from this ledger.")
                .buildObserver();

        markDeleteCounter = meter
                .counterBuilder(MARK_DELETE_COUNTER)
                .setUnit("{operation}")
                .setDescription("The total number of mark delete operations for this ledger.")
                .buildObserver();

        batchCallback = meter.batchCallback(() -> factory.getManagedLedgers()
                        .values()
                        .forEach(this::recordMetricsForManagedLedger),
                addEntryCounter,
                bytesOutCounter,
                readEntryCounter,
                bytesInCounter,
                markDeleteCounter);
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private static final AttributeKey<String> PULSAR_MANAGED_LEDGER_OPERATION_STATUS =
            AttributeKey.stringKey("pulsar.managed_ledger.operation.status");

    private void recordMetricsForManagedLedger(ManagedLedgerImpl ml) {
        var stats = ml.getMbean();
        var attributes = stats.getAttributes();
        var attributesSucceed = attributes.toBuilder().put(PULSAR_MANAGED_LEDGER_OPERATION_STATUS, "succeed").build();
        var attributesFailure = attributes.toBuilder().put(PULSAR_MANAGED_LEDGER_OPERATION_STATUS, "failure").build();
        var attributesActive = attributes.toBuilder().put(PULSAR_MANAGED_LEDGER_OPERATION_STATUS, "active").build();

        var addEntryTotal = stats.getAddEntrySucceedTotal();
        var addEntrySucceed = stats.getAddEntrySucceedTotal();
        var addEntryFailed = stats.getAddEntryErrors();
        var addEntryActive = addEntryTotal - addEntrySucceed - addEntryFailed;
        addEntryCounter.record(addEntrySucceed, attributesSucceed);
        addEntryCounter.record(addEntryFailed, attributesFailure);
        addEntryCounter.record(addEntryActive, attributesActive);
        bytesOutCounter.record(stats.getAddEntryBytesTotal(), attributes);

        var readEntryTotal = stats.getReadEntriesSucceededTotal();
        var readEntrySucceed = stats.getReadEntriesSucceededTotal();
        var readEntryFailed = stats.getReadEntriesErrors();
        var readEntryActive = readEntryTotal - readEntrySucceed - readEntryFailed;
        readEntryCounter.record(readEntrySucceed, attributesSucceed);
        readEntryCounter.record(readEntryFailed, attributesFailure);
        readEntryCounter.record(readEntryActive, attributesActive);
        bytesInCounter.record(stats.getReadEntriesBytesTotal(), attributes);

        markDeleteCounter.record(stats.getMarkDeleteTotal(), attributes);
    }
}
