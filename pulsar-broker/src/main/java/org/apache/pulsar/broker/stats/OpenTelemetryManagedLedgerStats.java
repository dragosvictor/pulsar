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
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;

public class OpenTelemetryManagedLedgerStats implements AutoCloseable {

    // Replaces pulsar_ml_AddEntryMessagesRate
    public static final String ENTRY_OUT_COUNTER = "pulsar.broker.managed_ledger.message.outgoing.count";
    private final ObservableLongMeasurement entryOutCounter;

    // Replaces pulsar_ml_AddEntryBytesRate
    public static final String BYTES_OUT_COUNTER = "pulsar.broker.managed_ledger.message.outgoing.size";
    private final ObservableLongMeasurement bytesOutCounter;

    // Replaces pulsar_ml_ReadEntriesRate
    public static final String ENTRY_IN_COUNTER = "pulsar.broker.managed_ledger.message.incoming.count";
    private final ObservableLongMeasurement entryInCounter;

    // Replaces pulsar_ml_ReadEntriesBytesRate
    public static final String BYTES_IN_COUNTER = "pulsar.broker.managed_ledger.message.incoming.size";
    private final ObservableLongMeasurement bytesInCounter;

    private final BatchCallback batchCallback;

    public OpenTelemetryManagedLedgerStats(PulsarService pulsar) {
        var meter = pulsar.getOpenTelemetry().getMeter();

        entryOutCounter = meter
                .counterBuilder(ENTRY_OUT_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages written to this ledger.")
                .buildObserver();

        bytesOutCounter = meter
                .counterBuilder(BYTES_OUT_COUNTER)
                .setUnit("By")
                .setDescription("The total number of messages bytes written to this ledger.")
                .buildObserver();

        entryInCounter = meter
                .counterBuilder(ENTRY_IN_COUNTER)
                .setUnit("{message}")
                .setDescription("The total number of messages read from this ledger.")
                .buildObserver();

        bytesInCounter = meter
                .counterBuilder(BYTES_IN_COUNTER)
                .setUnit("By")
                .setDescription("The total number of messages bytes read from this ledger.")
                .buildObserver();

        batchCallback = meter.batchCallback(() -> ((ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory())
                        .getManagedLedgers()
                        .values()
                        .forEach(this::recordMetricsForManagedLedger),
                entryOutCounter,
                bytesOutCounter,
                entryInCounter,
                bytesInCounter);
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private void recordMetricsForManagedLedger(ManagedLedgerImpl ml) {
        var builder = Attributes.builder();
        var attributes = builder.build();
        var dummyValue = 0L;

        entryOutCounter.record(dummyValue, attributes);
        bytesOutCounter.record(dummyValue, attributes);
        entryInCounter.record(dummyValue, attributes);
        bytesInCounter.record(dummyValue, attributes);
    }
}