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

import com.google.common.collect.Streams;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import org.apache.bookkeeper.mledger.ManagedCursor;

public class OpenTelemetryManagedLedgerCursorStats implements AutoCloseable {

    private final ObservableLongMeasurement persistLedgerCounter;

    private final BatchCallback batchCallback;

    public OpenTelemetryManagedLedgerCursorStats(ManagedLedgerFactoryImpl factory, OpenTelemetry openTelemetry) {
        var meter = openTelemetry.getMeter("pulsar.managed_ledger.cursor"); // TODO

        persistLedgerCounter = meter.upDownCounterBuilder("persist_ledger_counter")
            .setDescription("Number of times a ledger is persisted")
            .buildObserver();

        batchCallback = meter.batchCallback(() -> factory.getManagedLedgers()
                        .values()
                        .stream()
                        .map(ManagedLedgerImpl::getCursors)
                        .flatMap(Streams::stream)
                        .forEach(this::recordMetrics),
                persistLedgerCounter
        );
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    public static final AttributeKey<String> CURSOR_NAME = AttributeKey.stringKey("cursor_name");
    public static final AttributeKey<String> OPERATION_STATUS = AttributeKey.stringKey("operation_status");

    private void recordMetrics(ManagedCursor cursor) {
        var stats = cursor.getStats();
        var attributes = Attributes.of(
                ManagedLedgerMBeanImpl.PULSAR_MANAGER_LEDGER_NAME, stats.getLedgerName(),
                CURSOR_NAME, cursor.getName());
        var attributesSucceed = Attributes.of(
                ManagedLedgerMBeanImpl.PULSAR_MANAGER_LEDGER_NAME, stats.getLedgerName(),
                CURSOR_NAME, cursor.getName(),
                OPERATION_STATUS, "succeed");
        var attributesFailed = Attributes.of(
                ManagedLedgerMBeanImpl.PULSAR_MANAGER_LEDGER_NAME, stats.getLedgerName(),
                CURSOR_NAME, cursor.getName(),
                OPERATION_STATUS, "failed");

        persistLedgerCounter.record(stats.getPersistLedgerSucceed(), attributesSucceed);
        persistLedgerCounter.record(stats.getPersistLedgerErrors(), attributesFailed);

        persistLedgerCounter.record(cursor.getTotalNonContiguousDeletedMessagesRange(), attributes);
        persistLedgerCounter.record(stats.getReadCursorLedgerSize(), attributes);
        persistLedgerCounter.record(stats.getWriteCursorLedgerSize(), attributes);
        persistLedgerCounter.record(stats.getWriteCursorLedgerLogicalSize(), attributes);

        persistLedgerCounter.record(stats.getPersistZookeeperSucceed(), attributesSucceed);
        persistLedgerCounter.record(stats.getPersistZookeeperErrors(), attributesFailed);
    }
}
