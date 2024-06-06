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

import static org.apache.bookkeeper.mledger.impl.OpenTelemetryManagedLedgerCacheStats.CACHE_ENTRY_COUNTER;
import static org.apache.bookkeeper.mledger.impl.OpenTelemetryManagedLedgerCacheStats.CACHE_EVICTION_OPERATION_COUNTER;
import static org.apache.bookkeeper.mledger.impl.OpenTelemetryManagedLedgerCacheStats.CACHE_HIT_BYTES_COUNTER;
import static org.apache.bookkeeper.mledger.impl.OpenTelemetryManagedLedgerCacheStats.CACHE_HIT_COUNTER;
import static org.apache.bookkeeper.mledger.impl.OpenTelemetryManagedLedgerCacheStats.CACHE_MISS_BYTES_COUNTER;
import static org.apache.bookkeeper.mledger.impl.OpenTelemetryManagedLedgerCacheStats.CACHE_MISS_COUNTER;
import static org.apache.bookkeeper.mledger.impl.OpenTelemetryManagedLedgerCacheStats.CACHE_POOL_ACTIVE_ALLOCATION_COUNTER;
import static org.apache.bookkeeper.mledger.impl.OpenTelemetryManagedLedgerCacheStats.CACHE_POOL_ACTIVE_ALLOCATION_SIZE_COUNTER;
import static org.apache.bookkeeper.mledger.impl.OpenTelemetryManagedLedgerCacheStats.CACHE_SIZE_COUNTER;
import static org.apache.bookkeeper.mledger.impl.OpenTelemetryManagedLedgerCacheStats.MANAGED_LEDGER_COUNTER;
import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongSumValue;
import static org.assertj.core.api.Assertions.assertThat;
import io.opentelemetry.api.common.Attributes;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.impl.OpenTelemetryManagedLedgerCacheStats.CacheEntryStatus;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OpenTelemetryManagedLedgerCacheStatsTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder builder) {
        super.customizeMainPulsarTestContextBuilder(builder);
        builder.enableOpenTelemetry(true);
    }

    @Test
    public void testManagedLedgerCacheStats() throws Exception {
        var topicName = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/testManagedLedgerCacheStats");

        @Cleanup
        var producer = pulsarClient.newProducer().topic(topicName).create();
        producer.send("test".getBytes());

        @Cleanup
        var consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(BrokerTestUtil.newUniqueName("sub"))
                .subscribe();
        consumer.receive();

        var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        var attributes = Attributes.empty();

        assertMetricLongSumValue(metrics, MANAGED_LEDGER_COUNTER, attributes, 2);
        assertMetricLongSumValue(metrics, CACHE_EVICTION_OPERATION_COUNTER, attributes, 0);
        assertMetricLongSumValue(metrics, CACHE_ENTRY_COUNTER, CacheEntryStatus.ACTIVE.attributes, 1);
        assertMetricLongSumValue(metrics, CACHE_ENTRY_COUNTER, CacheEntryStatus.INSERTED.attributes, 1);
        assertMetricLongSumValue(metrics, CACHE_ENTRY_COUNTER, CacheEntryStatus.EVICTED.attributes, 0);
        assertMetricLongSumValue(metrics, CACHE_SIZE_COUNTER, attributes, value -> assertThat(value).isPositive());

        assertMetricLongSumValue(metrics, CACHE_HIT_COUNTER, attributes, 1);
        assertMetricLongSumValue(metrics, CACHE_HIT_BYTES_COUNTER, attributes, value -> assertThat(value).isPositive());
        assertMetricLongSumValue(metrics, CACHE_MISS_COUNTER, attributes, 1);
        assertMetricLongSumValue(metrics, CACHE_MISS_BYTES_COUNTER, attributes, 1);

        assertMetricLongSumValue(metrics, CACHE_POOL_ACTIVE_ALLOCATION_COUNTER, attributes, 1);
        assertMetricLongSumValue(metrics, CACHE_POOL_ACTIVE_ALLOCATION_SIZE_COUNTER, attributes, 1);
    }
}
