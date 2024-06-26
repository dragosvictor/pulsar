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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandleAttributes;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandleStats;
import org.apache.pulsar.common.naming.TopicName;

public class PendingAckHandleStatsImpl implements PendingAckHandleStats {
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static Counter commitTxnCounter;
    private static Counter abortTxnCounter;
    private static Summary commitTxnLatency;
    private static boolean exposeTopicLevelMetrics0;

    private final String[] labelSucceed;
    private final String[] labelFailed;
    private final String[] commitLatencyLabel;

    private final String topic;
    private final String subscription;

    private final LongAdder commitTxnSucceedCounter = new LongAdder();
    private final LongAdder commitTxnFailedCounter = new LongAdder();
    private final LongAdder abortTxnSucceedCounter = new LongAdder();
    private final LongAdder abortTxnFailedCounter = new LongAdder();

    private volatile PendingAckHandleAttributes attributes = null;
    private static final AtomicReferenceFieldUpdater<PendingAckHandleStatsImpl, PendingAckHandleAttributes>
            ATTRIBUTES_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
                    PendingAckHandleStatsImpl.class, PendingAckHandleAttributes.class, "attributes");

    public PendingAckHandleStatsImpl(String topic, String subscription, boolean exposeTopicLevelMetrics) {
        initialize(exposeTopicLevelMetrics);

        String namespace;
        if (StringUtils.isBlank(topic)) {
            namespace = topic = "unknown";
        } else {
            try {
                namespace = TopicName.get(topic).getNamespace();
            } catch (IllegalArgumentException ex) {
                namespace = "unknown";
            }
        }

        this.topic = topic;
        this.subscription = subscription;

        labelSucceed = exposeTopicLevelMetrics0
                ? new String[]{namespace, topic, subscription, "succeed"} : new String[]{namespace, "succeed"};
        labelFailed = exposeTopicLevelMetrics0
                ? new String[]{namespace, topic, subscription, "failed"} : new String[]{namespace, "failed"};
        commitLatencyLabel = exposeTopicLevelMetrics0
                ? new String[]{namespace, topic, subscription} : new String[]{namespace};
    }

    @Override
    public void recordCommitTxn(boolean success, long nanos) {
        String[] labels;
        LongAdder counter;
        if (success) {
            labels = labelSucceed;
            counter = commitTxnSucceedCounter;
            commitTxnLatency.labels(commitLatencyLabel).observe(TimeUnit.NANOSECONDS.toMicros(nanos));
        } else {
            labels = labelFailed;
            counter = commitTxnFailedCounter;
        }
        commitTxnCounter.labels(labels).inc();
        counter.increment();
    }

    @Override
    public void recordAbortTxn(boolean success) {
        abortTxnCounter.labels(success ? labelSucceed : labelFailed).inc();
        var counter = success ? abortTxnSucceedCounter : abortTxnFailedCounter;
        counter.increment();
    }

    @Override
    public void close() {
        if (exposeTopicLevelMetrics0) {
            commitTxnCounter.remove(this.labelSucceed);
            commitTxnCounter.remove(this.labelFailed);
            abortTxnCounter.remove(this.labelSucceed);
            abortTxnCounter.remove(this.labelFailed);
        }
    }

    @Override
    public long getCommitSuccessCount() {
        return commitTxnSucceedCounter.sum();
    }

    @Override
    public long getCommitFailedCount() {
        return commitTxnFailedCounter.sum();
    }

    @Override
    public long getAbortSuccessCount() {
        return abortTxnSucceedCounter.sum();
    }

    @Override
    public long getAbortFailedCount() {
        return abortTxnFailedCounter.sum();
    }

    @Override
    public PendingAckHandleAttributes getAttributes() {
        if (attributes != null) {
            return attributes;
        }
        return ATTRIBUTES_UPDATER.updateAndGet(PendingAckHandleStatsImpl.this,
                old -> old != null ? old : new PendingAckHandleAttributes(topic, subscription));
    }

    static void initialize(boolean exposeTopicLevelMetrics) {
        if (INITIALIZED.compareAndSet(false, true)) {
            exposeTopicLevelMetrics0 = exposeTopicLevelMetrics;

            String[] labelNames = exposeTopicLevelMetrics
                    ? new String[]{"namespace", "topic", "subscription", "status"}
                    : new String[]{"namespace", "status"};

            commitTxnCounter = Counter
                    .build("pulsar_txn_tp_committed_count", "-")
                    .labelNames(labelNames)
                    .register();

            abortTxnCounter = Counter
                    .build("pulsar_txn_tp_aborted_count", "-")
                    .labelNames(labelNames)
                    .register();

            commitTxnLatency = Summary.build("pulsar_txn_tp_commit_latency", "-")
                    .quantile(0.5, 0.01)
                    .quantile(0.9, 0.01)
                    .quantile(0.99, 0.01)
                    .quantile(0.999, 0.01)
                    .labelNames(exposeTopicLevelMetrics
                            ? new String[]{"namespace", "topic", "subscription"} : new String[]{"namespace"})
                    .register();
        }
    }
}
