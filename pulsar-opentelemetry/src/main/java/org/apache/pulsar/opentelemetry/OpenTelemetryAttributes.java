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
package org.apache.pulsar.opentelemetry;

import io.opentelemetry.api.common.AttributeKey;

/**
 * Common OpenTelemetry attributes to be used by Pulsar components.
 */
public interface OpenTelemetryAttributes {
    /**
     * The name of the Pulsar cluster. This attribute is automatically added to
     * all signals by
     * {@link OpenTelemetryService}.
     */
    AttributeKey<String> PULSAR_CLUSTER = AttributeKey.stringKey("pulsar.cluster");

    /**
     * The name of the Pulsar namespace.
     */
    AttributeKey<String> PULSAR_NAMESPACE = AttributeKey.stringKey("pulsar.namespace");

    /**
     * The name of the Pulsar tenant.
     */
    AttributeKey<String> PULSAR_TENANT = AttributeKey.stringKey("pulsar.tenant");

    /**
     * The Pulsar topic domain.
     */
    AttributeKey<String> PULSAR_DOMAIN = AttributeKey.stringKey("pulsar.domain");

    /**
     * The name of the Pulsar topic.
     */
    AttributeKey<String> PULSAR_TOPIC = AttributeKey.stringKey("pulsar.topic");

    /**
     * The partition index of a Pulsar topic.
     */
    AttributeKey<Long> PULSAR_PARTITION_INDEX = AttributeKey.longKey("pulsar.partition.index");

    /**
     * The status of the Pulsar transaction.
     */
    AttributeKey<String> PULSAR_TRANSACTION_STATUS = AttributeKey.stringKey("pulsar.transaction.status");

    /**
     * The status of the Pulsar compaction operation.
     */
    AttributeKey<String> PULSAR_COMPACTION_STATUS = AttributeKey.stringKey("pulsar.compaction.status");

    /**
     * The type of the backlog quota.
     */
    AttributeKey<String> PULSAR_BACKLOG_QUOTA_TYPE = AttributeKey.stringKey("pulsar.backlog.quota.type");
}
