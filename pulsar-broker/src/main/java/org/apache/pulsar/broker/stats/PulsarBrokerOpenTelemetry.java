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

import io.opentelemetry.api.metrics.Meter;
import java.io.Closeable;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.stats.OpenTelemetryService;

public class PulsarBrokerOpenTelemetry implements Closeable {

    private final OpenTelemetryService openTelemetryService;

    @Getter
    private final Meter meter;

    public PulsarBrokerOpenTelemetry(PulsarService pulsar) {
        this.openTelemetryService = new OpenTelemetryService(pulsar.getConfiguration().getClusterName());
        this.meter = openTelemetryService.getMeter("pulsar.broker");
    }

    @Override
    public void close() {
        openTelemetryService.close();
    }
}
