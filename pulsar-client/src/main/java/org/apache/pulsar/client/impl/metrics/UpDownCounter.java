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

package org.apache.pulsar.client.impl.metrics;

import static org.apache.pulsar.client.impl.metrics.MetricsUtil.getDefaultAggregationLabels;
import static org.apache.pulsar.client.impl.metrics.MetricsUtil.getTopicAttributes;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.incubator.metrics.ExtendedLongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.Meter;

public class UpDownCounter {

    private final LongUpDownCounter counter;
    private final Attributes attributes;

    UpDownCounter(Meter meter, String name, Unit unit, String description, String topic, Attributes attributes) {
        LongUpDownCounterBuilder builder = meter.upDownCounterBuilder(name)
                .setDescription(description)
                .setUnit(unit.toString());

        if (topic != null) {
            if (builder instanceof ExtendedLongUpDownCounterBuilder) {
                ExtendedLongUpDownCounterBuilder eb = (ExtendedLongUpDownCounterBuilder) builder;
                eb.setAttributesAdvice(getDefaultAggregationLabels(attributes));
            }

            attributes = getTopicAttributes(topic, attributes);
        }

        this.counter = builder.build();
        this.attributes = attributes;
    }

    public void increment() {
        add(1);
    }

    public void decrement() {
        add(-1);
    }

    public void add(long delta) {
        counter.add(delta, attributes);
    }

    public void subtract(long diff) {
        add(-diff);
    }
}
