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

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import org.apache.bookkeeper.mledger.impl.cache.PooledByteBufAllocatorStats;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheImpl;

public class OpenTelemetryManagedLedgerCacheStats implements AutoCloseable {

    public static final AttributeKey<String> POOL_ARENA_TYPE = AttributeKey.stringKey("pool_arena_type");
    @VisibleForTesting
    enum PoolArenaType {
        SMALL,
        NORMAL,
        HUGE;
        final Attributes attributes = Attributes.of(POOL_ARENA_TYPE, name().toLowerCase());
    }

    public static final AttributeKey<String> POOL_CHUNK_ALLOCATION_TYPE =
            AttributeKey.stringKey("pool_chunk_allocation_type");
    @VisibleForTesting
    enum PoolChunkAllocationType {
        ALLOCATED,
        USED;
        final Attributes attributes = Attributes.of(POOL_CHUNK_ALLOCATION_TYPE, name().toLowerCase());
    }

    // Replaces pulsar_ml_cache_evictions
    public static final String CACHE_EVICTION_COUNTER_RATE = "pulsar.broker.managed_ledger.cache.eviction.count";
    private final ObservableLongMeasurement cacheEvictionCounterRate;

    // Replaces pulsar_ml_cache_inserted_entries_total
    public static final String CACHE_INSERTION_COUNTER = "pulsar.broker.managed_ledger.cache.entry.insertion.count";
    private final ObservableLongMeasurement cacheInsertionCounter;

    // Replaces pulsar_ml_cache_evicted_entries_total
    public static final String CACHE_EVICTION_COUNTER = "pulsar.broker.managed_ledger.cache.entry.eviction.count";
    private final ObservableLongMeasurement cacheEvictionCounter;

    // Replaces pulsar_ml_cache_entries
    public static final String CACHE_ENTRY_COUNTER = "pulsar.broker.managed_ledger.cache.entry.count";
    private final ObservableLongMeasurement cacheEntryCounter;

    // Replaces pulsar_ml_cache_hits_rate
    public static final String CACHE_HIT_COUNTER = "pulsar.broker.managed_ledger.cache.hit.count";
    private final ObservableLongMeasurement cacheHitCounter;

    // Replaces pulsar_ml_cache_hits_throughput
    public static final String CACHE_HIT_BYTES_COUNTER = "pulsar.broker.managed_ledger.cache.hit.size";
    private final ObservableLongMeasurement cacheHitBytesCounter;

    // Replaces pulsar_ml_cache_misses_rate
    public static final String CACHE_MISS_COUNTER = "pulsar.broker.managed_ledger.cache.miss.count";
    private final ObservableLongMeasurement cacheMissCounter;

    // Replaces pulsar_ml_cache_misses_throughput
    public static final String CACHE_MISS_BYTES_COUNTER = "pulsar.broker.managed_ledger.cache.miss.size";
    private final ObservableLongMeasurement cacheMissBytesCounter;

    // Replaces ['pulsar_ml_cache_pool_active_allocations', 'pulsar_ml_cache_pool_active_allocations_huge',
    //           'pulsar_ml_cache_pool_active_allocations_normal', 'pulsar_ml_cache_pool_active_allocations_small']
    public static final String CACHE_POOL_ACTIVE_ALLOCATION_COUNTER =
            "pulsar.broker.managed_ledger.cache.pool.allocation.count";
    private final ObservableLongMeasurement cachePoolActiveAllocationCounter;

    // Replaces ['pulsar_ml_cache_pool_allocated', 'pulsar_ml_cache_pool_used', 'pulsar_ml_cache_pool_size']
    public static final String CACHE_POOL_ACTIVE_ALLOCATION_SIZE_COUNTER =
            "pulsar.broker.managed_ledger.cache.pool.allocation.size";
    private final ObservableLongMeasurement cachePoolActiveAllocationSizeCounter;

    private final BatchCallback batchCallback;

    public OpenTelemetryManagedLedgerCacheStats(ManagedLedgerFactoryImpl factory, OpenTelemetry openTelemetry) {
        var meter = openTelemetry.getMeter("pulsar.managed_ledger.cache");

        cacheEvictionCounterRate = meter
                .counterBuilder(CACHE_EVICTION_COUNTER_RATE)
                .setUnit("{eviction}")
                .setDescription("The total number of cache evictions.")
                .buildObserver();

        cacheInsertionCounter = meter
                .counterBuilder(CACHE_INSERTION_COUNTER)
                .setUnit("{insertion}")
                .setDescription("The total number of entries inserted into the entry cache.")
                .buildObserver();

        cacheEvictionCounter = meter
                .counterBuilder(CACHE_EVICTION_COUNTER)
                .setUnit("{eviction}")
                .setDescription("The total number of entries evicted from the entry cache.")
                .buildObserver();

        cacheEntryCounter = meter
                .upDownCounterBuilder(CACHE_ENTRY_COUNTER)
                .setUnit("{entry}")
                .setDescription("The number of entries in the entry cache.")
                .buildObserver();

        cacheHitCounter = meter
                .counterBuilder(CACHE_HIT_COUNTER)
                .setUnit("{entry}")
                .setDescription("The number of cache hits.")
                .buildObserver();

        cacheHitBytesCounter = meter
                .counterBuilder(CACHE_HIT_BYTES_COUNTER)
                .setUnit("{By}")
                .setDescription("The byte amount of data retrieved from cache hits.")
                .buildObserver();

        cacheMissCounter = meter
                .counterBuilder(CACHE_MISS_COUNTER)
                .setUnit("{entry}")
                .setDescription("The number of cache misses.")
                .buildObserver();

        cacheMissBytesCounter = meter
                .counterBuilder(CACHE_MISS_BYTES_COUNTER)
                .setUnit("{By}")
                .setDescription("The byte amount of data not retrieved due to cache misses.")
                .buildObserver();

        cachePoolActiveAllocationCounter = meter
                .upDownCounterBuilder(CACHE_POOL_ACTIVE_ALLOCATION_COUNTER)
                .setUnit("{allocation}")
                .setDescription("The number of currently active allocations in the direct arena.")
                .buildObserver();

        cachePoolActiveAllocationSizeCounter = meter
                .upDownCounterBuilder(CACHE_POOL_ACTIVE_ALLOCATION_SIZE_COUNTER)
                .setUnit("{By}")
                .setDescription("The memory allocated in the direct arena.")
                .buildObserver();

        batchCallback = meter.batchCallback(() -> recordMetrics(factory),
                cacheEvictionCounterRate,
                cacheInsertionCounter,
                cacheEvictionCounter,
                cacheEntryCounter,
                cacheHitCounter,
                cacheHitBytesCounter,
                cacheMissCounter,
                cacheMissBytesCounter,
                cachePoolActiveAllocationCounter,
                cachePoolActiveAllocationSizeCounter);
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private void recordMetrics(ManagedLedgerFactoryImpl factory) {
        var stats = factory.getMbean();

        cacheEvictionCounterRate.record(stats.getNumberOfCacheEvictionsTotal());

        var entriesOut = stats.getCacheEvictedEntriesCount();
        var entriesIn = stats.getCacheInsertedEntriesCount();
        var entriesActive = entriesIn - entriesOut;
        cacheInsertionCounter.record(stats.getCacheInsertedEntriesCount());
        cacheEvictionCounter.record(stats.getCacheEvictedEntriesCount());
        cacheEntryCounter.record(entriesActive);

        cacheHitCounter.record(stats.getCacheHitsTotal());
        cacheHitBytesCounter.record(stats.getCacheHitsBytesTotal());
        cacheMissCounter.record(stats.getCacheMissesTotal());
        cacheMissBytesCounter.record(stats.getCacheMissesBytesTotal());

        var allocatorStats = new PooledByteBufAllocatorStats(RangeEntryCacheImpl.ALLOCATOR);
        cachePoolActiveAllocationCounter.record(allocatorStats.activeAllocationsSmall, PoolArenaType.SMALL.attributes);
        cachePoolActiveAllocationCounter.record(allocatorStats.activeAllocationsNormal,
                PoolArenaType.NORMAL.attributes);
        cachePoolActiveAllocationCounter.record(allocatorStats.activeAllocationsHuge, PoolArenaType.HUGE.attributes);
        cachePoolActiveAllocationSizeCounter.record(allocatorStats.totalAllocated,
                PoolChunkAllocationType.ALLOCATED.attributes);
        cachePoolActiveAllocationSizeCounter.record(allocatorStats.totalUsed, PoolChunkAllocationType.USED.attributes);
    }
}