// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.protobuf.Message;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmDataset;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmResidency;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmStore;

/**
 * Drops warm rows from the Hazelcast map once the disk leg owns them — the
 * reclaim half of the residency boundary.
 *
 * <h2>Why this is not what makes the count right</h2>
 * The read path does NOT depend on this having run. {@link WarmResidency} cuts the
 * memory leg at the same boundary on every query, so a row that has aged out but
 * has not been swept yet is already served by the disk leg alone and counted once.
 * That has to be true independently: this sweep runs on the retention interval, and
 * a correctness property that only held between sweeps would be a property that is
 * false most of the time. What the sweep buys is the memory back — an aged row that
 * nothing reads is still occupying the map and its backups until it is removed.
 *
 * <h2>What it will not evict</h2>
 * A row is dropped only when its epoch is past the boundary, its asynchronous disk
 * write is not still in flight, AND a slice on this node's disk actually holds the
 * key. The third condition is the one that matters: "no longer pending" is also
 * true of a write that FAILED, and evicting on that would delete the only copy this
 * node can serve. The conditional {@code remove(key, value)} adds the last case — a
 * row re-written between the read and the remove has a fresh epoch and is inside
 * the window again, so the remove must not fire on it.
 *
 * <h2>One sweeper, node-local evidence</h2>
 * The map is cluster-wide, so one member sweeps it, exactly as the retention
 * cleanup does. The disk evidence is necessarily the sweeping node's own — each
 * instance holds a full local replica, and asking the others would be a
 * scatter-gather on the cleanup path. That is sound because the boundary is a whole
 * residency window behind now, orders of magnitude past the async write window: a
 * peer still missing the row at that age did not lose a race, it failed the write,
 * which {@code WarmStore.writeFailureCount} counts and which the event log repairs.
 */
final class WarmEviction {

    private static final Logger log = LoggerFactory.getLogger(WarmEviction.class);

    private WarmEviction() {}

    /**
     * Runs one sweep over every warm dataset.
     *
     * @return rows evicted, by dataset name; datasets that gave up nothing are absent
     */
    static Map<String, Long> sweep(HazelcastInstance hz, WarmStore warm) {
        Map<String, Long> evicted = new LinkedHashMap<>();
        for (String name : warm.datasetNames()) {
            try {
                long n = sweepDataset(hz, warm, name);
                if (n > 0) {
                    evicted.put(name, n);
                }
            } catch (Exception e) {
                log.error("warm eviction failed for {}", name, e);
            }
        }
        return evicted;
    }

    private static long sweepDataset(HazelcastInstance hz, WarmStore warm, String name)
            throws Exception {
        WarmDataset dataset = warm.index(name).dataset();
        WarmResidency residency = warm.residency(name);
        // Dataset name IS the Hazelcast map name — one identifier names the type in
        // both tiers, which is why nothing here has to be generated per record type.
        IMap<String, Message> map = hz.getMap(name);

        // Server-side narrowing on the same field the descriptor reads, so the sweep
        // pulls back candidates rather than the whole map. Hazelcast resolves the
        // attribute by reflecting for the JavaBean getter, so it is the camelCase
        // property name, not the proto field name — the same translation the
        // generated retention sweep makes.
        long n = 0;
        for (Map.Entry<String, Message> entry
                : map.entrySet(Predicates.lessThan(camel(dataset.epochField), residency.boundaryMs))) {
            String key = entry.getKey();
            Message row = entry.getValue();
            // Re-read the epoch through the descriptor rather than trusting the
            // predicate's reflection: the descriptor is what the read path's cut uses,
            // and eviction disagreeing with it by even one row is a lost row.
            if (!residency.evictable(key, dataset.epochMs(row))) {
                continue;
            }
            if (!warm.isDurableOnDisk(name, key)) {
                log.warn("warm eviction skipped {} key={}: past the residency boundary but this"
                        + " node's disk index does not hold it", name, key);
                continue;
            }
            if (map.remove(key, row)) {
                n++;
            }
        }
        if (n > 0) {
            log.info("warm eviction: {} row(s) of {} released from memory to the disk leg", n, name);
        }
        return n;
    }

    /** snake_case proto field -> lowerCamelCase property (store_epoch_ms -> storeEpochMs). */
    private static String camel(String snake) {
        StringBuilder b = new StringBuilder();
        boolean up = false;
        for (char c : snake.toCharArray()) {
            if (c == '_') { up = true; continue; }
            b.append(up ? Character.toUpperCase(c) : c);
            up = false;
        }
        return b.toString();
    }
}
