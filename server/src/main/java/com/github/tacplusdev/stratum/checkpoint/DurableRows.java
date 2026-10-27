// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.checkpoint;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Message;
import com.hazelcast.core.HazelcastInstance;

import com.github.tacplusdev.stratum.disk.RecordDisk;
import com.github.tacplusdev.stratum.disk.RecordIndex;

/**
 * {@link DailyCheckpoint.Rows} over the union of the live Hazelcast maps and this
 * node's on-disk record indexes.
 *
 * <h2>Why the union and not either one</h2>
 * This replaced {@code HazelcastRows}, which read {@code hz.getMap(name)} alone.
 * That was correct only while nothing ever left memory, and it stopped being
 * correct the moment the per-entry max-idle clock was applied: a row evicted for
 * idleness is off-heap, so it was absent from the next dump, and step 3 of the
 * same transaction truncated the redo entries that still named it. The loss became
 * permanent at the next checkpoint and no cold start could recover it — the
 * incident that hotfixed the clock off
 * ({@code decisions/stratum-idle-eviction-durability-gap-2026-09-20.md}).
 *
 * <p>Disk alone would be the decision's own answer — "what is on disk after step
 * 1; memory is a subset" — and it is very nearly true. It is not exactly true for
 * one window: the disk write is queued off the ack path (see {@link RecordDisk}),
 * so a row written moments before the sweep is in memory and not yet on disk.
 * Dumping disk alone would drop it. The union costs one map merge per type and
 * cannot drop a row for either reason, so the dump is a superset of what either
 * leg would have given on its own and the checkpoint stays what it claims to be:
 * every row this pod holds.
 *
 * <p>Memory wins a key held by both. The two copies are the same row, and when
 * they briefly differ it is because the memory copy is the newer write that has
 * not reached the index yet.
 *
 * <h2>Scanning does not restamp the idle clock</h2>
 * {@code IMap.entrySet()} reads the resident entries without going through the
 * per-key access path, so the sweep does not keep the whole store hot by walking
 * it once a day, and it does not fire the read-through loader for keys that are
 * not resident either — which is why the disk leg has to be read directly rather
 * than by getting each key back.
 */
public final class DurableRows implements DailyCheckpoint.Rows {

    private final HazelcastInstance hz;
    private final RecordDisk disk;
    private final List<String> mapNames;

    /**
     * @param disk this node's on-disk leg, or null for a deployment that opened
     *             none — in which case this degrades to exactly what
     *             {@code HazelcastRows} did, which is also exactly right there:
     *             no disk leg means no idle clock either (see
     *             {@code StratumMapConfig.durableMap}), so memory holds everything.
     */
    public DurableRows(HazelcastInstance hz, RecordDisk disk, List<String> mapNames) {
        this.hz = hz;
        this.disk = disk;
        this.mapNames = List.copyOf(mapNames);
    }

    @Override
    public Collection<String> mapNames() {
        return mapNames;
    }

    @Override
    public Collection<Map.Entry<String, Message>> entries(String mapName) {
        Map<String, Message> union = new LinkedHashMap<>();
        RecordIndex index = disk == null ? null : disk.index(mapName);
        if (index != null) {
            try {
                union.putAll(index.entries());
            } catch (IOException e) {
                // The checkpoint's own contract: a transaction that aborts leaves
                // the previous checkpoint plus an un-truncated redo log, which is a
                // complete recovery set. Dumping what memory happened to hold
                // instead would commit a checkpoint that is silently short of every
                // row this index was the only copy of, and then truncate the redo
                // log behind it.
                throw new IllegalStateException("the disk leg for " + mapName + " could not be read;"
                        + " refusing to dump a checkpoint that would be missing every row that is"
                        + " currently off-heap", e);
            }
        }
        union.putAll(hz.<String, Message>getMap(mapName));
        return union.entrySet();
    }

    /**
     * {@inheritDoc}
     *
     * <p>The conditional remove is against MEMORY, and it reaches an idle-evicted
     * row because the map carries the read-through loader: Hazelcast loads the key
     * to compare it, finds the same bytes the scan above read off disk, and removes
     * it. So a row that is off-heap is purgeable on exactly the same terms as a
     * resident one, and a row republished under the sweep still fails the compare
     * and keeps its disk copy — which is what {@link DailyCheckpoint} reads a false
     * return as.
     */
    @Override
    public boolean remove(String mapName, String key, Message expected) {
        return hz.<String, Message>getMap(mapName).remove(key, expected);
    }
}
