// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.disk;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.protobuf.Message;
import com.hazelcast.map.MapLoader;

/**
 * The read-through leg behind one record map: a Hazelcast {@link MapLoader} whose
 * store is that type's {@link RecordIndex}.
 *
 * <p>This is the half of the 2026-09-20 record lifecycle decision that was
 * declared and never built, and whose absence made per-entry max-idle destructive
 * rather than a cache miss (see
 * {@code decisions/stratum-idle-eviction-durability-gap-2026-09-20.md}). With it
 * wired, a row evicted after {@code idle_seconds} of no touch is off-heap and
 * nothing more: the next {@code get} misses in memory, Hazelcast calls
 * {@link #load}, and the row comes back from disk as the concrete protobuf type
 * the maps and the query plane expect.
 *
 * <h2>A LOADER, not a store</h2>
 * {@link MapLoader} and not {@code MapStore}: the write leg is not Hazelcast's to
 * run. Writes ride {@code RecordPersistTask} out to every WRITER member, so each
 * writer's index holds the whole store and a per-pod checkpoint dump is a complete
 * dump. A MapStore would write from the partition owner only, on Hazelcast's
 * schedule — and the owner may be a search member with no volume at all.
 *
 * <h2>Why {@link #loadAllKeys} answers null</h2>
 * Null is Hazelcast's "there is no key set to preload", and it is the whole point.
 * The alternative — hand back every key on disk — makes the map eagerly reload the
 * entire type at startup, which is a cold start out of the wrong file (the
 * checkpoint is the baseline, see {@code EventStore.loadCheckpoint}) and, worse,
 * undoes idle eviction every time the map is opened. This leg is read-through and
 * nothing else.
 *
 * <h2>What does and does not trigger a load</h2>
 * The decision's "what counts as a read" rule falls out of Hazelcast's own
 * behaviour rather than being enforced here, which is why it is stated here:
 * {@code get} and {@code containsKey} miss into {@link #load}, while {@code size}
 * and a {@code values()} scan do not — they run over the resident entries only. So
 * {@code Count} and a predicate that matches nothing pull nothing off disk, and a
 * dashboard counting a dataset does not keep it hot.
 */
public final class RecordDiskLoader implements MapLoader<String, Message> {

    private final String mapName;

    public RecordDiskLoader(String mapName) {
        this.mapName = mapName;
    }

    /** The record type this loader answers for. */
    public String mapName() {
        return mapName;
    }

    @Override
    public Message load(String key) {
        return RecordDisk.load(mapName, key);
    }

    /**
     * The rows held for {@code keys}, skipping the ones this node's index does not
     * hold.
     *
     * <p>A missing key is an omission, never a null value: Hazelcast reads a null
     * value in the returned map as a row whose value is null and rejects it.
     */
    @Override
    public Map<String, Message> loadAll(Collection<String> keys) {
        Map<String, Message> out = new LinkedHashMap<>();
        for (String key : keys) {
            Message row = RecordDisk.load(mapName, key);
            if (row != null) {
                out.put(key, row);
            }
        }
        return out;
    }

    /** See the class note: read-through only, so there is nothing to preload. */
    @Override
    public Iterable<String> loadAllKeys() {
        return null;
    }
}
