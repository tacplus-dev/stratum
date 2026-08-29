// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot;

import java.io.Serializable;
import java.util.Map;

import com.hazelcast.map.EntryProcessor;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.RedoLog;

/**
 * The hot write: set the value AND queue it for the receiving node's own redo
 * log, on the partition owner and on its synchronous backup alike.
 *
 * <h2>Why an entry processor and not a put</h2>
 * "Two redo logs" only means anything if the two logs are on the two instances
 * that hold the value — otherwise losing one volume can lose a write that was
 * acked. An {@code IMap.put} gives the second in-memory copy and nothing else;
 * the redo leg would then have to pick its own second node, which is not the one
 * Hazelcast chose. Riding the replication is what makes the two sets identical by
 * construction.
 *
 * <p>{@link #getBackupProcessor()} returns {@code this}: the backup body is the
 * same body. It resolves {@link HotRedoLogs#appendAsync} on the member it is
 * running on, so "queue for this node's log" means the owner's log on the owner
 * and the backup's log on the backup, from one class.
 *
 * <h2>What is and is not on the ack path</h2>
 * The value's second in-memory copy IS: {@code backupCount=1, asyncBackupCount=0}
 * means the put does not complete until the backup operation has run, and the
 * backup body runs inside that operation — see {@code HotRedoBackupTimingTest},
 * which proves it against a real two-member cluster rather than trusting the
 * documentation. That in-memory replication is the entire ack condition.
 *
 * <p>The disk append is NOT on it. Both bodies hand the record to the node's
 * single append thread and return; neither waits for the file write. See
 * {@link HotRedoLogs} for the ordering that buys and the window it opens, and
 * {@code HotRedoAckLatencyTest} for the assertion that a stalled writer does not
 * stall the write.
 */
public final class RedoAppendProcessor<V> implements EntryProcessor<String, V, Boolean>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String mapName;
    private final V value;
    private final String payload;
    private final long epoch;

    public RedoAppendProcessor(String mapName, V value, String payload, long epoch) {
        this.mapName = mapName;
        this.value = value;
        this.payload = payload;
        this.epoch = epoch;
    }

    /** @return true when this node queued a redo append for the write. */
    @Override
    public Boolean process(Map.Entry<String, V> entry) {
        entry.setValue(value);
        // The epoch travels as the record's metadata, which is exactly what
        // RedoLog.Record.metadata is for ("optional, caller-set"). It is the same
        // number on both nodes because it was stamped once, by the ingesting node,
        // before this processor was serialized.
        //
        // Queued, not written: appendAsync returns before the record reaches disk,
        // so the in-memory mutation above is all the write waits on. A node with no
        // log for this type queues nothing and says so — the in-memory copy still
        // lands, the durability leg simply does not exist there.
        return HotRedoLogs.appendAsync(mapName,
                new RedoLog.Record(entry.getKey(), false, Long.toString(epoch), payload));
    }

    @Override
    public EntryProcessor<String, V, Boolean> getBackupProcessor() {
        return this;
    }

    /** The store epoch stamped into the appended record. */
    public long epoch() {
        return epoch;
    }
}
