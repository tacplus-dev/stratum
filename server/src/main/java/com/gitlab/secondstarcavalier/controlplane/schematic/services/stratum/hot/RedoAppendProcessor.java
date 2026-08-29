// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot;

import java.io.Serializable;
import java.util.Map;

import com.hazelcast.map.EntryProcessor;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.RedoLog;

/**
 * The hot write: set the value AND append it to the receiving node's own redo
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
 * same body. It resolves {@link HotRedoLogs#logFor} on the member it is running
 * on, so "append to this node's log" means the owner's log on the owner and the
 * backup's log on the backup, from one class.
 *
 * <h2>What is and is not on the ack path</h2>
 * The value's second in-memory copy IS: {@code backupCount=1, asyncBackupCount=0}
 * means the put does not complete until the backup operation has run. Whether the
 * backup body's {@code append} is part of that synchronous operation is a
 * Hazelcast behaviour, not a property this class can assert — see
 * {@code HotRedoBackupTimingTest}, which runs it against a real two-member
 * cluster rather than trusting the documentation.
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

    @Override
    public Boolean process(Map.Entry<String, V> entry) {
        entry.setValue(value);
        RedoLog log = HotRedoLogs.logFor(mapName);
        if (log == null) {
            // No redo log configured on this node for this type. The in-memory
            // copy still lands; the durability leg simply does not exist here,
            // which is the state of a node with no disk_persistence configured.
            return Boolean.FALSE;
        }
        // The epoch travels as the record's metadata, which is exactly what
        // RedoLog.Record.metadata is for ("optional, caller-set"). It is the same
        // number on both nodes because it was stamped once, by the ingesting node,
        // before this processor was serialized.
        log.append(new RedoLog.Record(entry.getKey(), false, Long.toString(epoch), payload));
        return Boolean.TRUE;
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
