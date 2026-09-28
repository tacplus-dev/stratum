// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hot;

import java.io.Serializable;

import com.google.protobuf.Message;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.disk.RecordDisk;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.RedoLog;

/**
 * One write's DISK legs, delivered to a writer member: queue the redo append, queue
 * the record-index write, return.
 *
 * <h2>Why every writer and not the partition's replicas</h2>
 * A writer node holds the whole store on disk regardless of which member owns the
 * partition. That is what makes durability independent of memory redundancy, and it
 * is the property search members exist on top of: a partition's primary and backup
 * may both sit on members with no volume, and the write is still durable on
 * {@code min_durable_copies} writers because the writers were sent it directly.
 *
 * <p>This replaced riding the partition replication. That earlier arrangement —
 * the append as an entry processor whose backup body ran on the synchronous backup —
 * made "the two logs are on the two instances that hold the value" true by
 * construction, and it tied the number of logs to {@code backupCount}, which is
 * exactly the coupling the member roles remove. With a search member able to own a
 * partition, riding the replication means a write whose replicas are all
 * volume-less is logged nowhere at all.
 *
 * <p>What it costs, named: the ordering that came free from one partition thread is
 * gone, because each writer is reached over its own connection. The redo log does
 * not care — {@code HotRestore} resolves its records by {@code (spec.store_epoch)}
 * on the way in, so a log holding one key's two versions in either order recovers
 * the same row. The record index does care, and {@code RecordDisk.writeAsync}
 * carries the guard: an older row never overwrites a newer one there.
 *
 * <h2>Fire and forget, like the appends it queues</h2>
 * Submitted with {@code IExecutorService.executeOnMembers}, which does not wait, and
 * both legs it runs are themselves queues. That is the same discipline the append
 * has always had — the ack is the in-memory replication, disk is not on it — moved
 * one hop out: the window that used to be "killed between the ack and the local
 * queue" is now "killed between the ack and the remote queue". What bounds it is
 * unchanged: every writer was sent the same task, so the write survives while any
 * one of them queued it, and it is on the event log besides.
 */
public final class RecordPersistTask implements Runnable, Serializable {

    private static final long serialVersionUID = 1L;

    private final String mapName;
    private final String key;
    /** The record itself, for the index leg. Null when the value is not a protobuf. */
    private final Message value;
    /** The redo payload, computed once on the ingesting node. Null when no log keeps the type. */
    private final String payload;
    private final long epoch;

    public RecordPersistTask(String mapName, String key, Message value, String payload, long epoch) {
        this.mapName = mapName;
        this.key = key;
        this.value = value;
        this.payload = payload;
        this.epoch = epoch;
    }

    @Override
    public void run() {
        // Each leg resolves this member's own registry and answers false when there
        // is none — the state of a member with no volume, which this task should not
        // have been sent to, and of a type this deployment keeps no log for, which it
        // legitimately is.
        if (value != null) {
            RecordDisk.writeAsync(mapName, key, value);
        }
        if (payload != null) {
            // The epoch travels as the record's metadata, which is what
            // RedoLog.Record.metadata is for ("optional, caller-set"). It is the same
            // number on every writer because it was stamped once, by the ingesting
            // node, before this task was serialized.
            HotRedoLogs.appendAsync(mapName, new RedoLog.Record(key, false, Long.toString(epoch), payload));
        }
    }

    /** The store epoch stamped into the appended record. */
    public long epoch() {
        return epoch;
    }
}
