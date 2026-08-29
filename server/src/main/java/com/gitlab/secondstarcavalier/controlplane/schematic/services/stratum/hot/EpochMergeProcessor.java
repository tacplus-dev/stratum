// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot;

import java.io.Serializable;
import java.util.Map;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.hazelcast.map.EntryProcessor;

/**
 * The restore union's merge rule, applied at the partition owner: highest store
 * epoch per key wins.
 *
 * <h2>Why this is not last-writer-wins convergence</h2>
 * Each node's redo log is partial, so recovery unions the reachable nodes' logs.
 * {@code RedoLog.dedupBytes}'s "later lines win" is a true causal order WITHIN one
 * node's log — one synchronized writer, file position is write order — and
 * meaningless ACROSS two. The epoch is not a tiebreak invented for the merge: the
 * atomic write path serialized every accepted write to a key through its
 * partition owner, so at most one accepted version exists per (key, epoch) and
 * this replay just re-runs an order that was already decided at write time.
 *
 * <p>Which is why the equal-epoch case FAILS CLOSED. Two records for one key at
 * one epoch with different bytes cannot happen if the write path held; seeing it
 * is evidence that it did not, and picking a winner there would silently convert
 * stratum's atomic store into a convergent one. It is reported, named, and the
 * service refuses to start.
 */
public final class EpochMergeProcessor implements EntryProcessor<String, Message, Integer>, Serializable {

    private static final long serialVersionUID = 1L;

    /** The candidate was applied — no existing row, or a strictly newer epoch. */
    public static final int APPLIED = 1;
    /** An existing row at an equal or higher epoch already covers this key. */
    public static final int SKIPPED = 2;
    /** Same key, same epoch, different bytes: the write path was violated. */
    public static final int DIVERGED = 3;

    private final String epochField;
    private final Message candidate;
    private final long candidateEpoch;
    private final boolean deleted;

    public EpochMergeProcessor(String epochField, Message candidate, long candidateEpoch, boolean deleted) {
        this.epochField = epochField;
        this.candidate = candidate;
        this.candidateEpoch = candidateEpoch;
        this.deleted = deleted;
    }

    @Override
    public Integer process(Map.Entry<String, Message> entry) {
        Message existing = entry.getValue();
        if (existing == null) {
            if (deleted) {
                return SKIPPED;
            }
            entry.setValue(candidate);
            return APPLIED;
        }
        long existingEpoch = epochOf(existing);
        if (candidateEpoch > existingEpoch) {
            entry.setValue(deleted ? null : candidate);
            return APPLIED;
        }
        if (candidateEpoch < existingEpoch) {
            return SKIPPED;
        }
        // Equal epoch: identical is the NORMAL case — one write appended to the
        // owner's log and to the backup's log replays twice — and is idempotent.
        // A tombstone at the same epoch as a surviving row is not identical: the
        // matching tombstone would have removed the row already, so the two logs
        // disagree about one write, which is the same violation as differing bytes.
        if (!deleted && existing.equals(candidate)) {
            return SKIPPED;
        }
        return DIVERGED;
    }

    /**
     * The row's store epoch, read by proto reflection off the declared
     * (spec.store_epoch) field — a field NAME travels to the partition owner,
     * where a lambda would not.
     */
    private long epochOf(Message msg) {
        Descriptors.FieldDescriptor fd = msg.getDescriptorForType().findFieldByName(epochField);
        if (fd == null) {
            throw new IllegalStateException(msg.getDescriptorForType().getFullName()
                    + " has no field " + epochField + " carrying (spec.store_epoch)");
        }
        return ((Number) msg.getField(fd)).longValue();
    }
}
