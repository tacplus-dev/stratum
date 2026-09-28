// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hazelcast;

import java.util.Collection;

import com.hazelcast.cluster.Member;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionFunction;

/**
 * Split-brain protection that counts WRITERS, not members.
 *
 * <p>Hazelcast's own {@code minimumClusterSize} counts everything in the view, and
 * with search members in the cluster that number no longer answers the question
 * the protection exists to ask. A partition holding one writer and five search
 * members satisfies "six members are here" while holding ONE durable copy, so a
 * minority partition of that shape would keep accepting writes at the cluster
 * level — the exact hole the protection was added to close, reopened by the
 * members that were added for query throughput.
 *
 * <p>The number compared against is {@code min_durable_copies}, the same
 * declaration the write gate enforces per record ({@code HotWrites}) and the same
 * one the restore barrier is decided on. The two now count the same population, so
 * a cluster cannot be refused by one and allowed by the other.
 *
 * @see StratumRole#writers
 */
public final class WriterCountProtection implements SplitBrainProtectionFunction {

    private final int minWriters;

    public WriterCountProtection(int minWriters) {
        this.minWriters = minWriters;
    }

    /** The writer floor this protection holds the cluster to. */
    public int minWriters() {
        return minWriters;
    }

    @Override
    public boolean apply(Collection<Member> members) {
        int writers = 0;
        for (Member m : members) {
            if (StratumRole.of(m).isWriter()) {
                writers++;
            }
        }
        return writers >= minWriters;
    }

    @Override
    public String toString() {
        return "WriterCountProtection{minWriters=" + minWriters + "}";
    }
}
