// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;

/**
 * What one stratum member is FOR: durability, or query throughput.
 *
 * <p>Every member of the cluster holds partitions and answers the parallel
 * scatter-gather a predicate query is. The role says what a member does BESIDES
 * that, and the whole of the difference is disk:
 *
 * <ul>
 *   <li>{@link #WRITER} — appends every write cluster-wide to its own redo log,
 *       keeps a per-type record index, takes part in the daily checkpoint
 *       transaction, and is counted for {@code min_durable_copies}, for the
 *       hot-restore barrier and as a restore contributor. A static count, sized
 *       for durability (2 or 3), each with a volume.</li>
 *   <li>{@link #SEARCH} — partitions in memory and nothing else. No volume, no
 *       redo log, no record index, no checkpoint; never a durable copy and never a
 *       restore contributor. A dynamic count, scaled up and down for query
 *       throughput. A search member that leaves loses nothing durable.</li>
 * </ul>
 *
 * <p>The role travels as a Hazelcast MEMBER ATTRIBUTE ({@link #ATTRIBUTE}) rather
 * than being inferred from anything: the members that count for durability have to
 * be identifiable from any other member's cluster view, and a member's own
 * deployment is the only thing that knows whether it was given a volume.
 *
 * <p>A member with no attribute reads as {@link #WRITER}. That is the direction
 * that cannot invent durability: every member of a cluster running code that
 * predates this attribute is in fact a writer, so reading absence as "writer" is
 * exactly right for it, while reading it as "search" would make a healthy cluster
 * look like it had no durable copies at all and refuse every write.
 */
public enum StratumRole {

    WRITER,
    SEARCH;

    /** The Hazelcast member attribute carrying the role. */
    public static final String ATTRIBUTE = "stratum.role";

    /** The environment variable one pod's role is declared in. */
    public static final String ENV = "STRATUM_MEMBER_ROLE";

    /**
     * This pod's declared role, {@link #WRITER} when nothing declares one.
     *
     * <p>Writer is the default because it is the role that needs a volume and the
     * operator mounts one for the {@code stratum} StatefulSet without being asked;
     * a pod that forgot to declare anything is therefore a pod with a volume, and
     * the safe reading of a missing declaration is the role that uses it. The
     * volume-less search workload is the one the operator has to say something
     * about, and it does.
     */
    public static StratumRole configured() {
        return parse(System.getenv(ENV), ENV);
    }

    /**
     * The role {@code member} declared, {@link #WRITER} when it declared none —
     * see the class note for why absence is writer and not search.
     */
    public static StratumRole of(Member member) {
        return parse(member.getAttribute(ATTRIBUTE), "member attribute " + ATTRIBUTE);
    }

    private static StratumRole parse(String raw, String source) {
        if (raw == null || raw.isBlank()) {
            return WRITER;
        }
        String name = raw.trim().toUpperCase(Locale.ROOT);
        for (StratumRole role : values()) {
            if (role.name().equals(name)) {
                return role;
            }
        }
        throw new IllegalArgumentException(source + " must be one of WRITER, SEARCH (case-insensitive),"
                + " got \"" + raw + "\"");
    }

    /** True for the role that keeps a volume. */
    public boolean isWriter() {
        return this == WRITER;
    }

    /**
     * The members of the current cluster view that are writers.
     *
     * <p>This is the population every durability number in stratum is counted
     * over: the write gate's durable copies, the split-brain minimum, the
     * formation wait and the restore barrier. Search members are members in every
     * other respect and are deliberately absent from all four — a cluster of one
     * writer and five search members has one durable copy, not six.
     */
    public static List<Member> writers(HazelcastInstance hz) {
        List<Member> writers = new ArrayList<>();
        for (Member m : hz.getCluster().getMembers()) {
            if (of(m).isWriter()) {
                writers.add(m);
            }
        }
        return writers;
    }

    /** {@link #writers} as UUID strings, for the maps that are keyed by member UUID. */
    public static Set<String> writerUuids(HazelcastInstance hz) {
        Set<String> uuids = new HashSet<>();
        for (Member m : writers(hz)) {
            uuids.add(m.getUuid().toString());
        }
        return uuids;
    }

    /** How many writers the cluster view holds. */
    public static int writerCount(HazelcastInstance hz) {
        return writers(hz).size();
    }
}
