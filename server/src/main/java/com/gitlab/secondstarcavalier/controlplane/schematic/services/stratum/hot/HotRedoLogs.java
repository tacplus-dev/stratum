// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.Codec;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.RedoLog;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.StorePath;

/**
 * The node's own redo logs — one per hot record type, on this pod's volume.
 *
 * <h2>Per node, no leader</h2>
 * {@link RedoLog} is single-writer, single-process, local-filesystem only. Three
 * instances therefore own three logs, each satisfying that contract untouched,
 * and the cluster-level durability property is composed above the log rather
 * than inside it. Each log is PARTIAL — it holds the rows this node owned or
 * backed up — so recovery is the union of the reachable nodes' logs, resolved by
 * epoch ({@link HotRestore}).
 *
 * <h2>Why the node-local handle is reachable statically</h2>
 * A redo append rides the Hazelcast write as an {@link RedoAppendProcessor},
 * which is SERIALIZED to the partition owner and to its backup and deserialized
 * there. A deserialized entry processor carries no reference to anything on the
 * receiving member, so the only way its body can reach that member's own log is a
 * node-local lookup — this registry. It is set once at init and cleared at
 * shutdown; a second live instance in one JVM would be a second process by
 * RedoLog's contract, so {@link #install} refuses it rather than let two writers
 * share an append stream.
 */
public final class HotRedoLogs implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HotRedoLogs.class);

    /** Where the hot redo logs live under the pod's volume. */
    public static final String HOT_SUBDIR = "hot";

    /**
     * The node-local registry the entry-processor bodies resolve through. Not a
     * cache and not a singleton for convenience: see the class note.
     */
    private static final AtomicReference<HotRedoLogs> ACTIVE = new AtomicReference<>();

    private final Map<String, RedoLog> logs = new LinkedHashMap<>();

    /**
     * Opens one log per dataset under {@code dataRoot/hot/}, and installs this as
     * the node's active registry.
     */
    public HotRedoLogs(Path dataRoot, Iterable<HotDataset> datasets, int archiveCount, Codec codec) {
        Path base = dataRoot.resolve(HOT_SUBDIR);
        for (HotDataset d : datasets) {
            logs.put(d.mapName, RedoLog.open(base, StorePath.ROOT_TENANT, d.mapName, archiveCount, codec));
        }
        install(this);
        log.info("hot redo logs opened under {}: {}", base, logs.keySet());
    }

    private static void install(HotRedoLogs instance) {
        if (!ACTIVE.compareAndSet(null, instance)) {
            throw new IllegalStateException(
                    "a HotRedoLogs is already active in this JVM; a redo log is single-writer,"
                            + " single-process, so a second one would share an append stream with it");
        }
    }

    /**
     * This node's log for {@code mapName}, or null when the node keeps no redo log
     * for it. Called from entry-processor bodies on whichever member they landed
     * on — the primary resolves its own log, the backup resolves its own.
     */
    public static RedoLog logFor(String mapName) {
        HotRedoLogs active = ACTIVE.get();
        return active == null ? null : active.logs.get(mapName);
    }

    /** This node's log for {@code mapName}. */
    public RedoLog log(String mapName) {
        return logs.get(mapName);
    }

    @Override
    public void close() {
        ACTIVE.compareAndSet(this, null);
        for (RedoLog l : logs.values()) {
            l.close();
        }
    }
}
