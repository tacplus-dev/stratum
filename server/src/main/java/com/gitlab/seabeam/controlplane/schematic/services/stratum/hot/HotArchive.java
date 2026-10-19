// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hot;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.checkpoint.DailyCheckpoint;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.hazelcast.StratumRole;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.ArchiveRestore;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.ArchiveShipper;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.ArchiveSink;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.Codec;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.RedoLog;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.RollSchedule;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.sys.diskstore.Diskstore.ArchiveConfig;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.sys.diskstore.Diskstore.DiskPersistenceConfig;

/**
 * This writer's hot redo logs shipped off the volume, through the generated
 * diskstore archive ({@code grpcconfig.disk_persistence.archive}): every roll
 * ships the rolled log as a segment, a daily checkpoint ships the full dataset,
 * and {@link #restore} rebuilds a writer from the newest checkpoint plus the
 * segments after it ({@code decisions/stratum-backup-redo-2026-10-18.md}).
 *
 * <p>Stratum opens its hot logs itself ({@link HotRedoLogs}), not through the
 * ServiceManager's service_store handles, so the generated ServiceManager never
 * attaches the archive to them. This class does the same composition with the
 * same generated pieces — {@link ArchiveShipper}, {@link RollSchedule},
 * {@link ArchiveRestore} — and nothing else: no object-store or file I/O is
 * written here.
 *
 * <h2>Writers only</h2>
 * Writers and search members run the same image against the same config, so the
 * archive block reaches both. Only a writer has a log to ship
 * ({@link #configFor}); a search member ignores it.
 *
 * <h2>The member id is the pod name</h2>
 * The archive is laid out per member, {@code <prefix>/<member>/<store>/}, and a
 * restore has to name the member whose chain it reads. The ServiceManager's
 * instance id is assigned by the kernel at every boot, so a restarted writer
 * would start a new chain under a new name each time. The StatefulSet pod name
 * ({@code stratum-0}) is the writer's stable identity — the volume belongs to it
 * — so a restarted writer continues its own chain and a restore names it.
 *
 * <h2>Rolls carry the whole dataset</h2>
 * {@link RedoLog#roll} folds its snapshot into the new local baseline, and the
 * shipped segment is the old active log, which leaves the volume. The snapshot is
 * therefore every row this writer holds ({@link DailyCheckpoint.Rows}, memory and
 * disk), never empty: an empty one would drop every write since the last
 * {@code DailyCheckpoint} dump from the local log, and the local cold start with
 * it. The cost of that at a short roll interval is measured in
 * {@code workers/stratum-archive-result.md}.
 */
public final class HotArchive implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HotArchive.class);

    /** The env var holding this writer's pod name, which is its archive member id. */
    public static final String MEMBER_ENV = "POD_NAME";

    /**
     * The env var that asks a writer to restore its hot logs, at boot, from the
     * archive chain of the member it names.
     */
    public static final String RESTORE_ENV = "STRATUM_ARCHIVE_RESTORE_MEMBER";

    private final List<ScheduledExecutorService> schedulers = new ArrayList<>();

    /**
     * The archive a member of {@code role} ships to, or null when it ships none: a
     * search member has no log, and a writer ships only when the archive is set.
     */
    public static ArchiveConfig configFor(StratumRole role, DiskPersistenceConfig persistence) {
        return role.isWriter() && persistence.hasArchive() ? persistence.getArchive() : null;
    }

    /** This writer's archive member id: its pod name (see the class note). */
    public static String member() {
        String pod = System.getenv(MEMBER_ENV);
        if (pod == null || pod.isBlank()) {
            throw new IllegalStateException("grpcconfig.disk_persistence.archive is set but " + MEMBER_ENV
                    + " is not; the archive member id is this writer's pod name");
        }
        return pod.trim();
    }

    /**
     * Restores every dataset of {@code member}'s chain into this writer's logs,
     * which then replay into memory through the ordinary cold start
     * ({@link HotRestore#replay}). Runs only onto empty logs: a log that already
     * holds records is left alone and reported, because a restore is for a writer
     * whose volume is gone.
     *
     * @return records restored, across all datasets
     */
    public static int restore(ArchiveSink sink, String prefix, String member,
                              Iterable<HotDataset> datasets, HotRedoLogs logs) throws IOException {
        int restored = 0;
        for (HotDataset d : datasets) {
            RedoLog redoLog = logs.log(d.mapName);
            AtomicInteger held = new AtomicInteger();
            redoLog.restore(rec -> held.incrementAndGet());
            if (held.get() > 0) {
                log.warn("archive restore from {} skipped for {}: this volume already holds {} record(s);"
                        + " a restore runs only onto an empty volume (unset {})",
                        member, d.mapName, held.get(), RESTORE_ENV);
                continue;
            }
            // Appended through the log's one writer thread, not here: peers may already
            // be sending this member persist tasks for the same log.
            int n = ArchiveRestore.restore(sink, prefix, member, d.mapName,
                    rec -> HotRedoLogs.appendAsync(d.mapName, rec));
            log.info("archive restore: {} record(s) of {} restored from {}", n, d.mapName, member);
            restored += n;
        }
        try {
            logs.awaitAppends(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while writing the restored records", e);
        }
        return restored;
    }

    /**
     * Attaches a shipper to every dataset's log. Call before the first roll, so
     * every roll — including {@code DailyCheckpoint}'s truncation — ships.
     * Segments and checkpoints are compressed with {@code codec} in the archive.
     */
    public static void attach(ArchiveSink sink, String prefix, String member, Codec codec,
                              Iterable<HotDataset> datasets, HotRedoLogs logs) {
        for (HotDataset d : datasets) {
            logs.log(d.mapName).attachShipper(new ArchiveShipper(sink, prefix, member, d.mapName, codec));
        }
    }

    /**
     * Starts the roll and checkpoint schedules for every dataset's log. Call only
     * once the hot tier has been restored: a snapshot taken from a store still
     * filling would fold a partial baseline into the log.
     */
    public HotArchive(Iterable<HotDataset> datasets, HotRedoLogs logs, DailyCheckpoint.Rows rows,
                      String member, Duration rollInterval, Duration checkpointInterval) {
        for (HotDataset d : datasets) {
            RedoLog redoLog = logs.log(d.mapName);
            String key = member + "/" + d.mapName;
            schedulers.add(RollSchedule.runRollScheduler(redoLog, rollInterval, key, () -> snapshot(rows, d)));
            schedulers.add(RollSchedule.runCheckpointScheduler(
                    redoLog, checkpointInterval, key, () -> snapshot(rows, d)));
        }
        log.info("hot archive shipping as {}: roll every {}, checkpoint every {}",
                member, rollInterval, checkpointInterval);
    }

    /** Every row of {@code d} this writer holds, as the records its log carries. */
    static List<RedoLog.Record> snapshot(DailyCheckpoint.Rows rows, HotDataset d) {
        List<RedoLog.Record> out = new ArrayList<>();
        for (Map.Entry<String, Message> e : rows.entries(d.mapName)) {
            Message value = e.getValue();
            out.add(new RedoLog.Record(e.getKey(), false,
                    Long.toString(d.epoch.applyAsLong(value)), d.toJson.apply(value)));
        }
        return out;
    }

    @Override
    public void close() {
        for (ScheduledExecutorService s : schedulers) {
            s.shutdownNow();
        }
    }
}
