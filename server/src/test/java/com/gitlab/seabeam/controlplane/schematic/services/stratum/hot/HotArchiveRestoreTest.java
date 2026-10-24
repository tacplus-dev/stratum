// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import com.google.protobuf.Message;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.checkpoint.DurableRows;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.ArchiveSink;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.Codec;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.RedoLog;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.services.letsencrypt.Letsencrypt;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.services.letsencrypt.Helpers;

/**
 * A writer whose volume is gone comes back from the archive: write, roll,
 * checkpoint, roll again, ship, wipe the volume, restore into an empty volume on
 * a fresh member, and read back exactly the rows that were written — through the
 * generated shipper and restore, against an in-memory {@link ArchiveSink} in place
 * of the bucket.
 */
class HotArchiveRestoreTest {

    private static final String MAP = "letsencrypt.LetsEncryptCert";
    private static final String PREFIX = "stratum";
    private static final String MEMBER = "stratum-0";

    private HazelcastInstance hz;
    private HotRedoLogs logs;

    @AfterEach
    void tearDown() {
        if (logs != null) {
            logs.close();
            logs = null;
        }
        if (hz != null) {
            hz.shutdown();
            hz = null;
        }
    }

    /** The bucket, in memory. */
    private static final class MemSink implements ArchiveSink {
        final Map<String, byte[]> objects = new TreeMap<>();

        @Override
        public synchronized void put(String name, byte[] data) {
            objects.put(name, data.clone());
        }

        @Override
        public synchronized byte[] get(String name) {
            byte[] data = objects.get(name);
            return data == null ? null : data.clone();
        }

        @Override
        public synchronized List<String> list(String prefix) {
            List<String> names = new ArrayList<>();
            for (String name : objects.keySet()) {
                if (name.startsWith(prefix)) {
                    names.add(name);
                }
            }
            return names;
        }

        synchronized List<String> names(String part) {
            List<String> names = new ArrayList<>();
            for (String name : objects.keySet()) {
                if (name.contains(part)) {
                    names.add(name);
                }
            }
            return names;
        }
    }

    private static HotDataset dataset() {
        return new HotDataset(MAP, 1,
                m -> ((Letsencrypt.LetsEncryptCert) m).getCertSetId(),
                m -> ((Letsencrypt.LetsEncryptCert) m).getIssuedAt(),
                "issued_at",
                m -> {
                    try {
                        return Helpers.letsEncryptCertToJson((Letsencrypt.LetsEncryptCert) m);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(e);
                    }
                },
                json -> {
                    try {
                        return Helpers.letsEncryptCertFromJson(json);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                });
    }

    private static Letsencrypt.LetsEncryptCert event(String id, long epochMs, String session) {
        return Letsencrypt.LetsEncryptCert.newBuilder()
                .setCertSetId(id)
                .setIssuedAt(epochMs)
                .setSpiffeId(session)
                .build();
    }

    private static HazelcastInstance member(String cluster) {
        Config cfg = new Config();
        cfg.setClusterName(cluster);
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        return Hazelcast.newHazelcastInstance(cfg);
    }

    private static void write(HotWrites hot, IMap<String, Message> map, Letsencrypt.LetsEncryptCert e) {
        hot.write(map, MAP, e.getCertSetId(), e, 1, () -> Helpers.letsEncryptCertToJson(e), e.getIssuedAt());
    }

    private static void awaitShipped(RedoLog redoLog) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (redoLog.shipPending() > 0) {
            assertTrue(System.currentTimeMillis() < deadline, "the archive did not drain");
            Thread.sleep(10);
        }
    }

    private static void wipe(Path volume) throws IOException {
        try (Stream<Path> files = Files.walk(volume)) {
            for (Path p : files.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }

    @Test
    void aWipedWriterIsRestoredFromTheNewestCheckpointPlusTheSegmentsAfterIt(@TempDir Path dir)
            throws Exception {
        HotDataset d = dataset();
        MemSink sink = new MemSink();
        Path volume = dir.resolve("stratum-0");

        hz = member("stratum-archive-source");
        IMap<String, Message> map = hz.getMap(MAP);
        logs = new HotRedoLogs(volume, List.of(d), 3, Codec.NONE);
        HotArchive.attach(sink, PREFIX, MEMBER, Codec.GZ, List.of(d), logs);
        HotWrites hot = new HotWrites(hz, 0);
        DurableRows rows = new DurableRows(hz, null, List.of(MAP));
        RedoLog redoLog = logs.log(MAP);

        // Before the checkpoint: segment 1.
        write(hot, map, event("evt-1", 1000L, "s1"));
        write(hot, map, event("evt-2", 1001L, "s1"));
        logs.awaitAppends(5_000);
        redoLog.roll(HotArchive.snapshot(rows, d));
        redoLog.checkpoint(() -> HotArchive.snapshot(rows, d));

        // After it: a new row and an update in segment 2, and another row in segment 3.
        write(hot, map, event("evt-3", 1002L, "s2"));
        write(hot, map, event("evt-1", 2000L, "s1-updated"));
        logs.awaitAppends(5_000);
        redoLog.roll(HotArchive.snapshot(rows, d));
        write(hot, map, event("evt-4", 1003L, "s2"));
        logs.awaitAppends(5_000);
        redoLog.roll(HotArchive.snapshot(rows, d));
        awaitShipped(redoLog);

        Map<String, Message> written = new HashMap<>(map);
        assertEquals(4, written.size());
        assertEquals(1, sink.names("/checkpoints/").size(), sink.objects.keySet().toString());
        assertEquals(3, sink.names("/segments/").size(), sink.objects.keySet().toString());
        // The checkpoint covers segment 1, so a restore must not need it: drop it.
        String first = sink.names("/segments/").get(0);
        assertTrue(first.startsWith(PREFIX + "/" + MEMBER + "/" + MAP + "/segments/"), first);
        sink.objects.remove(first);

        // The volume and the member are gone.
        logs.close();
        logs = null;
        hz.shutdown();
        wipe(volume);

        hz = member("stratum-archive-restored");
        logs = new HotRedoLogs(dir.resolve("stratum-0-fresh"), List.of(d), 3, Codec.NONE);
        int restored = HotArchive.restore(sink, PREFIX, MEMBER, List.of(d), logs);
        HotRestore.Result replay = HotRestore.replay(logs.log(MAP), d, hz.getMap(MAP));

        assertEquals(4, restored);
        assertEquals(4, replay.applied);
        assertEquals(written, new HashMap<>(hz.<String, Message>getMap(MAP)));
        assertEquals("s1-updated", ((Letsencrypt.LetsEncryptCert) hz.getMap(MAP).get("evt-1")).getSpiffeId(),
                "the update after the checkpoint must win over the checkpointed row");
    }

    @Test
    void aRestoreLeavesAVolumeThatAlreadyHoldsALogAlone(@TempDir Path dir) throws Exception {
        HotDataset d = dataset();
        MemSink sink = new MemSink();
        hz = member("stratum-archive-nonempty");
        logs = new HotRedoLogs(dir, List.of(d), 3, Codec.NONE);
        HotArchive.attach(sink, PREFIX, MEMBER, Codec.GZ, List.of(d), logs);
        IMap<String, Message> map = hz.getMap(MAP);
        write(new HotWrites(hz, 0), map, event("evt-1", 1000L, "s1"));
        logs.awaitAppends(5_000);
        logs.log(MAP).roll(HotArchive.snapshot(new DurableRows(hz, null, List.of(MAP)), d));
        awaitShipped(logs.log(MAP));
        assertFalse(sink.objects.isEmpty(), "the roll must have shipped a segment");

        // This volume still holds evt-1 in its log; restoring the archive onto it again
        // would append the archived rows behind it.
        assertEquals(0, HotArchive.restore(sink, PREFIX, MEMBER, List.of(d), logs));
    }
}
