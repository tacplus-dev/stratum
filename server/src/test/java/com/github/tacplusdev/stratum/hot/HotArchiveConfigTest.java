// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import com.github.tacplusdev.stratum.hazelcast.StratumRole;
import com.github.tacplusdev.stratum.model.types.sys.diskstore.Diskstore.ArchiveConfig;
import com.github.tacplusdev.stratum.model.types.sys.diskstore.Diskstore.DiskPersistenceConfig;
import com.github.tacplusdev.stratum.model.types.sys.grpcconfig.Grpcconfig;
import com.github.tacplusdev.stratum.model.types.sys.grpcconfig.Helpers;

/**
 * The archive block of stratum's bootstrap config, loaded the way the generated
 * {@code GrpcConfigLoader} loads it: writers ship under it, search members — which
 * run the same image against the same config — do not.
 */
class HotArchiveConfigTest {

    /**
     * The disk_persistence block the stratum bootstrap config carries (writers and
     * search alike). Both intervals are explicit: the generated loader validates
     * before it defaults nested blocks, so an omitted one is refused as 0 rather
     * than defaulted to 86400.
     */
    static final String BOOTSTRAP = """
            disk_persistence:
              dir: /app/data
              roll_interval_seconds: 60
              compression: COMPRESSION_GZ
              archive:
                gcs_bucket: jody1-328023-datagate-backups
                prefix: stratum
                checkpoint_interval_seconds: 86400
            """;

    private static DiskPersistenceConfig load(String yaml) throws Exception {
        Grpcconfig.Config cfg = Helpers.configFromBytes(yaml.getBytes(StandardCharsets.UTF_8));
        cfg = Helpers.configDefaults(cfg).build();
        Helpers.validateConfig(cfg);
        return cfg.getDiskPersistence();
    }

    @Test
    void aWriterShipsToTheConfiguredBucketWithADailyCheckpoint() throws Exception {
        DiskPersistenceConfig persistence = load(BOOTSTRAP);

        ArchiveConfig archive = HotArchive.configFor(StratumRole.WRITER, persistence);

        assertNotNull(archive);
        assertEquals("jody1-328023-datagate-backups", archive.getGcsBucket());
        assertEquals("stratum", archive.getPrefix());
        assertEquals(86400, archive.getCheckpointIntervalSeconds(), "one checkpoint a day");
        assertEquals(60, persistence.getRollIntervalSeconds(), "the recovery point is one roll");
    }

    @Test
    void aSearchMemberShipsNothingUnderTheSameConfig() throws Exception {
        assertNull(HotArchive.configFor(StratumRole.SEARCH, load(BOOTSTRAP)));
    }

    @Test
    void aWriterWithoutAnArchiveBlockShipsNothing() {
        assertNull(HotArchive.configFor(StratumRole.WRITER, DiskPersistenceConfig.getDefaultInstance()));
    }
}
