// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.github.tacplusdev.stratum.model.types.services.stratum.Helpers;
import com.github.tacplusdev.stratum.model.types.services.stratum.Stratum;
import com.github.tacplusdev.stratum.model.types.sys.errors.ProtoException;

/**
 * The tenant's seed members are part of the typed Config the operator renders
 * from the uni-config (stratum#23, operator#17), not an env var: a Config that
 * names none fails its own validation, and the member joins exactly the ones it
 * names.
 */
class SeedMembersConfigTest {

    private static Stratum.Config withSeeds(String... seeds) {
        return Helpers.configDefaults(Stratum.Config.newBuilder().addAllSeedMembers(List.of(seeds)).build())
                .build();
    }

    @Test
    void aConfigWithoutSeedMembersIsInvalid() {
        assertThrows(ProtoException.class,
                () -> Helpers.validateConfig(Helpers.configDefaults(Stratum.Config.getDefaultInstance()).build()));
    }

    @Test
    void aConfigWithSeedMembersIsValid() {
        assertDoesNotThrow(() -> Helpers.validateConfig(withSeeds("stratum-0.stratum:5001")));
    }

    @Test
    void theSeedsAreTheConfigsTrimmedWithoutBlanks() {
        assertEquals(List.of("stratum-0.stratum:5001", "stratum-1.stratum:5001"),
                StratumHandler.seedMembers(withSeeds(" stratum-0.stratum:5001", "", "  ", "stratum-1.stratum:5001 ")));
        assertEquals(List.of(), StratumHandler.seedMembers(withSeeds()));
    }
}
