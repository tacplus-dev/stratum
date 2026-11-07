// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.github.tacplusdev.stratum.model.types.generated.tenant_config.TenantConfigOuterClass.TenantConfig;
import com.github.tacplusdev.stratum.model.types.services.kernel.Kernel;
import com.github.tacplusdev.stratum.model.types.services.stratum.Helpers;
import com.github.tacplusdev.stratum.model.types.services.stratum.Stratum;

/**
 * A tenant's seed members are its own StratumTenantConfig slice, which kernel
 * holds per tenant in ConfigState.tenant_configs (stratum#26, operator#2 option
 * B), not the deployment-wide Config every tenant's Stratum shares: a member
 * joins exactly its own tenant's list, never another tenant's, and with none it
 * has nothing to join (MemberNetwork refuses to start it, MemberNetworkTest).
 */
class SeedMembersTenantConfigTest {

    static Kernel.TenantConfigEntry entry(String tenant, String... seeds) {
        return Kernel.TenantConfigEntry.newBuilder()
                .setTenantName(tenant)
                .setConfig(TenantConfig.newBuilder()
                        .setStratum(Stratum.StratumTenantConfig.newBuilder().addAllSeedMembers(List.of(seeds))))
                .build();
    }

    static final Map<String, Kernel.TenantConfigEntry> TWO_TENANTS = Map.of(
            "tenanta", entry("tenanta", "stratum-0.stratum.tac-d-a:5001", "stratum-1.stratum.tac-d-a:5001"),
            "tenantb", entry("tenantb", "stratum-0.stratum.tac-d-b:5001", "stratum-1.stratum.tac-d-b:5001",
                    "stratum-2.stratum.tac-d-b:5001"));

    @Test
    void eachTenantGetsOnlyItsOwnSeedMembers() {
        assertEquals(List.of("stratum-0.stratum.tac-d-a:5001", "stratum-1.stratum.tac-d-a:5001"),
                StratumHandler.seedMembers("tenanta", TWO_TENANTS));
        assertEquals(List.of("stratum-0.stratum.tac-d-b:5001", "stratum-1.stratum.tac-d-b:5001",
                "stratum-2.stratum.tac-d-b:5001"), StratumHandler.seedMembers("tenantb", TWO_TENANTS));
    }

    @Test
    void aTenantWithoutSeedMembersHasNone() {
        assertEquals(List.of(), StratumHandler.seedMembers("tenantc", TWO_TENANTS), "no TenantConfig");
        assertEquals(List.of(), StratumHandler.seedMembers("tenantc", Map.of("tenantc",
                Kernel.TenantConfigEntry.newBuilder().setTenantName("tenantc").setConfig(TenantConfig.getDefaultInstance())
                        .build())), "a TenantConfig without Stratum's slice");
        assertEquals(List.of(), StratumHandler.seedMembers("tenantc", Map.of("tenantc", entry("tenantc"))),
                "an empty list");
    }

    @Test
    void theSeedsAreTheSlicesTrimmedWithoutBlanks() {
        assertEquals(List.of("stratum-0.stratum:5001", "stratum-1.stratum:5001"), StratumHandler.seedMembers("t",
                Map.of("t", entry("t", " stratum-0.stratum:5001", "", "  ", "stratum-1.stratum:5001 "))));
    }

    @Test
    void theDeploymentConfigNoLongerCarriesSeedMembers() {
        // The same Config reaches every tenant's Stratum: without seeds it is valid.
        assertDoesNotThrow(
                () -> Helpers.validateConfig(Helpers.configDefaults(Stratum.Config.getDefaultInstance()).build()));
        assertNull(Stratum.Config.getDescriptor().findFieldByName("seed_members"));
    }
}
