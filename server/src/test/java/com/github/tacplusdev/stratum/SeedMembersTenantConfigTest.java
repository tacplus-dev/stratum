// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.github.tacplusdev.stratum.hazelcast.StratumMapConfig;
import com.github.tacplusdev.stratum.model.types.generated.tenant_config.TenantConfigOuterClass.TenantConfig;
import com.github.tacplusdev.stratum.model.types.services.kernel.Kernel;
import com.github.tacplusdev.stratum.model.types.services.stratum.Helpers;
import com.github.tacplusdev.stratum.model.types.services.stratum.Stratum;

/**
 * A tenant's seed members are its own StratumTenantConfig slice, which kernel
 * holds per tenant in ConfigState.tenant_configs (stratum#26, operator#2 option
 * B), not the deployment-wide Config every tenant's Stratum shares. Kernel keys
 * that map by the tenant's namespace, /tenants/&lt;name&gt;, and pins a tenant's
 * Stratum to its own entry (kernel#34 option A), so the member takes the one
 * entry delivered, never looking it up by the tid in its SPIFFE id (stratum#50).
 * With none it has nothing to join (MemberNetwork refuses to start it,
 * MemberNetworkTest); with more than one it cannot tell which is its own.
 */
class SeedMembersTenantConfigTest {

    /** The tid of this instance, from the identity the fleet minted it. */
    static final String TID = StratumMapConfig.tenantOf("spiffe://tacplus.dev/tenants/ab12cd34/services/stratum");

    static Kernel.TenantConfigEntry entry(String tenant, String... seeds) {
        return Kernel.TenantConfigEntry.newBuilder()
                .setTenantName(tenant)
                .setConfig(TenantConfig.newBuilder()
                        .setStratum(Stratum.StratumTenantConfig.newBuilder().addAllSeedMembers(List.of(seeds))))
                .build();
    }

    @Test
    void theOneEntryKernelDeliversKeyedByNamespaceGivesItsSeeds() {
        assertEquals(List.of("stratum-0.stratum.tac-d-a:5001", "stratum-1.stratum.tac-d-a:5001"),
                StratumHandler.seedMembers(TID, Map.of("/tenants/acme",
                        entry("/tenants/acme", "stratum-0.stratum.tac-d-a:5001", "stratum-1.stratum.tac-d-a:5001"))));
    }

    @Test
    void aTenantWithoutSeedMembersHasNone() {
        assertEquals(List.of(), StratumHandler.seedMembers(TID, Map.of()), "no TenantConfig");
        assertEquals(List.of(), StratumHandler.seedMembers(TID, Map.of("/tenants/acme",
                Kernel.TenantConfigEntry.newBuilder().setTenantName("/tenants/acme")
                        .setConfig(TenantConfig.getDefaultInstance()).build())), "a TenantConfig without Stratum's slice");
        assertEquals(List.of(), StratumHandler.seedMembers(TID, Map.of("/tenants/acme", entry("/tenants/acme"))),
                "an empty list");
    }

    @Test
    void moreThanOneEntryIsRefusedNeverPicked() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> StratumHandler.seedMembers(TID,
                Map.of("/tenants/acme", entry("/tenants/acme", "stratum-0.stratum.tac-d-a:5001"),
                        "/tenants/beta", entry("/tenants/beta", "stratum-0.stratum.tac-d-b:5001"))));
        assertTrue(e.getMessage().contains("/tenants/acme") && e.getMessage().contains("/tenants/beta"),
                e.getMessage());
    }

    @Test
    void theSeedsAreTheSlicesTrimmedWithoutBlanks() {
        assertEquals(List.of("stratum-0.stratum:5001", "stratum-1.stratum:5001"), StratumHandler.seedMembers(TID,
                Map.of("/tenants/t", entry("/tenants/t", " stratum-0.stratum:5001", "", "  ", "stratum-1.stratum:5001 "))));
    }

    @Test
    void theDeploymentConfigNoLongerCarriesSeedMembers() {
        // The same Config reaches every tenant's Stratum: without seeds it is valid.
        assertDoesNotThrow(
                () -> Helpers.validateConfig(Helpers.configDefaults(Stratum.Config.getDefaultInstance()).build()));
        assertNull(Stratum.Config.getDescriptor().findFieldByName("seed_members"));
    }
}
