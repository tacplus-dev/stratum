// Copyright (c) 2026 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.stratum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.servicemanager.Caller;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.sys.errors.ProtoException;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.query.Query;

/**
 * The query-handle registry is bounded.
 *
 * <p>A handle is dropped by {@code delete<T>Query} and by nothing else — no sweep,
 * no expiry — so before this an entitled caller that opened queries and never
 * closed them grew the registry for the life of the process, one entry per call,
 * with no cap of any kind to stop it.
 *
 * <p>The bound is per namespace on purpose, and the second test is the reason: a
 * global cap would have turned one tenant's leak into every other tenant's
 * outage, which is a worse failure than the heap growth it fixes.
 *
 * <p>Only {@code create<T>Query} is exercised: it is the only op that adds an
 * entry, and it touches neither the hot store nor the warm one, so the suppliers
 * can stay null and the test stays a unit test of the bound.
 */
class QueryHandleCapTest {

    private static final int CAP = StratumRecordMaps.MAX_OPEN_QUERIES_PER_NAMESPACE;

    private static Caller caller(String namespace) {
        return new Caller("{\"namespace\":\"" + namespace + "\"}", "");
    }

    private static StratumRecordMaps plane() {
        return new StratumRecordMaps(() -> null, () -> true);
    }

    private static String open(StratumRecordMaps plane, Caller c) throws ProtoException {
        Query.QueryHandle h = plane.createAgentEventQuery(c,
                Query.OpenAgentEventQueryRequest.getDefaultInstance());
        assertNotNull(h);
        return h.getQueryId();
    }

    @Test
    void theHandlePastTheCapIsRefused() throws Exception {
        StratumRecordMaps plane = plane();
        Caller a = caller("/tenants/a");

        for (int i = 0; i < CAP; i++) {
            open(plane, a);
        }

        ProtoException refused = assertThrows(ProtoException.class, () -> open(plane, a));
        assertEquals(ProtoException.RESOURCE_EXHAUSTED, refused.getCode(),
                "an exhausted registry is a retryable resource limit, not an internal error");
        assertTrue(refused.getMessage().contains("/tenants/a"), refused.getMessage());
    }

    /** Closing a handle frees its slot — the cap bounds what is OPEN, not what was ever opened. */
    @Test
    void deletingAHandleFreesItsSlot() throws Exception {
        StratumRecordMaps plane = plane();
        Caller a = caller("/tenants/a");

        String first = open(plane, a);
        for (int i = 1; i < CAP; i++) {
            open(plane, a);
        }
        assertThrows(ProtoException.class, () -> open(plane, a));

        plane.deleteAgentEventQuery(a, Query.QueryHandle.newBuilder().setQueryId(first).build());
        assertNotNull(open(plane, a), "a closed handle must give its slot back");
    }

    /** One tenant at its cap must not be able to refuse another tenant's first query. */
    @Test
    void oneTenantsExhaustionDoesNotRefuseAnother() throws Exception {
        StratumRecordMaps plane = plane();
        Caller a = caller("/tenants/a");
        Caller b = caller("/tenants/b");

        for (int i = 0; i < CAP; i++) {
            open(plane, a);
        }
        assertThrows(ProtoException.class, () -> open(plane, a));

        assertNotNull(open(plane, b),
                "the cap is per namespace; a global one would make a tenant's leak a fleet outage");
    }
}
