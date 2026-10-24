// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.services.letsencrypt.Letsencrypt;

/**
 * The marker annotations the checkpoint reads, resolved off the record's own
 * descriptor rather than off a generated accessor.
 */
class RecordAnnotationsTest {

    @Test
    void theDeletedFlagIsFoundByItsAnnotationNotByItsName() {
        assertEquals("deleted",
                RecordAnnotations.deletedFlag(TestRecords.descriptor()).getName());
        assertEquals("store_epoch_ms",
                RecordAnnotations.storeEpoch(TestRecords.descriptor()).getName());
        assertEquals("namespace",
                RecordAnnotations.storeNamespace(TestRecords.descriptor()).getName());
        assertEquals("spiffe_id",
                RecordAnnotations.storePrincipal(TestRecords.descriptor()).getName());
    }

    @Test
    void aTypeWithNoMarkerHasNoDeletedState() {
        // letsencrypt.LetsEncryptCert is a real fleet record type, and no fleet type
        // carries (spec.deleted_flag) until the annotation pass runs. The purge step
        // must select nothing from it rather than guess at a field called "deleted".
        assertNull(RecordAnnotations.deletedFlag(Letsencrypt.LetsEncryptCert.getDescriptor()));
        assertFalse(RecordAnnotations.isDeleted(Letsencrypt.LetsEncryptCert.getDefaultInstance()));
    }

    @Test
    void theFlagAndTheEpochAreReadOffTheRow() {
        assertTrue(RecordAnnotations.isDeleted(TestRecords.row("k", "/t", "spiffe://p", 42L, true)));
        assertFalse(RecordAnnotations.isDeleted(TestRecords.row("k", "/t", "spiffe://p", 42L, false)));
        assertEquals(42L, RecordAnnotations.storeEpochMs(TestRecords.row("k", "/t", "spiffe://p", 42L, true)));
    }

    /**
     * The fleet's real store-namespace marker, resolved the same way — proof the
     * mechanism reads the annotation protoc actually wrote and not only the one
     * this test assembles. (No fleet record declares {@code (spec.store_epoch)}
     * yet, so the namespace marker is the one a generated type carries.)
     */
    @Test
    void theStoreNamespaceMarkerResolvesOnAGeneratedType() {
        assertEquals("namespace",
                RecordAnnotations.storeNamespace(Letsencrypt.LetsEncryptCert.getDescriptor()).getName());
    }
}
