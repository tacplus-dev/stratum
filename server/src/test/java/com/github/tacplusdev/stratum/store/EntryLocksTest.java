// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoKey;

class EntryLocksTest {

    private static EntryId id(String type, String key) {
        return new EntryId(type, RedoKey.newBuilder().setStringKey(key).build());
    }

    private static EntryId id(String type, long key) {
        return new EntryId(type, RedoKey.newBuilder().setIntKey(key).build());
    }

    @Test
    void entriesAreLockedByTypeThenKeyAndOnce() {
        List<EntryId> taken = Collections.synchronizedList(new ArrayList<>());
        EntryLocks locks = new EntryLocks(taken::add);
        try (EntryLocks.Hold h = locks.lockEntries(List.of(
                id("b.T", "a"), id("a.T", "z"), id("a.T", "b"), id("a.T", "z")))) {
            assertEquals(List.of(id("a.T", "b"), id("a.T", "z"), id("b.T", "a")), taken);
        }
        assertEquals(0, locks.liveEntryLocks(), "an entry's lock exists only while it is held");
    }

    @Test
    void integerKeysAreOrderedNumerically() {
        List<EntryId> taken = Collections.synchronizedList(new ArrayList<>());
        EntryLocks locks = new EntryLocks(taken::add);
        try (EntryLocks.Hold h = locks.lockEntries(List.of(id("t.T", 10), id("t.T", -1), id("t.T", 9)))) {
            assertEquals(List.of(id("t.T", -1), id("t.T", 9), id("t.T", 10)), taken);
        }
    }

    @Test
    void aMapLockExcludesTheTypesEntryLocks() throws Exception {
        EntryLocks locks = new EntryLocks();
        CompletableFuture<Void> entry;
        try (EntryLocks.Hold map = locks.lockMaps(List.of("t.T"))) {
            entry = CompletableFuture.runAsync(() -> locks.lockEntries(List.of(id("t.T", "a"))).close());
            Thread.sleep(200);
            assertFalse(entry.isDone(), "an entry of a map-locked type waits for the map lock");
            // Another type's entries do not.
            CompletableFuture.runAsync(() -> locks.lockEntries(List.of(id("u.T", "a"))).close())
                    .get(5, TimeUnit.SECONDS);
        }
        entry.get(5, TimeUnit.SECONDS);
    }
}
