// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.store;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * The store's locks: one per entry, and one per record type (the map lock).
 *
 * <p>Atomicity is per entry first (decisions: stratum-per-tenant-2026-10-25,
 * item 4). A write takes the lock of every entry it touches, including an entry
 * that does not exist yet (an InsertIfAbsent's), and holds them from the
 * condition check until its outcome is applied, so nothing can change an entry
 * between "it is absent" and "it is inserted". An operation over several
 * entries takes them in the canonical order ({@link EntryId}), and every
 * operation does, so two operations over the same entries queue rather than
 * deadlock.
 *
 * <p>A map lock is rare: an operation over a whole record type takes it
 * exclusively; every entry operation holds the map locks of its types shared,
 * so the two exclude each other. The record types are themselves taken in
 * canonical order.
 *
 * <p>The locks live in the JVM of the store's sequencer (see
 * {@link RecordStore}): every write runs there, so a JVM lock is a store lock.
 * An entry's lock exists only while someone holds or waits for it.
 */
public final class EntryLocks {

    private static final class Held {
        final ReentrantLock lock = new ReentrantLock();
        int users;
    }

    private final ConcurrentHashMap<EntryId, Held> entries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> maps = new ConcurrentHashMap<>();
    private final Consumer<EntryId> onAcquire;

    public EntryLocks() {
        this(id -> { });
    }

    /** @param onAcquire told of each entry lock as it is taken, in order (tests). */
    EntryLocks(Consumer<EntryId> onAcquire) {
        this.onAcquire = onAcquire;
    }

    /** A set of held locks, released in reverse order. */
    public interface Hold extends AutoCloseable {
        @Override
        void close();
    }

    /**
     * Takes the map locks (shared) of the entries' types and then the entries'
     * own locks, each set in canonical order whatever order {@code ids} is in.
     * An entry named twice is locked once.
     */
    public Hold lockEntries(List<EntryId> ids) {
        TreeSet<EntryId> ordered = new TreeSet<>(ids);
        TreeSet<String> types = new TreeSet<>(EntryId::compareUtf8);
        for (EntryId id : ordered) {
            types.add(id.type());
        }
        List<Runnable> release = new ArrayList<>();
        try {
            for (String type : types) {
                ReentrantReadWriteLock.ReadLock r = mapLock(type).readLock();
                r.lock();
                release.add(r::unlock);
            }
            for (EntryId id : ordered) {
                Held h = entries.compute(id, (k, v) -> {
                    Held held = v == null ? new Held() : v;
                    held.users++;
                    return held;
                });
                h.lock.lock();
                onAcquire.accept(id);
                release.add(() -> {
                    h.lock.unlock();
                    entries.compute(id, (k, v) -> --v.users == 0 ? null : v);
                });
            }
        } catch (RuntimeException e) {
            releaseAll(release);
            throw e;
        }
        return () -> releaseAll(release);
    }

    /** Takes the map locks of {@code types} exclusively, in canonical order. */
    public Hold lockMaps(List<String> types) {
        TreeSet<String> ordered = new TreeSet<>(EntryId::compareUtf8);
        ordered.addAll(types);
        List<Runnable> release = new ArrayList<>();
        for (String type : ordered) {
            ReentrantReadWriteLock.WriteLock w = mapLock(type).writeLock();
            w.lock();
            release.add(w::unlock);
        }
        return () -> releaseAll(release);
    }

    /** Entry locks currently held or waited for; 0 when the store is idle. */
    int liveEntryLocks() {
        return entries.size();
    }

    private ReentrantReadWriteLock mapLock(String type) {
        return maps.computeIfAbsent(type, t -> new ReentrantReadWriteLock());
    }

    private static void releaseAll(List<Runnable> release) {
        for (int i = release.size() - 1; i >= 0; i--) {
            release.get(i).run();
        }
        release.clear();
    }
}
