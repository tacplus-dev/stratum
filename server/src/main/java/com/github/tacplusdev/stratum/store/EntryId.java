// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.store;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoKey;

/**
 * One entry of the store: a record type and a key, in the canonical lock order
 * the generated write surface declares (tacplus#163 D1-B): record type full
 * name, then key, the key compared as UTF-8 bytes when it is a string and
 * numerically when it is an integer.
 *
 * <p>The order is the platform's, not this class's choice: the generated
 * wrapper refuses an {@code Apply} whose ops are in any other order, and the
 * redo log keeps an operation's changes in it. Every multi-entry lock in the
 * store is taken in this order, which is what makes two operations over the
 * same entries unable to deadlock each other.
 */
public final class EntryId implements Comparable<EntryId> {

    private final String type;
    private final RedoKey key;

    public EntryId(String type, RedoKey key) {
        this.type = Objects.requireNonNull(type, "type");
        this.key = Objects.requireNonNull(key, "key");
    }

    /** The record type's full proto name, e.g. "letsencrypt.LetsEncryptCert". */
    public String type() { return type; }

    public RedoKey key() { return key; }

    /**
     * The key as the record maps hold it: the string itself, or the integer's
     * decimal form. Unique per type because a type's key has one field type.
     */
    public String mapKey() {
        return mapKey(key);
    }

    public static String mapKey(RedoKey key) {
        switch (key.getKeyCase()) {
            case STRING_KEY: return key.getStringKey();
            case INT_KEY: return Long.toString(key.getIntKey());
            case UINT_KEY: return Long.toUnsignedString(key.getUintKey());
            default: throw new IllegalArgumentException("record key is unset");
        }
    }

    @Override
    public int compareTo(EntryId o) {
        int c = compareUtf8(type, o.type);
        if (c != 0) {
            return c;
        }
        return compareKeys(key, o.key);
    }

    static int compareKeys(RedoKey a, RedoKey b) {
        if (a.getKeyCase() != b.getKeyCase()) {
            // One type has one key field, so this only separates malformed input;
            // it still has to be a total order.
            return Integer.compare(a.getKeyCase().getNumber(), b.getKeyCase().getNumber());
        }
        switch (a.getKeyCase()) {
            case STRING_KEY: return compareUtf8(a.getStringKey(), b.getStringKey());
            case INT_KEY: return Long.compare(a.getIntKey(), b.getIntKey());
            case UINT_KEY: return Long.compareUnsigned(a.getUintKey(), b.getUintKey());
            default: return 0;
        }
    }

    /**
     * UTF-8 byte order. Not {@link String#compareTo}, which compares UTF-16 code
     * units and disagrees with byte order above U+FFFF: the Go and Rust stores
     * order by bytes, and so does the wrapper's order check.
     */
    static int compareUtf8(String a, String b) {
        return Arrays.compareUnsigned(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntryId)) return false;
        EntryId e = (EntryId) o;
        return type.equals(e.type) && key.equals(e.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, key);
    }

    @Override
    public String toString() {
        return type + "/" + mapKey();
    }
}
