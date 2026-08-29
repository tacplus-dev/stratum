// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A caller's typed {@code <T>Filter}, flattened to the criteria the disk leg can
 * compile.
 *
 * Only fields the caller actually SET appear. That matters twice: an unset
 * criterion must not narrow the disk leg any more than it narrows the memory leg
 * (the generated {@code match<T>} skips it on {@code has<Field>()}), and an unset
 * epoch bound is what makes the leg prune decidable.
 */
public final class WarmFilter {

    private final Map<String, String> eqStrings = new LinkedHashMap<>();
    private final Map<String, Long> eqNumbers = new LinkedHashMap<>();
    private final Map<String, long[]> ranges = new LinkedHashMap<>();

    public WarmFilter eq(String field, String value) {
        eqStrings.put(field, value);
        return this;
    }

    public WarmFilter eq(String field, long value) {
        eqNumbers.put(field, value);
        return this;
    }

    /**
     * An inclusive range. Either side may be null for "unbounded that way", which
     * is how the generated filter's {@code has<Field>From()} / {@code To()} pair
     * spells a half-open range.
     */
    public WarmFilter range(String field, Long from, Long to) {
        ranges.put(field, new long[] {
                from == null ? Long.MIN_VALUE : from,
                to == null ? Long.MAX_VALUE : to });
        return this;
    }

    public Map<String, String> eqStrings() { return eqStrings; }
    public Map<String, Long> eqNumbers()   { return eqNumbers; }
    public Map<String, long[]> ranges()    { return ranges; }

    /**
     * The caller's own lower bound on a field, or null when it set none.
     *
     * The leg prune reads this for the store-epoch field: a caller whose epoch
     * floor is already at or above the boundary cannot match anything the disk
     * leg holds, so the leg is skipped without opening a single slice. It is a
     * comparison of two longs, not an estimate.
     */
    public Long lowerBound(String field) {
        long[] r = ranges.get(field);
        return r == null || r[0] == Long.MIN_VALUE ? null : r[0];
    }

    /** The caller's own upper bound on a field, or null when it set none. */
    public Long upperBound(String field) {
        long[] r = ranges.get(field);
        return r == null || r[1] == Long.MAX_VALUE ? null : r[1];
    }
}
