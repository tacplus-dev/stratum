// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import com.google.protobuf.Message;

/**
 * One (spec.query_field) criterion, as the disk tier sees it.
 *
 * The field list is not a second source of truth: it is the SAME list the
 * generator already reads out of the protos to compile the in-memory
 * {@code match<T>} predicate. This is a second backend for it, so a field that
 * is filterable in memory is filterable on disk by construction.
 */
public final class WarmField {

    /** How the criterion is indexed and matched. Mirrors QueryOp + the field's scalar type. */
    public enum Kind {
        /** QUERY_OP_EQ on a string field — exact term, never analysed. */
        EQ_STRING,
        /** QUERY_OP_EQ on a numeric or enum field — exact point. */
        EQ_NUMERIC,
        /** QUERY_OP_RANGE — inclusive point range. */
        RANGE_NUMERIC
    }

    /** Reads the field's value off a record. Returns null when the field is unset. */
    public interface ValueOf {
        Object apply(Message record);
    }

    public final String name;
    public final Kind kind;
    public final ValueOf valueOf;

    public WarmField(String name, Kind kind, ValueOf valueOf) {
        this.name = name;
        this.kind = kind;
        this.valueOf = valueOf;
    }
}
