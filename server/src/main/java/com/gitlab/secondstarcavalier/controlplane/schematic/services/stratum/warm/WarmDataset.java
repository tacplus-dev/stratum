// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * Everything the warm tier needs to know about one record type, and nothing
 * about which record type it is.
 *
 * Descriptors are GENERATED (StratumWarmStores) from the same (spec.record) /
 * (spec.query_field) discovery the hot store already uses, so this package —
 * like the rest of stratum's hand-written code — names no vservice.
 */
public final class WarmDataset {

    /** The record's key, from (spec.record).key. */
    public interface KeyOf {
        String apply(Message record);
    }

    /** The record's store epoch, from the (spec.store_epoch) field. Unix millis of its last write. */
    public interface EpochOf {
        long apply(Message record);
    }

    /** Parses the stored proto bytes back into the record type. */
    public interface ParseFrom {
        Message apply(byte[] bytes) throws InvalidProtocolBufferException;
    }

    /**
     * The record type's generated validator — the SAME one the hot store and the
     * read path enforce. Throws when the record is unservable.
     */
    public interface Validate {
        void apply(Message record) throws Exception;
    }

    /** Dataset name — the Hazelcast map name, so one identifier names the type in both tiers. */
    public final String name;
    /** The record's Java type; how a message is routed to its dataset. */
    public final Class<? extends Message> type;
    /**
     * Proto field name carrying (spec.store_epoch). It is indexed under the
     * reserved {@code _epoch} field rather than its own name, so a caller's range
     * on it and the leg's own epoch cut are the same Lucene field, indexed once.
     */
    public final String epochField;
    /** (spec.record).mutability = MUTABILITY_APPEND_ONLY — never updated, never deleted by key. */
    public final boolean appendOnly;
    /** The memory-residency window, in millis. Rows older than this are served from disk. */
    public final long windowMs;
    /** Filterable fields, EXCLUDING the store-epoch field (see {@link #epochField}). */
    public final List<WarmField> fields;

    private final KeyOf keyOf;
    private final EpochOf epochOf;
    private final ParseFrom parseFrom;
    private final Validate validate;

    public WarmDataset(String name, Class<? extends Message> type, String epochField,
                       boolean appendOnly, long windowMs, List<WarmField> fields,
                       KeyOf keyOf, EpochOf epochOf, ParseFrom parseFrom, Validate validate) {
        this.name = name;
        this.type = type;
        this.epochField = epochField;
        this.appendOnly = appendOnly;
        this.windowMs = windowMs;
        this.fields = List.copyOf(fields);
        this.keyOf = keyOf;
        this.epochOf = epochOf;
        this.parseFrom = parseFrom;
        this.validate = validate;
    }

    public String key(Message record) {
        return keyOf.apply(record);
    }

    public long epochMs(Message record) {
        return epochOf.apply(record);
    }

    public Message parse(byte[] bytes) throws InvalidProtocolBufferException {
        return parseFrom.apply(bytes);
    }

    public void validate(Message record) throws Exception {
        validate.apply(record);
    }

    /**
     * The boundary between the two legs at this instant: memory serves
     * {@code epoch >= boundary}, disk serves {@code epoch < boundary}. One
     * expression, used by both the leg prune and the disk query's epoch clause,
     * so the legs cannot drift apart.
     */
    public long boundaryMs(long nowMs) {
        return nowMs - windowMs;
    }
}
