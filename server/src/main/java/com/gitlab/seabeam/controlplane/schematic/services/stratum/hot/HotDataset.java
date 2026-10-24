// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hot;

import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.google.protobuf.Message;

/**
 * One hot record type, described for the redo log: enough to append a write and
 * to replay one back.
 *
 * The descriptors are GENERATED (StratumHotStores) from the same record
 * declarations {@code StratumEventStores} is generated from — the redo tier is a
 * second consumer of that one declaration, not a second source of truth. The
 * runtime in this package names no vservice, the same split the warm tier has.
 */
public final class HotDataset {

    /** Hazelcast map name, e.g. "letsencrypt.LetsEncryptCert". */
    public final String mapName;
    /**
     * Distinct nodes that must hold a value before the write is acked — the
     * record's declared (spec.min_durable_copies), defaulted to 2.
     */
    public final int minDurableCopies;
    /** The record's key (its (spec.record).key field). */
    public final Function<Message, String> key;
    /**
     * The row's store epoch, in millis. This is what the cross-node restore union
     * resolves by, so it must be the SAME number on every node that logs one
     * write — it is stamped once by the ingesting node and carried to both logs.
     */
    public final ToLongFunction<Message> epoch;
    /**
     * The (spec.store_epoch) field's proto NAME. The restore merge runs at the
     * partition owner, where a field name travels and a lambda does not.
     */
    public final String epochField;
    /** The record as JSON — the redo log's payload encoding. */
    public final Function<Message, String> toJson;
    /** The inverse, for replay. */
    public final Function<String, Message> fromJson;

    public HotDataset(String mapName, int minDurableCopies,
                      Function<Message, String> key,
                      ToLongFunction<Message> epoch,
                      String epochField,
                      Function<Message, String> toJson,
                      Function<String, Message> fromJson) {
        this.mapName = mapName;
        this.minDurableCopies = minDurableCopies;
        this.key = key;
        this.epoch = epoch;
        this.epochField = epochField;
        this.toJson = toJson;
        this.fromJson = fromJson;
    }
}
