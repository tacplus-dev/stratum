// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.store;

import java.util.function.Function;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoKey;
import com.github.tacplusdev.stratum.model.types.sys.spec.Spec.Durability;

/**
 * What the store needs to know about one record type, built per type by the
 * generated {@code StratumRecordTypes} from its {@code (spec.record)}: its name,
 * the durability its writes are acked at, and how to read a row's key and
 * owning namespace.
 */
public final class RecordType {

    private final String fullName;
    private final Durability durability;
    private final Parser<? extends Message> parser;
    private final Function<Message, RedoKey> keyOf;
    private final Function<Message, String> namespaceOf;

    public RecordType(String fullName, Durability durability, Parser<? extends Message> parser,
                      Function<Message, RedoKey> keyOf, Function<Message, String> namespaceOf) {
        this.fullName = fullName;
        this.durability = durability == Durability.DURABILITY_UNSPECIFIED ? Durability.DURABILITY_LOCAL : durability;
        this.parser = parser;
        this.keyOf = keyOf;
        this.namespaceOf = namespaceOf;
    }

    /** The record's full proto name; also the name of its Hazelcast map. */
    public String fullName() { return fullName; }

    /** The level a write of this type is acked at unless it asks for more. */
    public Durability durability() { return durability; }

    public RedoKey keyOf(Message row) { return keyOf.apply(row); }

    public String namespaceOf(Message row) { return namespaceOf.apply(row); }

    public Message parse(ByteString bytes) throws InvalidProtocolBufferException {
        return parser.parseFrom(bytes);
    }

    @Override
    public String toString() {
        return fullName + " (" + durability + ")";
    }
}
