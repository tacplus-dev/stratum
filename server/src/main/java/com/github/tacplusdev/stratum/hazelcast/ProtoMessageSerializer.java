// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.hazelcast;

import java.io.IOException;

import com.google.protobuf.Message;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.StreamSerializer;

import com.github.tacplusdev.stratum.StratumRecordTypes;

/**
 * Hazelcast's wire form for every protobuf record: the message's protobuf full
 * name, then its protobuf bytes.
 *
 * <p>Without this a record reached Hazelcast as a {@link java.io.Serializable}, and
 * protobuf's own Java serialization writes the generated class's Java name and
 * resolves it again with {@code Class.forName} — a package-layout coupling, and
 * the reason an org rename broke deserialization. The full name resolves through
 * the generated registry ({@link StratumRecordTypes#defaultInstance}), and a name
 * that is not a record type this build declares is refused rather than guessed at.
 */
public final class ProtoMessageSerializer implements StreamSerializer<Message> {

    /** Hazelcast custom-serializer type id; any positive id unique within the Config. */
    static final int TYPE_ID = 1;

    @Override
    public int getTypeId() {
        return TYPE_ID;
    }

    @Override
    public void write(ObjectDataOutput out, Message value) throws IOException {
        out.writeString(value.getDescriptorForType().getFullName());
        out.writeByteArray(value.toByteArray());
    }

    @Override
    public Message read(ObjectDataInput in) throws IOException {
        String type = in.readString();
        Message defaultInstance = StratumRecordTypes.defaultInstance(type);
        if (defaultInstance == null) {
            throw new HazelcastSerializationException("a Hazelcast value names " + type
                    + ", which is not a record type this build declares; refusing to deserialize it");
        }
        return defaultInstance.getParserForType().parseFrom(in.readByteArray());
    }
}
