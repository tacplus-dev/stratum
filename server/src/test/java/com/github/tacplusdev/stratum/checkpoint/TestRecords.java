// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.checkpoint;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;

/**
 * A record type carrying the four marker annotations the checkpoint reads,
 * built at runtime because no type in the fleet carries them yet.
 *
 * <p>{@code (spec.deleted_flag)} is on tacplus's unmerged annotation branch
 * (spec.proto: {@code bool deleted_flag = 60091}), so there is no .proto in this
 * build that declares it and no generated class that sets it. A descriptor
 * assembled here carries the marker exactly as protoc writes it — an extension on
 * {@code FieldOptions} at its assigned number — which is what
 * {@link RecordAnnotations} reads, so the purge step is tested against the real
 * annotation rather than against a stand-in for it.
 */
final class TestRecords {

    static final String TYPE = "test.PurgeableRecord";

    private static final Descriptor DESCRIPTOR = build();

    private TestRecords() {}

    static Descriptor descriptor() {
        return DESCRIPTOR;
    }

    /** A row of the synthetic type. */
    static Message row(String key, String tenant, String principal, long storeEpochMs, boolean deleted) {
        return DynamicMessage.newBuilder(DESCRIPTOR)
                .setField(DESCRIPTOR.findFieldByName("record_key"), key)
                .setField(DESCRIPTOR.findFieldByName("namespace"), tenant)
                .setField(DESCRIPTOR.findFieldByName("spiffe_id"), principal)
                .setField(DESCRIPTOR.findFieldByName("store_epoch_ms"), storeEpochMs)
                .setField(DESCRIPTOR.findFieldByName("deleted"), deleted)
                .build();
    }

    private static FieldOptions marked(int extensionNumber) {
        return FieldOptions.newBuilder()
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(extensionNumber,
                                UnknownFieldSet.Field.newBuilder().addVarint(1L).build())
                        .build())
                .build();
    }

    private static FieldDescriptorProto field(String name, int number,
                                              FieldDescriptorProto.Type type, Integer marker) {
        FieldDescriptorProto.Builder b = FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(type);
        if (marker != null) {
            b.setOptions(marked(marker));
        }
        return b.build();
    }

    private static Descriptor build() {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("test/purgeable.proto")
                .setSyntax("proto3")
                .setPackage("test")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("PurgeableRecord")
                        .addField(field("record_key", 1, FieldDescriptorProto.Type.TYPE_STRING, null))
                        .addField(field("namespace", 2, FieldDescriptorProto.Type.TYPE_STRING,
                                RecordAnnotations.STORE_NAMESPACE))
                        .addField(field("spiffe_id", 3, FieldDescriptorProto.Type.TYPE_STRING,
                                RecordAnnotations.STORE_PRINCIPAL))
                        .addField(field("store_epoch_ms", 4, FieldDescriptorProto.Type.TYPE_INT64,
                                RecordAnnotations.STORE_EPOCH))
                        .addField(field("deleted", 5, FieldDescriptorProto.Type.TYPE_BOOL,
                                RecordAnnotations.DELETED_FLAG)))
                .build();
        try {
            return FileDescriptor.buildFrom(file, new FileDescriptor[0]).getMessageTypes().get(0);
        } catch (DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
    }
}
