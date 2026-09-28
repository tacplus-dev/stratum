// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.checkpoint;

import java.util.Map;

import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;

/**
 * Where a record keeps the three values the daily checkpoint needs, read from
 * the record's own descriptor.
 *
 * <h2>Why the descriptor and not a generated accessor</h2>
 * The checkpoint runs over EVERY record type in one pass. A generated per-type
 * accessor would be the right source — {@code HotDataset} and {@code WarmDataset}
 * are exactly that for the tiers they serve — but neither covers every type, and
 * the one that would (a generated purge descriptor) is not emitted. Reading the
 * marker annotation off the descriptor is the same declaration those generators
 * read, taken at runtime instead of at generation time, so this names no record
 * type and stays correct as types are added.
 *
 * <h2>Why the numbers are literals</h2>
 * {@code (spec.store_epoch)} and {@code (spec.store_principal)} /
 * {@code (spec.store_namespace)} are in the spec.proto this build compiles
 * against, so protobuf-java resolves them as KNOWN extensions and they arrive
 * through {@code getAllFields()}. {@code (spec.deleted_flag)} is on tacplus's
 * unmerged annotation branch (spec.proto:2063, {@code bool deleted_flag = 60091})
 * and so arrives as an UNKNOWN field on a descriptor built by a newer protoc.
 * {@link #marks} reads both forms, which is what makes this survive the moment
 * the annotation lands: nothing here changes, the same number simply moves from
 * one accessor to the other.
 *
 * <p>Numbers rather than {@code Spec.deletedFlag}: the generated extension
 * constant does not exist in this build, and inventing a stand-in for it would be
 * the hand-rolled generated surface the platform rules forbid. The number IS the
 * annotation's identity — it is what protoc writes into every descriptor — so
 * pinning it is pinning the declaration, not duplicating it.
 */
public final class RecordAnnotations {

    /** {@code (spec.store_epoch)} — the int64 field holding the row's last-write epoch. */
    public static final int STORE_EPOCH = 60086;
    /** {@code (spec.store_principal)} — the string field holding the row's owning subject. */
    public static final int STORE_PRINCIPAL = 60084;
    /** {@code (spec.store_namespace)} — the string field holding the row's owning tenant. */
    public static final int STORE_NAMESPACE = 60088;
    /** {@code (spec.deleted_flag)} — the one bool field holding the row's soft-delete state. */
    public static final int DELETED_FLAG = 60091;

    private RecordAnnotations() {}

    /** The record's {@code (spec.store_epoch)} field, or null when it declares none. */
    public static FieldDescriptor storeEpoch(Descriptor type) {
        return marked(type, STORE_EPOCH);
    }

    /** The record's {@code (spec.store_principal)} field, or null when it declares none. */
    public static FieldDescriptor storePrincipal(Descriptor type) {
        return marked(type, STORE_PRINCIPAL);
    }

    /** The record's {@code (spec.store_namespace)} field, or null when it declares none. */
    public static FieldDescriptor storeNamespace(Descriptor type) {
        return marked(type, STORE_NAMESPACE);
    }

    /**
     * The record's {@code (spec.deleted_flag)} field, or null when it declares
     * none — an append-only type, or any type the fleet annotation pass has not
     * reached yet. A type with no marker has no soft-delete state, so the purge
     * step skips it entirely rather than guessing at a field named "deleted".
     */
    public static FieldDescriptor deletedFlag(Descriptor type) {
        return marked(type, DELETED_FLAG);
    }

    /** True when {@code row}'s deleted flag is set. False for a type carrying no marker. */
    public static boolean isDeleted(Message row) {
        FieldDescriptor f = deletedFlag(row.getDescriptorForType());
        return f != null && Boolean.TRUE.equals(row.getField(f));
    }

    /**
     * The row's store epoch in millis, or 0 when the type declares no
     * {@code (spec.store_epoch)}. Zero is the same value the generated store
     * passes for such a type today, so "no epoch" and "epoch zero" are already
     * one state here and this does not make them two.
     */
    public static long storeEpochMs(Message row) {
        FieldDescriptor f = storeEpoch(row.getDescriptorForType());
        return f == null ? 0L : ((Number) row.getField(f)).longValue();
    }

    /** The row's {@code field} as a string, or null when the type declares no such field. */
    public static String stringAt(Message row, FieldDescriptor field) {
        return field == null ? null : String.valueOf(row.getField(field));
    }

    private static FieldDescriptor marked(Descriptor type, int extension) {
        for (FieldDescriptor f : type.getFields()) {
            if (marks(f.getOptions(), extension)) {
                return f;
            }
        }
        return null;
    }

    /**
     * True when {@code options} carries extension {@code number} set to true, in
     * either of the two forms a descriptor can present it in. See the class note
     * for why both are read.
     */
    static boolean marks(FieldOptions options, int number) {
        for (Map.Entry<FieldDescriptor, Object> e : options.getAllFields().entrySet()) {
            if (e.getKey().getNumber() == number) {
                return Boolean.TRUE.equals(e.getValue());
            }
        }
        UnknownFieldSet.Field unknown = options.getUnknownFields().getField(number);
        return !unknown.getVarintList().isEmpty() && unknown.getVarintList().get(0) != 0L;
    }
}
