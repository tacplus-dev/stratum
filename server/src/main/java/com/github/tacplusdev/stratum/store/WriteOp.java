// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.store;

import com.google.protobuf.Message;

import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoKey;
import com.github.tacplusdev.stratum.model.types.sys.spec.Spec.Durability;

/**
 * One conditional write, as the generated {@code StratumRecordTypes} decodes it
 * from a {@code RecordOp}: what it does to which entry, and on what condition.
 */
public final class WriteOp {

    public enum Kind { PUT, INSERT_IF_ABSENT, REPLACE_IF_UNCHANGED, DELETE }

    final Kind kind;
    final RecordType type;
    final EntryId entry;
    /** The new row for every kind but DELETE. */
    final Message item;
    /** The namespace the op is held to: the row's, or a delete's request field. */
    final String namespace;
    /** REPLACE_IF_UNCHANGED's expected revision; null when it expects a value. */
    final Long expectedRevision;
    /** REPLACE_IF_UNCHANGED's expected stored row; null when it expects a revision. */
    final Message expectedItem;
    /** What the write asked for; never lower than its type's level by the time it runs. */
    final Durability durability;

    private WriteOp(Kind kind, RecordType type, RedoKey key, Message item, String namespace,
                    Long expectedRevision, Message expectedItem, Durability durability) {
        this.kind = kind;
        this.type = type;
        this.entry = new EntryId(type.fullName(), key);
        this.item = item;
        this.namespace = namespace == null ? "" : namespace;
        this.expectedRevision = expectedRevision;
        this.expectedItem = expectedItem;
        this.durability = durability;
    }

    public static WriteOp put(RecordType type, Message item, Durability durability) {
        return new WriteOp(Kind.PUT, type, type.keyOf(item), item, type.namespaceOf(item), null, null, durability);
    }

    public static WriteOp insertIfAbsent(RecordType type, Message item, Durability durability) {
        return new WriteOp(Kind.INSERT_IF_ABSENT, type, type.keyOf(item), item, type.namespaceOf(item),
                null, null, durability);
    }

    public static WriteOp replaceIfRevision(RecordType type, Message item, long expectedRevision,
                                            Durability durability) {
        return new WriteOp(Kind.REPLACE_IF_UNCHANGED, type, type.keyOf(item), item, type.namespaceOf(item),
                expectedRevision, null, durability);
    }

    public static WriteOp replaceIfItem(RecordType type, Message item, Message expectedItem,
                                        Durability durability) {
        return new WriteOp(Kind.REPLACE_IF_UNCHANGED, type, type.keyOf(item), item, type.namespaceOf(item),
                null, expectedItem, durability);
    }

    public static WriteOp delete(RecordType type, RedoKey key, String namespace, Durability durability) {
        return new WriteOp(Kind.DELETE, type, key, null, namespace, null, null, durability);
    }

    public Kind kind() { return kind; }

    public EntryId entry() { return entry; }

    @Override
    public String toString() {
        return kind + " " + entry;
    }
}
