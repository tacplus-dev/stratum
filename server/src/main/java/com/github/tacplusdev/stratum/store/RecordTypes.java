// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.store;

import java.util.List;

import com.github.tacplusdev.stratum.model.types.sys.errors.ProtoException;
import com.github.tacplusdev.stratum.model.types.vservices.query.Query;

/**
 * Every record type the store holds, and the one piece of per-type code the
 * write path needs: turning a generated {@code RecordOp} into a {@link WriteOp}.
 * Implemented by the generated {@code StratumRecordTypes}, so nothing
 * hand-written names a record type.
 */
public interface RecordTypes {

    /** Every record type, in declaration order. */
    List<RecordType> all();

    /** The record type with this full proto name, or null. */
    RecordType byName(String fullName);

    /** The op a {@code RecordOp} carries; INVALID_ARGUMENT when it carries none. */
    WriteOp decode(Query.RecordOp op) throws ProtoException;
}
