// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.stratum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.collection.CollectionRuntime;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.sys.errors.ProtoException;

/**
 * A page limit no {@code int} can hold is a bad REQUEST, and has to read as one.
 *
 * {@code pageSorted} takes the limit as a long and narrowed it with an unchecked
 * cast. 2^31 wraps to {@code Integer.MIN_VALUE}, which makes the window's end
 * negative and blows up inside {@code List.subList} — an IndexOutOfBoundsException
 * on the query path, surfaced to the caller as INTERNAL, i.e. as stratum's fault.
 * Nothing about "you asked for 2147483648 rows" survives to the client.
 *
 * The generated RecordMap servers clamp page_size to 1,000,000 before they get
 * here, so this is not reachable from the wire TODAY; the check belongs on the
 * helper that does the narrowing, which is the thing that is wrong.
 */
class PageLimitBoundsTest {

    private static List<String> rows(int n) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(String.format("%04d", i));
        }
        return out;
    }

    private static CollectionRuntime.Page<String> page(List<String> all, long limit,
            CollectionRuntime.Cursor after) throws ProtoException {
        return StratumHandler.pageSorted(new ArrayList<>(all),
                new CollectionRuntime.PageParams(limit, after), List::of);
    }

    @Test
    void aLimitTooBigForAnIntIsRejectedAsInvalidArgument() {
        // 2^31 exactly: the smallest long whose (int) cast is negative.
        ProtoException e = assertThrows(ProtoException.class,
                () -> page(rows(5), 2147483648L, null));
        assertEquals(ProtoException.INVALID_ARGUMENT, e.getCode(),
                "an unmeetable page limit is the caller's error, not an INTERNAL");
        assertTrue(e.getMessage().contains("2147483648"),
                "the error must name the limit that was refused: " + e.getMessage());
    }

    @Test
    void theLargestUsableLimitStillPagesRatherThanOverflowing() throws Exception {
        // Integer.MAX_VALUE is in range, but start + limit is an int overflow away
        // from a negative subList end as soon as a cursor moves start off zero.
        List<String> all = rows(5);
        CollectionRuntime.Page<String> first = page(all, Integer.MAX_VALUE, null);
        assertEquals(all, first.items, "an int-max limit must return the whole list");

        CollectionRuntime.Page<String> after = page(all, Integer.MAX_VALUE,
                new CollectionRuntime.Cursor(List.of("0001")));
        assertEquals(List.of("0002", "0003", "0004"), after.items,
                "and must keep paging from a cursor instead of overflowing to a negative end");
    }

    @Test
    void ordinaryLimitsAreUnchanged() throws Exception {
        CollectionRuntime.Page<String> p = page(rows(5), 2, null);
        assertEquals(List.of("0000", "0001"), p.items);
        assertEquals(List.of("0001"), p.nextAfter.keys);
        assertEquals(5, p.total);
    }
}
