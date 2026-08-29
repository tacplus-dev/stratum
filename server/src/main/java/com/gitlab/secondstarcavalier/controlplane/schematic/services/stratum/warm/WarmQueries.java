// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

/**
 * Compiles a {@link WarmFilter} into the disk leg's Lucene query.
 *
 * Every clause is {@code Occur.FILTER}, never {@code MUST}: the filter is a
 * set-membership test, there is no relevance to rank by, and FILTER skips scoring
 * entirely.
 */
public final class WarmQueries {

    private WarmQueries() {}

    /**
     * @param dataset       the record type being queried
     * @param filter        the caller's criteria
     * @param epochFromMs   INCLUSIVE store-epoch floor for the disk leg
     * @param epochToMs     EXCLUSIVE store-epoch ceiling for the disk leg — the leg
     *                      boundary, already narrowed by the caller's own bound.
     *                      Present on every disk-leg query, which is also what
     *                      handles the slice that straddles the boundary without a
     *                      special case for it.
     * @param excludedKeys  keys whose on-disk copy is known stale because their
     *                      asynchronous write has not committed yet; see
     *                      {@link WarmIndex#pendingKeys()}.
     */
    public static Query compile(WarmDataset dataset, WarmFilter filter,
                                long epochFromMs, long epochToMs, Collection<String> excludedKeys) {
        BooleanQuery.Builder b = new BooleanQuery.Builder();

        for (Map.Entry<String, String> e : filter.eqStrings().entrySet()) {
            b.add(new TermQuery(new Term(e.getKey(), e.getValue())), Occur.FILTER);
        }
        for (Map.Entry<String, Long> e : filter.eqNumbers().entrySet()) {
            b.add(LongPoint.newExactQuery(luceneField(dataset, e.getKey()), e.getValue()), Occur.FILTER);
        }
        for (Map.Entry<String, long[]> e : filter.ranges().entrySet()) {
            if (e.getKey().equals(dataset.epochField)) {
                continue; // folded into the epoch clause below — one field, one clause
            }
            b.add(LongPoint.newRangeQuery(e.getKey(), e.getValue()[0], e.getValue()[1]), Occur.FILTER);
        }

        b.add(LongPoint.newRangeQuery(WarmIndex.EPOCH_FIELD, epochFromMs, epochToMs - 1), Occur.FILTER);

        if (!excludedKeys.isEmpty()) {
            List<BytesRef> terms = new ArrayList<>(excludedKeys.size());
            for (String k : excludedKeys) {
                terms.add(new BytesRef(k));
            }
            b.add(new TermInSetQuery(WarmIndex.KEY_FIELD, terms), Occur.MUST_NOT);
        }

        return b.build();
    }

    /** The store-epoch field is indexed under its reserved name, never its proto name. */
    private static String luceneField(WarmDataset dataset, String protoField) {
        return protoField.equals(dataset.epochField) ? WarmIndex.EPOCH_FIELD : protoField;
    }
}
