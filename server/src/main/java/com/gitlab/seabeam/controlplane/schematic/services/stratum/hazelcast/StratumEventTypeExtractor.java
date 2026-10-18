// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hazelcast;

/**
 * StratumEventTypeExtractor provides the attribute name for the
 * event_type_value index on the hot event store.
 *
 * Hazelcast reflects on AgentEvent.getEventTypeValue() using this attribute
 * name for HASH-indexed predicate queries against the events IMap.
 */
public final class StratumEventTypeExtractor {

    /**
     * Hazelcast attribute name that maps to AgentEvent.getEventTypeValue().
     * Used in Predicates.equal(ATTRIBUTE_NAME, eventType.getNumber()).
     */
    public static final String ATTRIBUTE_NAME = "eventTypeValue";

    private StratumEventTypeExtractor() {}
}
