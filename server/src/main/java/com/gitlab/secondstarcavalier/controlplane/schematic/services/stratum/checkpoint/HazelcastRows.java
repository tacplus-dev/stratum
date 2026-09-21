// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.checkpoint;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Message;
import com.hazelcast.core.HazelcastInstance;

/**
 * {@link DailyCheckpoint.Rows} over the live Hazelcast maps.
 *
 * <p>The dataset name IS the map name — one identifier names the type in every
 * tier — so this needs the generated list of names and nothing else per type.
 */
public final class HazelcastRows implements DailyCheckpoint.Rows {

    private final HazelcastInstance hz;
    private final List<String> mapNames;

    public HazelcastRows(HazelcastInstance hz, List<String> mapNames) {
        this.hz = hz;
        this.mapNames = List.copyOf(mapNames);
    }

    @Override
    public Collection<String> mapNames() {
        return mapNames;
    }

    @Override
    public Collection<Map.Entry<String, Message>> entries(String mapName) {
        return hz.<String, Message>getMap(mapName).entrySet();
    }

    @Override
    public boolean remove(String mapName, String key, Message expected) {
        return hz.<String, Message>getMap(mapName).remove(key, expected);
    }
}
