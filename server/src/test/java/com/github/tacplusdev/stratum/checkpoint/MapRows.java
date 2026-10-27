// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.checkpoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Message;

/** {@link DailyCheckpoint.Rows} over plain maps, so a test can hold rows of any type. */
final class MapRows implements DailyCheckpoint.Rows {

    private final Map<String, Map<String, Message>> maps = new LinkedHashMap<>();

    MapRows put(String mapName, String key, Message value) {
        maps.computeIfAbsent(mapName, k -> new LinkedHashMap<>()).put(key, value);
        return this;
    }

    Map<String, Message> map(String mapName) {
        return maps.getOrDefault(mapName, Map.of());
    }

    @Override
    public Collection<String> mapNames() {
        return maps.keySet();
    }

    @Override
    public Collection<Map.Entry<String, Message>> entries(String mapName) {
        // A copy: the purge step removes while it iterates.
        return new ArrayList<>(maps.getOrDefault(mapName, Map.of()).entrySet());
    }

    @Override
    public boolean remove(String mapName, String key, Message expected) {
        Map<String, Message> m = maps.get(mapName);
        if (m == null || !expected.equals(m.get(key))) {
            return false;
        }
        m.remove(key);
        return true;
    }

    /** Every key still held, across every map, for an assertion. */
    List<String> keys() {
        List<String> out = new ArrayList<>();
        for (Map<String, Message> m : maps.values()) {
            out.addAll(m.keySet());
        }
        return out;
    }
}
