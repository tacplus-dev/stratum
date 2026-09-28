package com.gitlab.seabeam.controlplane.schematic.services.stratum.hazelcast;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.disk.LegacyClassNames;

/**
 * Class loader handed to Hazelcast so values serialized before the 2026-09-28 org rename
 * (redo-log entries carry the Java class name of their message) still resolve. Same
 * compatibility posture as {@link LegacyClassNames}; goes away with the durable-format fix.
 */
public final class LegacyClassNameLoader extends ClassLoader {
    public LegacyClassNameLoader(ClassLoader parent) {
        super(parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        return super.loadClass(LegacyClassNames.map(name), resolve);
    }
}
