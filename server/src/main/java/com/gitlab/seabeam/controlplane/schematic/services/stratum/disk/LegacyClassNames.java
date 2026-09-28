package com.gitlab.seabeam.controlplane.schematic.services.stratum.disk;

/**
 * Rows on disk (checkpoint dumps, the Lucene disk leg) carry the Java class name of
 * their protobuf message. The org rename of 2026-09-28 moved every generated class from
 * {@code com.gitlab.secondstarcavalier} to {@code com.gitlab.seabeam}; data written before
 * that day still names the old package. Map it on read so an existing store restores.
 *
 * <p>This is a compatibility shim, not the fix: storing a Java class name ties the durable
 * format to a package layout. The fix is to store the protobuf full name and resolve it
 * through the generated registry (filed with the schematic looper the same day).
 */
public final class LegacyClassNames {
    private static final String OLD = "com.gitlab.secondstarcavalier.";
    private static final String NEW = "com.gitlab.seabeam.";

    private LegacyClassNames() {}

    public static String map(String className) {
        return className.startsWith(OLD) ? NEW + className.substring(OLD.length()) : className;
    }
}
