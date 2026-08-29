// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.FSDirectory;

/**
 * {@code slice.json} — written ONCE, when a slice is sealed.
 *
 * It exists so an append-only dataset's sealed slices can be shown not to be
 * rewritten: the checksum is over the sealed segment files, so a stable checksum
 * across a write load is the evidence, not a claim. It is deliberately NEVER read
 * by the query path — pruning is done from the directory name alone, and a
 * manifest that had to be opened to be consulted would cost exactly what the
 * layout exists to avoid.
 */
public final class WarmSliceMeta {

    static final String FILE_NAME = "slice.json";

    private WarmSliceMeta() {}

    /**
     * Writes the slice's manifest. Called at rollover, on the slice being closed.
     *
     * A slice reopened later for a boundary-crossing delete or a retention drop is
     * re-sealed the next time it is closed, so the manifest always describes the
     * slice as it stands rather than as it once was.
     */
    static void seal(Path sliceDir, String sliceId) throws IOException {
        long docs = 0;
        long minEpoch = Long.MAX_VALUE;
        long maxEpoch = Long.MIN_VALUE;
        try (FSDirectory dir = FSDirectory.open(sliceDir);
             DirectoryReader reader = DirectoryReader.open(dir)) {
            docs = reader.numDocs();
            for (LeafReaderContext leaf : reader.leaves()) {
                NumericDocValues epochs = leaf.reader().getNumericDocValues(WarmIndex.EPOCH_FIELD);
                if (epochs == null) {
                    continue;
                }
                for (int doc = epochs.nextDoc(); doc != NumericDocValues.NO_MORE_DOCS; doc = epochs.nextDoc()) {
                    long v = epochs.longValue();
                    minEpoch = Math.min(minEpoch, v);
                    maxEpoch = Math.max(maxEpoch, v);
                }
            }
        }
        if (docs == 0) {
            minEpoch = 0;
            maxEpoch = 0;
        }
        String json = "{\n"
                + "  \"slice_id\": \"" + sliceId + "\",\n"
                + "  \"doc_count\": " + docs + ",\n"
                + "  \"min_epoch_ms\": " + minEpoch + ",\n"
                + "  \"max_epoch_ms\": " + maxEpoch + ",\n"
                + "  \"sha256\": \"" + checksum(sliceDir) + "\"\n"
                + "}\n";
        Files.writeString(sliceDir.resolve(FILE_NAME), json, StandardCharsets.UTF_8);
    }

    /**
     * SHA-256 over the slice's segment files, name and content, in sorted order.
     *
     * {@code slice.json} itself and {@code write.lock} are excluded: the manifest
     * cannot hash itself, and the lock is process state rather than data.
     */
    static String checksum(Path sliceDir) throws IOException {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the JLS platform spec", e);
        }
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> children = Files.newDirectoryStream(sliceDir)) {
            for (Path child : children) {
                String name = child.getFileName().toString();
                if (Files.isRegularFile(child) && !name.equals(FILE_NAME) && !name.equals("write.lock")) {
                    files.add(child);
                }
            }
        }
        files.sort((a, b) -> a.getFileName().toString().compareTo(b.getFileName().toString()));
        byte[] buf = new byte[8192];
        for (Path f : files) {
            md.update(f.getFileName().toString().getBytes(StandardCharsets.UTF_8));
            try (InputStream in = Files.newInputStream(f)) {
                int n;
                while ((n = in.read(buf)) > 0) {
                    md.update(buf, 0, n);
                }
            }
        }
        StringBuilder hex = new StringBuilder(64);
        for (byte b : md.digest()) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return hex.toString();
    }

    /** The slice ids that carry a sealed manifest. Test/observability only. */
    static List<String> sealed(Path datasetRoot) throws IOException {
        List<String> out = new ArrayList<>();
        for (String slice : WarmSlices.list(datasetRoot)) {
            if (Files.isRegularFile(datasetRoot.resolve(slice).resolve(FILE_NAME))) {
                out.add(slice);
            }
        }
        Collections.sort(out);
        return out;
    }
}
