// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.stratum;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.map.IMap;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.AgentEvent;

/**
 * CalciteEngine executes Calcite SQL queries over a Hazelcast working-set IMap.
 *
 * Schema: a single table named "events" with columns derived from AgentEvent:
 *   event_id         VARCHAR
 *   timestamp_ms     BIGINT
 *   spiffe_id            VARCHAR
 *   session_id       VARCHAR
 *   agent_id         VARCHAR
 *   event_type       VARCHAR   (enum name)
 *   event_type_value INTEGER   (enum ordinal)
 *
 * Three entry points:
 *   query()        — returns matching AgentEvents (full proto objects)
 *   queryToJson()  — returns matching rows as a JSON array string
 *   getMatchingIds() — returns the set of event_ids matching the SQL (for filterMap)
 */
public final class CalciteEngine {

    private static final Logger log = LoggerFactory.getLogger(CalciteEngine.class);

    /** Column index constants — must match getRowType() order. */
    static final int COL_EVENT_ID         = 0;
    static final int COL_TIMESTAMP_MS     = 1;
    static final int COL_EMAIL            = 2;
    static final int COL_SESSION_ID       = 3;
    static final int COL_AGENT_ID         = 4;
    static final int COL_EVENT_TYPE       = 5;
    static final int COL_EVENT_TYPE_VALUE = 6;

    private CalciteEngine() {}

    /**
     * Executes a Calcite SQL query over the given working-set IMap.
     * Returns the full AgentEvent proto for each matching row.
     */
    public static List<AgentEvent> query(IMap<String, byte[]> imap, String sql) throws Exception {
        Collection<byte[]> snapshot = new ArrayList<>(imap.values());

        try (Connection conn = newConnection()) {
            CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
            registerSchema(calciteConn, snapshot);

            List<AgentEvent> results = new ArrayList<>();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    String eventId = rs.getString(COL_EVENT_ID + 1);
                    byte[] raw = imap.get(eventId);
                    if (raw != null) {
                        results.add(AgentEvent.parseFrom(raw));
                    }
                }
            }
            return results;
        }
    }

    /**
     * Executes a Calcite SQL query and returns up to {@code limit} rows as a JSON array.
     * Also returns the total number of matching rows before the limit is applied.
     * Result: [count, jsonString] packed as Object[2].
     */
    public static Object[] queryToJson(IMap<String, byte[]> imap, String sql, int limit) throws Exception {
        Collection<byte[]> snapshot = new ArrayList<>(imap.values());

        try (Connection conn = newConnection()) {
            CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
            registerSchema(calciteConn, snapshot);

            StringBuilder json = new StringBuilder("[");
            long total = 0;
            boolean first = true;

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();
                String[] colNames = new String[colCount];
                for (int i = 0; i < colCount; i++) {
                    colNames[i] = meta.getColumnLabel(i + 1).toLowerCase();
                }

                while (rs.next()) {
                    total++;
                    if (total <= limit) {
                        if (!first) json.append(",");
                        first = false;
                        json.append("{");
                        for (int i = 0; i < colCount; i++) {
                            if (i > 0) json.append(",");
                            json.append("\"").append(colNames[i]).append("\":");
                            Object val = rs.getObject(i + 1);
                            if (val == null) {
                                json.append("null");
                            } else if (val instanceof Number) {
                                json.append(val);
                            } else {
                                json.append("\"").append(jsonEscape(val.toString())).append("\"");
                            }
                        }
                        json.append("}");
                    }
                }
            }
            json.append("]");
            return new Object[]{total, json.toString()};
        }
    }

    /**
     * Executes a Calcite SQL query and returns the set of event_ids in the result.
     * Used by filterMap to determine which events to keep.
     */
    public static Set<String> getMatchingIds(IMap<String, byte[]> imap, String sql) throws Exception {
        Collection<byte[]> snapshot = new ArrayList<>(imap.values());

        try (Connection conn = newConnection()) {
            CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
            registerSchema(calciteConn, snapshot);

            Set<String> ids = new HashSet<>();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    ids.add(rs.getString(COL_EVENT_ID + 1));
                }
            }
            return ids;
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private static Connection newConnection() throws Exception {
        Properties info = new Properties();
        info.setProperty("lex", "MYSQL");
        return DriverManager.getConnection("jdbc:calcite:", info);
    }

    private static void registerSchema(CalciteConnection calciteConn,
                                       Collection<byte[]> snapshot) throws Exception {
        SchemaPlus rootSchema = calciteConn.getRootSchema();
        rootSchema.add("stratum", new AgentEventSchema(snapshot));
        calciteConn.setSchema("stratum");
    }

    private static String jsonEscape(String s) {
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    // -----------------------------------------------------------------------
    // Calcite schema — wraps the IMap snapshot
    // -----------------------------------------------------------------------

    static final class AgentEventSchema extends AbstractSchema {
        private final Collection<byte[]> snapshot;

        AgentEventSchema(Collection<byte[]> snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        protected java.util.Map<String, org.apache.calcite.schema.Table> getTableMap() {
            return java.util.Map.of("events", new AgentEventTable(snapshot));
        }
    }

    // -----------------------------------------------------------------------
    // Calcite table — scans the snapshot, exposes AgentEvent fields as columns
    // -----------------------------------------------------------------------

    static final class AgentEventTable extends AbstractTable implements ScannableTable {
        private final Collection<byte[]> snapshot;

        AgentEventTable(Collection<byte[]> snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = typeFactory.builder();
            builder.add("event_id",         typeFactory.createSqlType(SqlTypeName.VARCHAR));
            builder.add("timestamp_ms",     typeFactory.createSqlType(SqlTypeName.BIGINT));
            builder.add("spiffe_id",            typeFactory.createSqlType(SqlTypeName.VARCHAR));
            builder.add("session_id",       typeFactory.createSqlType(SqlTypeName.VARCHAR));
            builder.add("agent_id",         typeFactory.createSqlType(SqlTypeName.VARCHAR));
            builder.add("event_type",       typeFactory.createSqlType(SqlTypeName.VARCHAR));
            builder.add("event_type_value", typeFactory.createSqlType(SqlTypeName.INTEGER));
            return builder.build();
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            return new AbstractEnumerable<Object[]>() {
                @Override
                public Enumerator<Object[]> enumerator() {
                    return new AgentEventEnumerator(snapshot.iterator());
                }
            };
        }
    }

    // -----------------------------------------------------------------------
    // Calcite enumerator — deserializes each row on demand
    // -----------------------------------------------------------------------

    static final class AgentEventEnumerator implements Enumerator<Object[]> {
        private final java.util.Iterator<byte[]> iterator;
        private Object[] current;

        AgentEventEnumerator(java.util.Iterator<byte[]> iterator) {
            this.iterator = iterator;
        }

        @Override
        public Object[] current() {
            return current;
        }

        @Override
        public boolean moveNext() {
            while (iterator.hasNext()) {
                byte[] raw = iterator.next();
                try {
                    AgentEvent ev = AgentEvent.parseFrom(raw);
                    current = toRow(ev);
                    return true;
                } catch (Exception e) {
                    log.warn("skipping unparseable AgentEvent in Calcite scan", e);
                }
            }
            return false;
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException("reset not supported");
        }

        @Override
        public void close() {}

        private static Object[] toRow(AgentEvent ev) {
            return new Object[]{
                ev.getEventId(),
                ev.getTimestampMs(),
                ev.getSpiffeId(),
                ev.getSessionId(),
                ev.getAgentId(),
                ev.getEventType().name(),
                ev.getEventTypeValue()
            };
        }
    }
}
