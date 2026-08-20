// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.collection.CollectionRuntime;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.servicemanager.Caller;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.s2sm.internal.grpc.servers.vservices.agentsensor.StratumQuery;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.services.stratum.Stratum;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.sys.errors.ProtoException;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.AgentEvent;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.SessionSummary;

/**
 * Real implementation of the StratumQuery vservice handler, backed by
 * the EventStore (hot event store, Hazelcast) for event/session queries.
 *
 * Session aggregation: folds AgentEvent records grouped by session_id into
 * SessionSummary. Active sessions are those whose last event is within the
 * configured idle threshold. Paging uses keyset pagination via
 * {@link StratumHandler#pageSorted}.
 *
 * subscribe_live_sessions: server-streaming driven by the ingest path —
 * each incoming AgentEvent is pushed to registered watchers via
 * {@link #onEvent(AgentEvent)}.
 */
final class StratumQueryImpl implements StratumQuery.StratumQueryHandler {

    private static final Logger log = LoggerFactory.getLogger(StratumQueryImpl.class);

    private final Supplier<EventStore> eventStoreSupplier;
    private final Supplier<Stratum.Config> configSupplier;

    // Live session watchers, registered by subscribeLiveSessions.
    private final CopyOnWriteArrayList<LiveWatcher> liveWatchers = new CopyOnWriteArrayList<>();

    StratumQueryImpl(Supplier<EventStore> eventStoreSupplier,
                     Supplier<Stratum.Config> configSupplier) {
        this.eventStoreSupplier = eventStoreSupplier;
        this.configSupplier = configSupplier;
    }

    private EventStore eventStore() { return eventStoreSupplier.get(); }

    private int idleThresholdSeconds() {
        Stratum.Config cfg = configSupplier.get();
        int v = cfg != null ? cfg.getActiveSessionIdleSeconds() : 0;
        return v > 0 ? v : 900;
    }

    // -----------------------------------------------------------------------
    // Live session fan-out (called from StratumHandler.handleEventStreamMessage)
    // -----------------------------------------------------------------------

    static final class LiveWatcher {
        final StreamObserver<Agentsensor.WatchLiveSessionsEvent> obs;
        final String filterSpiffeId;
        final boolean includeRawEvents;

        LiveWatcher(StreamObserver<Agentsensor.WatchLiveSessionsEvent> obs,
                    String filterSpiffeId, boolean includeRawEvents) {
            this.obs = obs;
            this.filterSpiffeId = filterSpiffeId;
            this.includeRawEvents = includeRawEvents;
        }

        boolean matches(AgentEvent ev) {
            return filterSpiffeId == null || filterSpiffeId.isEmpty()
                    || filterSpiffeId.equals(ev.getSpiffeId());
        }

        synchronized void send(Agentsensor.WatchLiveSessionsEvent evt) {
            obs.onNext(evt);
        }
    }

    /**
     * Called from the ingest path for every AgentEvent. Pushes live session
     * updates to all registered watchers. Thread-safe: CopyOnWriteArrayList
     * for iteration, synchronized send per watcher.
     */
    void onEvent(AgentEvent event) {
        if (liveWatchers.isEmpty()) return;
        long nowMs = System.currentTimeMillis();
        int idleSecs = idleThresholdSeconds();
        SessionSummary summary = buildQuickSummary(event, nowMs, idleSecs);

        Agentsensor.WatchLiveSessionsEvent updateEvt = Agentsensor.WatchLiveSessionsEvent.newBuilder()
                .setSessionUpdated(summary).build();
        Agentsensor.WatchLiveSessionsEvent rawEvt = null;
        for (LiveWatcher w : liveWatchers) {
            if (!w.matches(event)) continue;
            try {
                if (w.includeRawEvents) {
                    if (rawEvt == null) {
                        rawEvt = Agentsensor.WatchLiveSessionsEvent.newBuilder()
                                .setLiveEvent(event).build();
                    }
                    w.send(rawEvt);
                }
                w.send(updateEvt);
            } catch (Exception e) {
                liveWatchers.remove(w);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Session / event query RPCs (the argus-facing contract)
    // -----------------------------------------------------------------------

    @Override
    public CollectionRuntime.Page<SessionSummary> listActiveSessions(
            Caller caller, Agentsensor.ListActiveSessionsRequest req,
            CollectionRuntime.PageParams page) throws ProtoException {
        try {
            int idleSecs = idleThresholdSeconds();
            long cutoffMs = System.currentTimeMillis() - (long) idleSecs * 1000;
            EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
            preds.startMs = cutoffMs;
            List<AgentEvent> events = eventStore().query(preds);
            List<SessionSummary> sessions = buildSessionSummaries(events);
            // Only return sessions that are actually active.
            sessions.removeIf(s -> !s.getIsActive());
            return StratumHandler.pageSorted(sessions, page,
                    s -> List.of(StratumHandler.i64Key(s.getLastSeenMs()), s.getSessionId()));
        } catch (Exception e) {
            throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
        }
    }

    @Override
    public CollectionRuntime.Page<AgentEvent> listSessionEvents(
            Caller caller, Agentsensor.GetSessionEventsRequest req,
            CollectionRuntime.PageParams page) throws ProtoException {
        if (req.getSessionId().isEmpty()) {
            throw new ProtoException(ProtoException.INVALID_ARGUMENT, "session_id is required");
        }
        try {
            EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
            preds.sessionId = req.getSessionId();
            applyTimeRange(preds, req.hasTimeRange() ? req.getTimeRange() : null);
            List<AgentEvent> events = eventStore().query(preds);
            return StratumHandler.pageSorted(events, page,
                    e -> List.of(StratumHandler.i64Key(e.getTimestampMs()), e.getEventId()));
        } catch (ProtoException pe) {
            throw pe;
        } catch (Exception e) {
            throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
        }
    }

    @Override
    public CollectionRuntime.Page<SessionSummary> listUserSessions(
            Caller caller, Agentsensor.GetUserSessionsRequest req,
            CollectionRuntime.PageParams page) throws ProtoException {
        if (req.getSpiffeId().isEmpty()) {
            throw new ProtoException(ProtoException.INVALID_ARGUMENT, "spiffe_id is required");
        }
        try {
            EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
            preds.spiffeId = req.getSpiffeId();
            applyTimeRange(preds, req.hasTimeRange() ? req.getTimeRange() : null);
            List<AgentEvent> events = eventStore().query(preds);
            List<SessionSummary> sessions = buildSessionSummaries(events);
            return StratumHandler.pageSorted(sessions, page,
                    s -> List.of(StratumHandler.i64Key(s.getLastSeenMs()), s.getSessionId()));
        } catch (ProtoException pe) {
            throw pe;
        } catch (Exception e) {
            throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
        }
    }

    @Override
    public CollectionRuntime.Page<AgentEvent> listEventsByTimeRange(
            Caller caller, Agentsensor.ListEventsByTimeRangeRequest req,
            CollectionRuntime.PageParams page) throws ProtoException {
        if (!req.hasTimeRange()) {
            throw new ProtoException(ProtoException.INVALID_ARGUMENT, "time_range is required");
        }
        Agentsensor.EventTimeRange tr = req.getTimeRange();
        if (tr.getStartMs() <= 0 || tr.getEndMs() <= 0 || tr.getStartMs() >= tr.getEndMs()) {
            throw new ProtoException(ProtoException.INVALID_ARGUMENT,
                    "time_range requires start_ms < end_ms, both > 0");
        }
        try {
            EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
            applyTimeRange(preds, tr);
            List<AgentEvent> events = eventStore().query(preds);
            return StratumHandler.pageSorted(events, page,
                    e -> List.of(StratumHandler.i64Key(e.getTimestampMs()), e.getEventId()));
        } catch (ProtoException pe) {
            throw pe;
        } catch (Exception e) {
            throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
        }
    }

    @Override
    public CollectionRuntime.Page<AgentEvent> listEventsByActor(
            Caller caller, Agentsensor.ListEventsByActorRequest req,
            CollectionRuntime.PageParams page) throws ProtoException {
        if (req.getSpiffeId().isEmpty()) {
            throw new ProtoException(ProtoException.INVALID_ARGUMENT, "spiffe_id is required");
        }
        try {
            EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
            preds.spiffeId = req.getSpiffeId();
            applyTimeRange(preds, req.hasTimeRange() ? req.getTimeRange() : null);
            List<AgentEvent> events = eventStore().query(preds);
            return StratumHandler.pageSorted(events, page,
                    e -> List.of(StratumHandler.i64Key(e.getTimestampMs()), e.getEventId()));
        } catch (ProtoException pe) {
            throw pe;
        } catch (Exception e) {
            throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
        }
    }

    // -----------------------------------------------------------------------
    // Live session streaming
    // -----------------------------------------------------------------------

    @Override
    public void subscribeLiveSessions(Caller caller, Agentsensor.WatchLiveSessionsRequest req,
                                      StreamObserver<Agentsensor.WatchLiveSessionsEvent> obs) throws ProtoException {
        LiveWatcher w = new LiveWatcher(obs, req.getFilterSpiffeId(), req.getIncludeRawEvents());
        liveWatchers.add(w);
        if (obs instanceof io.grpc.stub.ServerCallStreamObserver) {
            ((io.grpc.stub.ServerCallStreamObserver<?>) obs)
                    .setOnCancelHandler(() -> liveWatchers.remove(w));
        }
        // Stream stays open until the client disconnects — never calls onCompleted().
    }

    // -----------------------------------------------------------------------
    // Session aggregation
    // -----------------------------------------------------------------------

    /**
     * Folds a list of AgentEvent into SessionSummary, one per session_id.
     * Deterministic: events are grouped by session_id; within each group,
     * first/last timestamps and max depth are tracked. Active/idle is
     * computed relative to wall-clock now.
     */
    List<SessionSummary> buildSessionSummaries(List<AgentEvent> events) {
        long nowMs = System.currentTimeMillis();
        int idleThresh = idleThresholdSeconds();
        return buildSessionSummaries(events, nowMs, idleThresh);
    }

    static List<SessionSummary> buildSessionSummaries(List<AgentEvent> events, long nowMs, int idleThresholdSeconds) {
        Map<String, SessionSummary.Builder> bySession = new LinkedHashMap<>();
        for (AgentEvent ev : events) {
            if (ev.getSessionId().isEmpty()) continue;
            bySession.compute(ev.getSessionId(), (k, b) -> {
                if (b == null) {
                    b = SessionSummary.newBuilder()
                            .setSessionId(ev.getSessionId())
                            .setSpiffeId(ev.getSpiffeId())
                            .setWorkingDirectory(ev.getWorkingDirectory())
                            .setSensorType(ev.getSensorType())
                            .setFirstSeenMs(ev.getTimestampMs())
                            .setLastSeenMs(ev.getTimestampMs())
                            .setLatestEventType(ev.getEventType());
                }
                if (ev.getTimestampMs() < b.getFirstSeenMs()) b.setFirstSeenMs(ev.getTimestampMs());
                if (ev.getTimestampMs() > b.getLastSeenMs()) {
                    b.setLastSeenMs(ev.getTimestampMs()).setLatestEventType(ev.getEventType());
                }
                if (ev.getDepth() > b.getMaxDepth()) b.setMaxDepth(ev.getDepth());
                return b.setEventCount(b.getEventCount() + 1);
            });
        }
        List<SessionSummary> result = new ArrayList<>(bySession.size());
        for (SessionSummary.Builder b : bySession.values()) {
            long idleSecs = (nowMs - b.getLastSeenMs()) / 1000;
            boolean active = idleSecs <= idleThresholdSeconds;
            result.add(b.setIsActive(active).setIdleSeconds(active ? 0 : idleSecs).build());
        }
        return result;
    }

    static SessionSummary buildQuickSummary(AgentEvent ev, long nowMs, int idleThresholdSeconds) {
        long idleSecs = (nowMs - ev.getTimestampMs()) / 1000;
        boolean active = idleSecs <= idleThresholdSeconds;
        return SessionSummary.newBuilder()
                .setSessionId(ev.getSessionId())
                .setSpiffeId(ev.getSpiffeId())
                .setWorkingDirectory(ev.getWorkingDirectory())
                .setSensorType(ev.getSensorType())
                .setLastSeenMs(ev.getTimestampMs())
                .setLatestEventType(ev.getEventType())
                .setIsActive(active)
                .setIdleSeconds(active ? 0 : idleSecs)
                .build();
    }

    private static void applyTimeRange(EventStore.SqlPredicates preds, Agentsensor.EventTimeRange range) {
        if (range == null || range == Agentsensor.EventTimeRange.getDefaultInstance()) return;
        if (range.getStartMs() > 0) preds.startMs = range.getStartMs();
        if (range.getEndMs() > 0) preds.endMs = range.getEndMs();
    }
}
