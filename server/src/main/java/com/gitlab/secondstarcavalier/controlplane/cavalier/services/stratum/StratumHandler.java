// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.s2sm.ServiceManager;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.s2sm.internal.grpc.servers.vservices.claudesensor.StratumQuery;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.libs.collection.CollectionRuntime;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.services.stratum.Stratum;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.services.stratum.Helpers;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.CreateMapRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.CreateMapResponse;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.DeleteMapRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.DeleteMapResponse;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.EventTimeRange;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.ExecuteMapQueryRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.ExecuteMapQueryResponse;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.GetMapDetailRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.GetMapDetailResponse;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.GetSessionEventsRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.GetUserSessionsRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.ListActiveSessionsRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.ListMapsRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.ListMapsResponse;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.MapSizeRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.MapSizeResponse;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.MergeRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.MergeResponse;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.ListEventsByEmailRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.ListEventsByTimeRangeRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.SensorEvent;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.SessionSummary;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.UpdateMapByGraphdbRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.UpdateMapByGraphdbResponse;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.UpdateMapByIdsRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.UpdateMapByIdsResponse;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.UpdateMapBySqlRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.UpdateMapBySqlResponse;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.UpdateMapByTimedbRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.UpdateMapByTimedbResponse;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.UpdateMapFilterRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.UpdateMapFilterResponse;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.WatchLiveSessionsEvent;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.WatchLiveSessionsRequest;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.libs.servicemanager.Caller;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.libs.errors.ProtoException;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.libs.x509.X509;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.llmsensor.Llmsensor;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.scmsensor.Scmsensor;

/**
 * StratumHandler wires together EventStore, WorkingSetStore, and the gRPC
 * StratumQuery service.
 *
 * Startup sequence:
 *   1. provideStratumQueryHandler() — initialises EventStore (hot event
 *      Hazelcast cluster) and WorkingSetStore (working-set Hazelcast cluster)
 *      from config defaults, then returns StratumQueryImpl for registration.
 *   2. handleConfig() — re-applies config when kernel delivers it.
 *   3. onEventStream() begins receiving SensorEvents from the EventBus.
 *
 * EventBus subscription is auto-generated by tacplus from (spec.hot_store).
 */
public class StratumHandler extends StratumVServiceBase {

    private static final Logger log = LoggerFactory.getLogger(StratumHandler.class);

    private final ServiceManager sm;
    private volatile Stratum.Config config;
    private EventStore      eventStore;
    private WorkingSetStore store;
    private final CopyOnWriteArrayList<LiveWatcher> liveWatchers = new CopyOnWriteArrayList<>();

    private static final class LiveWatcher {
        final StreamObserver<WatchLiveSessionsEvent> obs;
        final String filterEmail;
        final boolean includeRawEvents;

        LiveWatcher(StreamObserver<WatchLiveSessionsEvent> obs, String filterEmail, boolean includeRawEvents) {
            this.obs = obs;
            this.filterEmail = filterEmail;
            this.includeRawEvents = includeRawEvents;
        }

        boolean matches(SensorEvent ev) {
            return filterEmail == null || filterEmail.isEmpty() || filterEmail.equals(ev.getEmail());
        }

        // gRPC ServerCallStreamObserver must not receive concurrent onNext calls.
        synchronized void send(WatchLiveSessionsEvent evt) {
            obs.onNext(evt);
        }
    }

    public StratumHandler(ServiceManager sm) {
        this.sm = sm;
    }

    // -----------------------------------------------------------------------
    // StratumVServiceBase — vservice wiring
    // -----------------------------------------------------------------------

    @Override
    protected StratumQuery.StratumQueryHandler provideStratumQueryHandler() {
        try {
            Stratum.Config cfg = Helpers.configDefaults(Stratum.Config.getDefaultInstance()).build();
            initFromConfig(cfg);
        } catch (Exception e) {
            log.error("failed to initialize stratum", e);
            throw new RuntimeException("stratum init failed", e);
        }
        return new StratumQueryImpl();
    }

    @Override
    protected void handleConfig(Stratum.Config cfg) throws Exception {
        initFromConfig(cfg);
    }

    @Override
    public void handleStratumConfigStateConfig(Stratum.Config cfg) throws Exception {
        initFromConfig(cfg);
    }

    @Override
    public void handleStratumStateConfig(Stratum.Config cfg) throws Exception {
        // StratumState has no config singleton; nothing to do.
    }

    @Override
    public com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.s2sm.internal.grpc.servers.services.stratum.StratumStateService.StratumStateServiceHandler getStratumStateGrpcImpl() {
        return new com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.s2sm.internal.grpc.servers.services.stratum.StratumStateService.StratumStateServiceHandler() {};
    }

    @Override
    public com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.s2sm.internal.grpc.servers.services.stratum.StratumConfigStateService.StratumConfigStateServiceHandler getStratumConfigStateGrpcImpl() {
        return new com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.s2sm.internal.grpc.servers.services.stratum.StratumConfigStateService.StratumConfigStateServiceHandler() {
            @Override
            public Stratum.Config getConfig(Caller caller) throws ProtoException {
                Stratum.Config cfg = config;
                return cfg != null ? cfg : Stratum.Config.getDefaultInstance();
            }
            @Override
            public Stratum.Config setConfig(Caller caller, Stratum.Config request) throws ProtoException {
                try {
                    initFromConfig(request);
                } catch (Exception e) {
                    throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
                }
                return request;
            }
            @Override
            public void watchConfig(Caller caller, StreamObserver<Stratum.Config> responseObserver) throws ProtoException {
                Stratum.Config cfg = config;
                if (cfg != null) responseObserver.onNext(cfg);
                responseObserver.onCompleted();
            }
        };
    }

    private void initFromConfig(Stratum.Config cfg) {
        this.config = cfg;
        log.info("config: hazelcast_memory_mb={}, map_idle_ttl_seconds={}, max_maps={}, " +
                 "retention_days={}, cleanup_interval_seconds={}",
                cfg.getHazelcastMemoryMb(), cfg.getMapIdleTtlSeconds(), cfg.getMaxMaps(),
                cfg.getRetentionDays(), cfg.getCleanupIntervalSeconds());

        eventStore = new EventStore();
        eventStore.init(
                cfg.getRetentionDays(),
                cfg.getCleanupIntervalSeconds(),
                List.of()); // seed members: empty = single-node / multicast

        store = new WorkingSetStore(cfg);
        store.init();
    }

    @Override
    public void newTlsCert(X509.X509CertChain certChain, byte[] privateKeyDer) throws Exception {
        // mTLS for EventBus subscription is handled by the generated ServiceManager.
    }

    // -----------------------------------------------------------------------
    // EventBus subscription (auto-wired by (spec.hot_store))
    // -----------------------------------------------------------------------

    @Override
    public void onEventStreamClaudesensorAgenticCodeEventBusEventStream(SensorEvent event, SubscriptionMeta meta) throws Exception {
        eventStore.put(event);
        pushToLiveWatchers(event);
    }

    @Override
    public void onEventStreamLlmsensorLlmEventBusEventStream(Llmsensor.LlmEvent ev, SubscriptionMeta meta) throws Exception {
        eventStore.putLlmRaw(ev.getEventId(), ev.toByteArray());
        log.debug("stored llm event: event_id={} email={}", ev.getEventId(), ev.getEmail());
    }

    @Override
    public void onEventStreamScmsensorScmEventBusEventStream(Scmsensor.ScmEvent ev, SubscriptionMeta meta) throws Exception {
        eventStore.putScmRaw(ev.getEventId(), ev.toByteArray());
        log.debug("stored scm event: event_id={} email={}", ev.getEventId(), ev.getEmail());
    }

    /**
     * Fallback handler for any new vservice sensors added in the future.
     * The generated Handler routes unhandled topics here via the typed default
     * methods, so new sensors are surfaced (logged) until a typed override is added.
     */
    @Override
    public void handleEventStreamMessage(String topic, com.google.protobuf.Message event, SubscriptionMeta meta) throws Exception {
        log.warn("unhandled event stream topic: {} (offset={})", topic, meta.offset);
    }

    private void pushToLiveWatchers(SensorEvent event) {
        if (liveWatchers.isEmpty()) return;
        long nowMs = System.currentTimeMillis();
        int idleSecs = config != null ? config.getActiveSessionIdleSeconds() : 300;
        // Note: sessionUpdated contains a single-event snapshot (not a full session aggregate).
        // Clients should treat it as a "live heartbeat" rather than authoritative session state.
        // Use listActiveSessions or getSessionEvents for full aggregates.
        SessionSummary summary = buildQuickSummary(event, nowMs, idleSecs);
        WatchLiveSessionsEvent updateEvt = WatchLiveSessionsEvent.newBuilder()
                .setSessionUpdated(summary).build();
        WatchLiveSessionsEvent rawEvt = WatchLiveSessionsEvent.newBuilder()
                .setLiveEvent(event).build();
        for (LiveWatcher w : liveWatchers) {
            if (!w.matches(event)) continue;
            try {
                if (w.includeRawEvents) w.send(rawEvt);
                w.send(updateEvt);
            } catch (Exception e) {
                liveWatchers.remove(w);
            }
        }
    }

    @Override
    public void shutdown() {
        log.info("stratum shutting down");
        if (eventStore != null) eventStore.shutdown();
        if (store != null)      store.shutdown();
    }

    // -----------------------------------------------------------------------
    // gRPC service implementation
    // -----------------------------------------------------------------------

    private class StratumQueryImpl implements StratumQuery.StratumQueryHandler {

        // --- Map lifecycle ---

        @Override
        public CreateMapResponse createMap(Caller caller, CreateMapRequest req) throws ProtoException {
            try {
                store.createMap(req.getMapId());
                return CreateMapResponse.getDefaultInstance();
            } catch (IllegalArgumentException e) {
                throw new ProtoException(ProtoException.INVALID_ARGUMENT, e.getMessage());
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public DeleteMapResponse deleteMap(Caller caller, DeleteMapRequest req) throws ProtoException {
            try {
                store.deleteMap(req.getMapId());
                return DeleteMapResponse.getDefaultInstance();
            } catch (IllegalArgumentException e) {
                throw new ProtoException(ProtoException.INVALID_ARGUMENT, e.getMessage());
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public ListMapsResponse listMaps(Caller caller, ListMapsRequest req) throws ProtoException {
            try {
                return ListMapsResponse.newBuilder().addAllMaps(store.listMaps()).build();
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public GetMapDetailResponse getMapDetail(Caller caller, GetMapDetailRequest req) throws ProtoException {
            try {
                return GetMapDetailResponse.newBuilder()
                        .setDetail(store.describeMap(req.getMapId())).build();
            } catch (IllegalArgumentException e) {
                throw new ProtoException(ProtoException.INVALID_ARGUMENT, e.getMessage());
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public MapSizeResponse getMapSize(Caller caller, MapSizeRequest req) throws ProtoException {
            try {
                long[] sz = store.mapSize(req.getMapId());
                return MapSizeResponse.newBuilder()
                        .setEventCount(sz[0])
                        .setSizeMb(sz[1])
                        .build();
            } catch (IllegalArgumentException e) {
                throw new ProtoException(ProtoException.INVALID_ARGUMENT, e.getMessage());
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        // --- Loading ---

        @Override
        public UpdateMapByIdsResponse updateMapByIds(Caller caller, UpdateMapByIdsRequest req) throws ProtoException {
            try {
                long[] added = {0};
                for (String id : req.getEventIdsList()) {
                    eventStore.getByKey(id).ifPresent(ev -> {
                        store.putEvent(req.getMapId(), ev);
                        added[0]++;
                    });
                }
                return UpdateMapByIdsResponse.newBuilder().setEventsAdded(added[0]).build();
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public UpdateMapBySqlResponse updateMapBySql(Caller caller, UpdateMapBySqlRequest req) throws ProtoException {
            try {
                EventStore.SqlPredicates preds = EventStore.SqlParser.parse(req.getSql());
                List<SensorEvent> events = eventStore.query(preds);
                for (SensorEvent ev : events) {
                    store.putEvent(req.getMapId(), ev);
                }
                log.info("load_by_sql into {}: {} events loaded", req.getMapId(), events.size());
                return UpdateMapBySqlResponse.newBuilder().setEventsAdded(events.size()).build();
            } catch (IllegalArgumentException e) {
                throw new ProtoException(ProtoException.INVALID_ARGUMENT, e.getMessage());
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public UpdateMapByTimedbResponse updateMapByTimedb(Caller caller, UpdateMapByTimedbRequest req) throws ProtoException {
            throw new ProtoException(ProtoException.UNIMPLEMENTED, "load_by_timedb not yet implemented");
        }

        @Override
        public UpdateMapByGraphdbResponse updateMapByGraphdb(Caller caller, UpdateMapByGraphdbRequest req) throws ProtoException {
            throw new ProtoException(ProtoException.UNIMPLEMENTED, "load_by_graphdb not yet implemented");
        }

        // --- Map mutation ---

        @Override
        public UpdateMapFilterResponse updateMapFilter(Caller caller, UpdateMapFilterRequest req) throws ProtoException {
            try {
                long removed = store.filterMap(req.getMapId(), req.getSql());
                return UpdateMapFilterResponse.newBuilder().setEventsRemoved(removed).build();
            } catch (IllegalArgumentException e) {
                throw new ProtoException(ProtoException.INVALID_ARGUMENT, e.getMessage());
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public MergeResponse merge(Caller caller, MergeRequest req) throws ProtoException {
            try {
                long added = store.merge(req.getSrcMapId(), req.getDestMapId());
                return MergeResponse.newBuilder().setEventsAdded(added).build();
            } catch (IllegalArgumentException e) {
                throw new ProtoException(ProtoException.INVALID_ARGUMENT, e.getMessage());
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        // --- Read-only query ---

        @Override
        public ExecuteMapQueryResponse executeMapQuery(Caller caller, ExecuteMapQueryRequest req) throws ProtoException {
            try {
                int limit = req.getLimit() > 0 ? req.getLimit() : 100;
                Object[] result = (Object[]) store.queryMap(req.getMapId(), req.getSql(), limit);
                return ExecuteMapQueryResponse.newBuilder()
                        .setResultsJson((String) result[1])
                        .setTotalRows((Long) result[0])
                        .build();
            } catch (IllegalArgumentException e) {
                throw new ProtoException(ProtoException.INVALID_ARGUMENT, e.getMessage());
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        // --- Operator queries ---

        @Override
        public CollectionRuntime.Page<SessionSummary> listActiveSessions(Caller caller,
                                                                         ListActiveSessionsRequest req,
                                                                         CollectionRuntime.PageParams page) throws ProtoException {
            try {
                int idleSecs  = config != null ? config.getActiveSessionIdleSeconds() : 300;
                long cutoffMs = System.currentTimeMillis() - (long) idleSecs * 1000;
                EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
                preds.startMs = cutoffMs;
                List<SensorEvent> events = eventStore.query(preds);
                List<SessionSummary> sessions = buildSessionSummaries(events);
                return pageSorted(sessions, page, s -> List.of(i64Key(s.getLastSeenMs()), s.getSessionId()));
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public CollectionRuntime.Page<SensorEvent> listSessionEvents(Caller caller,
                                                                     GetSessionEventsRequest req,
                                                                     CollectionRuntime.PageParams page) throws ProtoException {
            try {
                EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
                preds.sessionId = req.getSessionId();
                applyTimeRange(preds, req.hasTimeRange() ? req.getTimeRange() : null);
                List<SensorEvent> events = eventStore.query(preds);
                return pageSorted(events, page, e -> List.of(i64Key(e.getTimestampMs()), e.getEventId()));
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public CollectionRuntime.Page<SessionSummary> listUserSessions(Caller caller,
                                                                       GetUserSessionsRequest req,
                                                                       CollectionRuntime.PageParams page) throws ProtoException {
            try {
                EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
                preds.email = req.getEmail();
                applyTimeRange(preds, req.hasTimeRange() ? req.getTimeRange() : null);
                List<SensorEvent> events = eventStore.query(preds);
                List<SessionSummary> sessions = buildSessionSummaries(events);
                return pageSorted(sessions, page, s -> List.of(i64Key(s.getLastSeenMs()), s.getSessionId()));
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public CollectionRuntime.Page<SensorEvent> listEventsByTimeRange(Caller caller,
                                                                         ListEventsByTimeRangeRequest req,
                                                                         CollectionRuntime.PageParams page) throws ProtoException {
            EventTimeRange tr = req.getTimeRange();
            if (tr.getStartMs() <= 0 || tr.getEndMs() <= 0 || tr.getStartMs() >= tr.getEndMs()) {
                throw new ProtoException(ProtoException.INVALID_ARGUMENT,
                        "time_range requires start_ms < end_ms, both > 0");
            }
            try {
                EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
                applyTimeRange(preds, tr);
                List<SensorEvent> events = eventStore.query(preds);
                return pageSorted(events, page, e -> List.of(i64Key(e.getTimestampMs()), e.getEventId()));
            } catch (ProtoException pe) {
                throw pe;
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public CollectionRuntime.Page<SensorEvent> listEventsByEmail(Caller caller,
                                                                     ListEventsByEmailRequest req,
                                                                     CollectionRuntime.PageParams page) throws ProtoException {
            try {
                EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
                preds.email = req.getEmail();
                applyTimeRange(preds, req.hasTimeRange() ? req.getTimeRange() : null);
                List<SensorEvent> events = eventStore.query(preds);
                return pageSorted(events, page, e -> List.of(i64Key(e.getTimestampMs()), e.getEventId()));
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public void subscribeLiveSessions(Caller caller, WatchLiveSessionsRequest req,
                                          StreamObserver<WatchLiveSessionsEvent> obs) throws ProtoException {
            LiveWatcher w = new LiveWatcher(obs, req.getFilterEmail(), req.getIncludeRawEvents());
            liveWatchers.add(w);
            if (obs instanceof io.grpc.stub.ServerCallStreamObserver) {
                ((io.grpc.stub.ServerCallStreamObserver<?>) obs)
                        .setOnCancelHandler(() -> liveWatchers.remove(w));
            }
            // Never calls onCompleted() — stays open until client disconnects.
        }
    }

    // -----------------------------------------------------------------------
    // Session summary helpers
    // -----------------------------------------------------------------------

    private List<SessionSummary> buildSessionSummaries(List<SensorEvent> events) {
        long nowMs     = System.currentTimeMillis();
        int idleThresh = config != null ? config.getActiveSessionIdleSeconds() : 300;
        Map<String, SessionSummary.Builder> bySession = new LinkedHashMap<>();
        for (SensorEvent ev : events) {
            if (ev.getSessionId().isEmpty()) continue;
            bySession.compute(ev.getSessionId(), (k, b) -> {
                if (b == null) {
                    b = SessionSummary.newBuilder()
                            .setSessionId(ev.getSessionId())
                            .setEmail(ev.getEmail())
                            .setWorkingDirectory(ev.getWorkingDirectory())
                            .setSensorType(ev.getSensorType())
                            .setFirstSeenMs(ev.getTimestampMs())
                            .setLastSeenMs(ev.getTimestampMs())
                            .setLatestEventType(ev.getEventType());
                }
                // Events arrive DESC by timestamp; track both ends.
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
            boolean active = idleSecs <= idleThresh;
            result.add(b.setIsActive(active).setIdleSeconds(active ? 0 : idleSecs).build());
        }
        return result;
    }

    private static SessionSummary buildQuickSummary(SensorEvent ev, long nowMs, int idleThresh) {
        long idleSecs = (nowMs - ev.getTimestampMs()) / 1000;
        boolean active = idleSecs <= idleThresh;
        return SessionSummary.newBuilder()
                .setSessionId(ev.getSessionId())
                .setEmail(ev.getEmail())
                .setWorkingDirectory(ev.getWorkingDirectory())
                .setSensorType(ev.getSensorType())
                .setLastSeenMs(ev.getTimestampMs())
                .setLatestEventType(ev.getEventType())
                .setIsActive(active)
                .setIdleSeconds(active ? 0 : idleSecs)
                .build();
    }

    private static void applyTimeRange(EventStore.SqlPredicates preds, EventTimeRange range) {
        if (range == null || range == EventTimeRange.getDefaultInstance()) return;
        if (range.getStartMs() > 0) preds.startMs = range.getStartMs();
        if (range.getEndMs()   > 0) preds.endMs   = range.getEndMs();
    }

    // -----------------------------------------------------------------------
    // Pagination helpers
    // -----------------------------------------------------------------------

    private static String i64Key(long value) {
        return String.format("%020d", Math.max(0L, value));
    }

    private static int compareKeys(List<String> left, List<String> right) {
        int n = Math.min(left.size(), right.size());
        for (int i = 0; i < n; i++) {
            int cmp = left.get(i).compareTo(right.get(i));
            if (cmp != 0) return cmp;
        }
        return Integer.compare(left.size(), right.size());
    }

    private static <T> CollectionRuntime.Page<T> pageSorted(
            List<T> all,
            CollectionRuntime.PageParams page,
            Function<T, List<String>> keyFor) {
        all.sort((a, b) -> compareKeys(keyFor.apply(a), keyFor.apply(b)));
        int limit = (int) Math.max(1L, page.limit);
        int start = 0;
        if (page.after != null && page.after.keys != null) {
            while (start < all.size() && compareKeys(keyFor.apply(all.get(start)), page.after.keys) <= 0) {
                start++;
            }
        }
        int end = Math.min(start + limit, all.size());
        List<T> items = new ArrayList<>(all.subList(start, end));
        CollectionRuntime.Cursor nextAfter = end < all.size() && !items.isEmpty()
                ? new CollectionRuntime.Cursor(keyFor.apply(items.get(items.size() - 1)))
                : null;
        return new CollectionRuntime.Page<>(items, nextAfter, all.size());
    }
}
