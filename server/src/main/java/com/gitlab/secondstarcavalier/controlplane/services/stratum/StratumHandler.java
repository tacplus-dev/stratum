// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.services.stratum;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.s2sm.ServiceManager;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.s2sm.internal.grpc.servers.vservices.claudesensor.StratumQuery;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.services.stratum.Stratum;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.services.stratum.Helpers;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.DeleteMapRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.DeleteMapResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.DescribeMapRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.DescribeMapResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.EventTimeRange;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.FilterMapRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.FilterMapResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.GetSessionEventsRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.GetSessionEventsResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.GetUserSessionsRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.GetUserSessionsResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.ListActiveSessionsRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.ListActiveSessionsResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.ListMapsRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.ListMapsResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.LoadByGraphdbRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.LoadByGraphdbResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.LoadByIdsRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.LoadByIdsResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.LoadBySqlRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.LoadBySqlResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.LoadByTimedbRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.LoadByTimedbResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.MapSizeRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.MapSizeResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.MergeRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.MergeResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.NewMapRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.NewMapResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.QueryByEmailRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.QueryByEmailResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.QueryByTimeRangeRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.QueryByTimeRangeResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.QueryMapRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.QueryMapResponse;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.SensorEvent;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.SessionSummary;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.WatchLiveSessionsEvent;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.claudesensor.Claudesensor.WatchLiveSessionsRequest;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.libs.servicemanager.Caller;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.libs.errors.ProtoException;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.libs.x509.X509;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.llmsensor.Llmsensor;
import com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.types.vservices.scmsensor.Scmsensor;

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
    public com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.s2sm.internal.grpc.servers.services.stratum.StratumStateService.StratumStateServiceHandler getStratumStateGrpcImpl() {
        return new com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.s2sm.internal.grpc.servers.services.stratum.StratumStateService.StratumStateServiceHandler() {};
    }

    @Override
    public com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.s2sm.internal.grpc.servers.services.stratum.StratumConfigStateService.StratumConfigStateServiceHandler getStratumConfigStateGrpcImpl() {
        return new com.gitlab.secondstarcavalier.controlplane.services.stratum.tacplus.s2sm.internal.grpc.servers.services.stratum.StratumConfigStateService.StratumConfigStateServiceHandler() {
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
    public void onEventStreamClaudesensor(byte[] eventBytes, SubscriptionMeta meta) throws Exception {
        SensorEvent event = SensorEvent.parseFrom(eventBytes);
        eventStore.put(event);
        pushToLiveWatchers(event);
    }

    @Override
    public void onEventStreamLlmsensor(byte[] eventBytes, SubscriptionMeta meta) throws Exception {
        Llmsensor.LlmEvent ev = Llmsensor.LlmEvent.parseFrom(eventBytes);
        eventStore.putLlmRaw(ev.getEventId(), eventBytes);
        log.debug("stored llm event: event_id={} email={}", ev.getEventId(), ev.getEmail());
    }

    @Override
    public void onEventStreamScmsensor(byte[] eventBytes, SubscriptionMeta meta) throws Exception {
        Scmsensor.ScmEvent ev = Scmsensor.ScmEvent.parseFrom(eventBytes);
        eventStore.putScmRaw(ev.getEventId(), eventBytes);
        log.debug("stored scm event: event_id={} email={}", ev.getEventId(), ev.getEmail());
    }

    /**
     * Fallback handler for any new vservice sensors added in the future.
     * The generated Handler.java routes unhandled topics here via default methods,
     * so new sensors are persisted automatically until a typed override is added.
     */
    @Override
    public void handleEventStreamMessage(String topic, byte[] eventBytes, SubscriptionMeta meta) throws Exception {
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
        public NewMapResponse newMap(Caller caller, NewMapRequest req) throws ProtoException {
            try {
                store.createMap(req.getMapId());
                return NewMapResponse.getDefaultInstance();
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
        public DescribeMapResponse describeMap(Caller caller, DescribeMapRequest req) throws ProtoException {
            try {
                return DescribeMapResponse.newBuilder()
                        .setDetail(store.describeMap(req.getMapId())).build();
            } catch (IllegalArgumentException e) {
                throw new ProtoException(ProtoException.INVALID_ARGUMENT, e.getMessage());
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public MapSizeResponse mapSize(Caller caller, MapSizeRequest req) throws ProtoException {
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
        public LoadByIdsResponse loadByIds(Caller caller, LoadByIdsRequest req) throws ProtoException {
            try {
                long[] added = {0};
                for (String id : req.getEventIdsList()) {
                    eventStore.getByKey(id).ifPresent(ev -> {
                        store.putEvent(req.getMapId(), ev);
                        added[0]++;
                    });
                }
                return LoadByIdsResponse.newBuilder().setEventsAdded(added[0]).build();
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public LoadBySqlResponse loadBySql(Caller caller, LoadBySqlRequest req) throws ProtoException {
            try {
                EventStore.SqlPredicates preds = EventStore.SqlParser.parse(req.getSql());
                List<SensorEvent> events = eventStore.query(preds);
                for (SensorEvent ev : events) {
                    store.putEvent(req.getMapId(), ev);
                }
                log.info("load_by_sql into {}: {} events loaded", req.getMapId(), events.size());
                return LoadBySqlResponse.newBuilder().setEventsAdded(events.size()).build();
            } catch (IllegalArgumentException e) {
                throw new ProtoException(ProtoException.INVALID_ARGUMENT, e.getMessage());
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public LoadByTimedbResponse loadByTimedb(Caller caller, LoadByTimedbRequest req) throws ProtoException {
            throw new ProtoException(ProtoException.UNIMPLEMENTED, "load_by_timedb not yet implemented");
        }

        @Override
        public LoadByGraphdbResponse loadByGraphdb(Caller caller, LoadByGraphdbRequest req) throws ProtoException {
            throw new ProtoException(ProtoException.UNIMPLEMENTED, "load_by_graphdb not yet implemented");
        }

        // --- Map mutation ---

        @Override
        public FilterMapResponse filterMap(Caller caller, FilterMapRequest req) throws ProtoException {
            try {
                long removed = store.filterMap(req.getMapId(), req.getSql());
                return FilterMapResponse.newBuilder().setEventsRemoved(removed).build();
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
        public QueryMapResponse queryMap(Caller caller, QueryMapRequest req) throws ProtoException {
            try {
                int limit = req.getLimit() > 0 ? req.getLimit() : 100;
                Object[] result = (Object[]) store.queryMap(req.getMapId(), req.getSql(), limit);
                return QueryMapResponse.newBuilder()
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
        public ListActiveSessionsResponse listActiveSessions(Caller caller,
                                                             ListActiveSessionsRequest req) throws ProtoException {
            try {
                int idleSecs  = config != null ? config.getActiveSessionIdleSeconds() : 300;
                long cutoffMs = System.currentTimeMillis() - (long) idleSecs * 1000;
                EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
                preds.startMs = cutoffMs;
                List<SensorEvent> events = eventStore.query(preds);
                List<SessionSummary> sessions = buildSessionSummaries(events);
                // Sort by last_seen_ms DESC for stable pagination.
                sessions.sort(java.util.Comparator.comparingLong(SessionSummary::getLastSeenMs).reversed()
                        .thenComparing(SessionSummary::getSessionId));

                int pageSize = req.getPageSize() > 0 ? req.getPageSize() : 50;
                int offset   = decodePageOffset(req.getPageToken());
                List<SessionSummary> page = new ArrayList<>();
                String nextToken = pageSlice(sessions, offset, pageSize, page);
                return ListActiveSessionsResponse.newBuilder()
                        .addAllSessions(page)
                        .setNextPageToken(nextToken)
                        .build();
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public GetSessionEventsResponse getSessionEvents(Caller caller,
                                                         GetSessionEventsRequest req) throws ProtoException {
            try {
                EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
                preds.sessionId = req.getSessionId();
                applyTimeRange(preds, req.hasTimeRange() ? req.getTimeRange() : null);
                List<SensorEvent> events = eventStore.query(preds);
                // Chronological order for session replay.
                events.sort(java.util.Comparator.comparingLong(SensorEvent::getTimestampMs));

                int pageSize = req.getPageSize() > 0 ? req.getPageSize() : 200;
                int offset   = decodePageOffset(req.getPageToken());
                List<SensorEvent> page = new ArrayList<>();
                String nextToken = pageSlice(events, offset, pageSize, page);
                return GetSessionEventsResponse.newBuilder()
                        .addAllEvents(page)
                        .setNextPageToken(nextToken)
                        .build();
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public GetUserSessionsResponse getUserSessions(Caller caller,
                                                       GetUserSessionsRequest req) throws ProtoException {
            try {
                EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
                preds.email = req.getEmail();
                applyTimeRange(preds, req.hasTimeRange() ? req.getTimeRange() : null);
                List<SensorEvent> events = eventStore.query(preds);
                List<SessionSummary> sessions = buildSessionSummaries(events);
                sessions.sort(java.util.Comparator.comparingLong(SessionSummary::getLastSeenMs).reversed()
                        .thenComparing(SessionSummary::getSessionId));

                int pageSize = req.getPageSize() > 0 ? req.getPageSize() : 50;
                int offset   = decodePageOffset(req.getPageToken());
                List<SessionSummary> page = new ArrayList<>();
                String nextToken = pageSlice(sessions, offset, pageSize, page);
                return GetUserSessionsResponse.newBuilder()
                        .addAllSessions(page)
                        .setNextPageToken(nextToken)
                        .build();
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public QueryByTimeRangeResponse queryByTimeRange(Caller caller,
                                                         QueryByTimeRangeRequest req) throws ProtoException {
            EventTimeRange tr = req.getTimeRange();
            if (tr.getStartMs() <= 0 || tr.getEndMs() <= 0 || tr.getStartMs() >= tr.getEndMs()) {
                throw new ProtoException(ProtoException.INVALID_ARGUMENT,
                        "time_range requires start_ms < end_ms, both > 0");
            }
            try {
                EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
                applyTimeRange(preds, tr);
                List<SensorEvent> events = eventStore.query(preds);
                events.sort(java.util.Comparator.comparingLong(SensorEvent::getTimestampMs));

                int pageSize = req.getPageSize() > 0 ? req.getPageSize() : 200;
                int offset   = decodePageOffset(req.getPageToken());
                List<SensorEvent> page = new ArrayList<>();
                String nextToken = pageSlice(events, offset, pageSize, page);
                return QueryByTimeRangeResponse.newBuilder()
                        .addAllEvents(page)
                        .setNextPageToken(nextToken)
                        .build();
            } catch (ProtoException pe) {
                throw pe;
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public QueryByEmailResponse queryByEmail(Caller caller,
                                                 QueryByEmailRequest req) throws ProtoException {
            try {
                EventStore.SqlPredicates preds = new EventStore.SqlPredicates();
                preds.email = req.getEmail();
                applyTimeRange(preds, req.hasTimeRange() ? req.getTimeRange() : null);
                List<SensorEvent> events = eventStore.query(preds);
                events.sort(java.util.Comparator.comparingLong(SensorEvent::getTimestampMs));

                int pageSize = req.getPageSize() > 0 ? req.getPageSize() : 200;
                int offset   = decodePageOffset(req.getPageToken());
                List<SensorEvent> page = new ArrayList<>();
                String nextToken = pageSlice(events, offset, pageSize, page);
                return QueryByEmailResponse.newBuilder()
                        .addAllEvents(page)
                        .setNextPageToken(nextToken)
                        .build();
            } catch (Exception e) {
                throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
            }
        }

        @Override
        public void watchLiveSessions(Caller caller, WatchLiveSessionsRequest req,
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

    private static int decodePageOffset(String token) {
        if (token == null || token.isEmpty()) return 0;
        try { return Integer.parseInt(new String(Base64.getDecoder().decode(token))); }
        catch (Exception e) { return 0; }
    }

    private static String encodePageToken(int offset) {
        return Base64.getEncoder().encodeToString(Integer.toString(offset).getBytes());
    }

    private static <T> String pageSlice(List<T> all, int offset, int size, List<T> out) {
        int from = Math.min(offset, all.size());
        int to   = Math.min(from + size, all.size());
        out.addAll(all.subList(from, to));
        return to < all.size() ? encodePageToken(to) : "";
    }
}
