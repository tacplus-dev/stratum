// Copyright (c) 2026 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;

import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.services.stratum.RecordMapGrpc;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.services.stratum.Stratum;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor;

/**
 * pull-agent-event — a caller for stratum's generated Pull/Refine/Count query
 * plane over the AgentEvent record type.
 *
 * AgentEvent is the record type this service's own vservice publishes: the
 * sensor path writes it through EventBus, stratum stores it under
 * {@code option (spec.record)}, and tacplus generates the RecordMap query plane
 * from that annotation. TaskActivity already had a caller
 * (testapp/cmd/pull-task-activity); AgentEvent is the SECOND record type through
 * the same generated surface, which is the point — the plane has to work for a
 * record type, not for one record type.
 *
 * It drives the GENERATED {@link RecordMapGrpc} stub and nothing else. There is
 * no hand-rolled request or wire framing here: the filter, the handle, the page
 * token and every RPC come from the generated Stratum types.
 *
 * The lifecycle the generated plane imposes, in order:
 *
 * <pre>
 *   OpenAgentEventQuery(filter) -&gt; QueryHandle   filter applied at open, pins a query
 *   RefineAgentEvent(handle, f) -&gt; QueryStats    replaces the predicate, returns new size
 *   CountAgentEvent(handle)     -&gt; RecordCount   server-side size
 *   PullAgentEvent(handle,page) -&gt; items + next_page_token
 *   CloseAgentEvent(handle)     -&gt; QueryStats    releases the working set
 * </pre>
 *
 * Usage:
 * <pre>
 *   pull-agent-event --target host:port [--token JWT] [--plaintext]
 *                    [--spiffe-id ID] [--session-id ID] [--agent-id ID]
 *                    [--from-ms N] [--to-ms N] [--page-size N]
 *                    [--refine-session-id ID] [--refine-agent-id ID]
 *                    [--refine-spiffe-id ID] [--refine-from-ms N] [--refine-to-ms N]
 *   pull-agent-event --print-filter-fields
 * </pre>
 *
 * Prints one JSON object on stdout; exits non-zero with a message on stderr if
 * any leg of the lifecycle fails.
 */
public final class PullAgentEvent {

    /** Page size used when the caller does not name one — the plane's own default. */
    static final int DEFAULT_PAGE_SIZE = 100;

    /**
     * Bound on the paging loop. A server that returned the same next_page_token
     * forever would hang a gate instead of failing it.
     */
    static final int MAX_PAGES = 1000;

    private PullAgentEvent() {}

    // -----------------------------------------------------------------------
    // Args
    // -----------------------------------------------------------------------

    static final class Args {
        String target = "";
        String token = System.getenv("STRATUM_TOKEN");
        boolean plaintext;
        boolean printFilterFields;
        int pageSize = DEFAULT_PAGE_SIZE;
        final Stratum.AgentEventFilter.Builder filter = Stratum.AgentEventFilter.newBuilder();
        Stratum.AgentEventFilter.Builder refine;

        Stratum.AgentEventFilter.Builder refine() {
            if (refine == null) {
                refine = Stratum.AgentEventFilter.newBuilder();
            }
            return refine;
        }
    }

    private static void usage() {
        System.err.println("usage: pull-agent-event --target host:port [--token JWT] [--plaintext]"
                + " [--spiffe-id ID] [--session-id ID] [--agent-id ID]"
                + " [--from-ms N] [--to-ms N] [--page-size N]"
                + " [--refine-spiffe-id ID] [--refine-session-id ID] [--refine-agent-id ID]"
                + " [--refine-from-ms N] [--refine-to-ms N]");
        System.err.println("       pull-agent-event --print-filter-fields");
        System.exit(2);
    }

    /** Every flag but the two booleans takes a value; a missing one is a usage error. */
    private static String value(String[] argv, int i) {
        if (i + 1 >= argv.length) {
            usage();
        }
        return argv[i + 1];
    }

    static Args parseArgs(String[] argv) {
        Args a = new Args();
        if (a.token != null && a.token.isEmpty()) {
            a.token = null;
        }
        for (int i = 0; i < argv.length; i++) {
            switch (argv[i]) {
                case "--target"            -> { a.target = value(argv, i); i++; }
                case "--token"             -> { a.token = value(argv, i); i++; }
                case "--plaintext"         -> a.plaintext = true;
                case "--print-filter-fields" -> a.printFilterFields = true;
                case "--page-size"         -> { a.pageSize = Integer.parseInt(value(argv, i)); i++; }
                case "--spiffe-id"         -> { a.filter.setSpiffeId(value(argv, i)); i++; }
                case "--session-id"        -> { a.filter.setSessionId(value(argv, i)); i++; }
                case "--agent-id"          -> { a.filter.setAgentId(value(argv, i)); i++; }
                case "--from-ms"           -> { a.filter.setTimestampMsFrom(Long.parseLong(value(argv, i))); i++; }
                case "--to-ms"             -> { a.filter.setTimestampMsTo(Long.parseLong(value(argv, i))); i++; }
                case "--refine-spiffe-id"  -> { a.refine().setSpiffeId(value(argv, i)); i++; }
                case "--refine-session-id" -> { a.refine().setSessionId(value(argv, i)); i++; }
                case "--refine-agent-id"   -> { a.refine().setAgentId(value(argv, i)); i++; }
                case "--refine-from-ms"    -> { a.refine().setTimestampMsFrom(Long.parseLong(value(argv, i))); i++; }
                case "--refine-to-ms"      -> { a.refine().setTimestampMsTo(Long.parseLong(value(argv, i))); i++; }
                default -> usage();
            }
        }
        if (!a.printFilterFields && a.target.isEmpty()) {
            usage();
        }
        return a;
    }

    // -----------------------------------------------------------------------
    // The generated filter surface
    // -----------------------------------------------------------------------

    /**
     * The field names the GENERATED filter surface exposes, read off the
     * generated descriptor rather than restated by hand.
     *
     * The filter surface is itself an egress channel: a caller who may not SEE a
     * field must also not be able to write a predicate on it and infer the value
     * from which rows come back. AgentEvent carries no {@code (spec.redact)}
     * field today, so nothing is excluded — but a redacted field added later
     * without care would show up here, and PullAgentEventE2ETest pins the list.
     *
     * trace_id and noop are the tacplus envelope pair (tags 200/201) present on
     * every generated message, not filter criteria, so they are dropped.
     */
    static List<String> filterFieldNames() {
        List<String> names = new ArrayList<>();
        for (Descriptors.FieldDescriptor f : Stratum.AgentEventFilter.getDescriptor().getFields()) {
            if (f.getNumber() >= 200) {
                continue;
            }
            names.add(f.getJsonName());
        }
        return names;
    }

    // -----------------------------------------------------------------------
    // The query lifecycle
    // -----------------------------------------------------------------------

    /** One full open → (refine) → count → pull → close pass. */
    static final class Result {
        String queryId;
        long count;
        long totalCount;
        int pages;
        /** size returned by RefineAgentEvent, or -1 when no refine was asked for. */
        long refinedSize = -1;
        long closedSize;
        final List<Agentsensor.AgentEvent> items = new ArrayList<>();
    }

    /**
     * Runs the lifecycle against an already-connected generated stub. Split out
     * from {@link #main} so a harness can drive the same code over its own
     * channel — the assertions must exercise the shipped path, not a copy of it.
     */
    static Result run(RecordMapGrpc.RecordMapBlockingStub stub,
                      Stratum.AgentEventFilter filter,
                      Stratum.AgentEventFilter refine,
                      int pageSize) {
        Result r = new Result();

        // standing:false — a one-shot snapshot query. A standing query keeps its
        // working set live for WatchAgentEvent, which this binary does not use.
        Stratum.QueryHandle handle = stub.openAgentEventQuery(
                Stratum.OpenAgentEventQueryRequest.newBuilder()
                        .setFilter(filter)
                        .setStanding(false)
                        .build());
        r.queryId = handle.getQueryId();

        try {
            if (refine != null) {
                r.refinedSize = stub.refineAgentEvent(
                        Stratum.RefineAgentEventRequest.newBuilder()
                                .setQueryId(handle.getQueryId())
                                .setFilter(refine)
                                .build()).getSize();
            }

            r.count = stub.countAgentEvent(handle).getCount();

            String pageToken = "";
            while (true) {
                Stratum.PullAgentEventResponse resp = stub.pullAgentEvent(
                        Stratum.PullAgentEventRequest.newBuilder()
                                .setQueryId(handle.getQueryId())
                                .setPageSize(pageSize)
                                .setPageToken(pageToken)
                                .build());
                r.pages++;
                r.totalCount = resp.getTotalCount();
                r.items.addAll(resp.getItemsList());
                if (resp.getNextPageToken().isEmpty()) {
                    break;
                }
                if (r.pages >= MAX_PAGES) {
                    throw new IllegalStateException("PullAgentEvent did not terminate: " + MAX_PAGES
                            + " pages and next_page_token is still set");
                }
                pageToken = resp.getNextPageToken();
            }
        } finally {
            // Close on the way out whatever happened above: a failed pull still
            // left a working set open in stratum.
            r.closedSize = stub.closeAgentEvent(handle).getSize();
        }
        return r;
    }

    // -----------------------------------------------------------------------
    // Transport
    // -----------------------------------------------------------------------

    /**
     * Opens a channel to {@code host:port}.
     *
     * TLS trusts any server certificate. A caller reaches a deployed stratum
     * through {@code kubectl port-forward}, so the connection terminates on
     * localhost and the pod's cert names neither localhost nor the per-replica
     * service name — the same posture pull-task-activity takes for TaskActivity,
     * and deliberately NOT the in-mesh posture (in the mesh the chassis verifies
     * the chain and the URI SAN). This binary authenticates with a bearer token,
     * not with the channel. {@code --plaintext} is for a local harness that
     * terminates no TLS at all.
     */
    static ManagedChannel channel(String target, boolean plaintext) throws Exception {
        NettyChannelBuilder b = NettyChannelBuilder.forTarget(target);
        if (plaintext) {
            return b.negotiationType(NegotiationType.PLAINTEXT).build();
        }
        return b.negotiationType(NegotiationType.TLS)
                .sslContext(GrpcSslContexts.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .build())
                .build();
    }

    /**
     * Attaches the bearer token. Sending an EMPTY authorization header is not the
     * same as sending none — it is a malformed credential — so an absent token
     * means no header at all.
     */
    static RecordMapGrpc.RecordMapBlockingStub stub(ManagedChannel ch, String token) {
        RecordMapGrpc.RecordMapBlockingStub s = RecordMapGrpc.newBlockingStub(ch);
        if (token == null || token.isEmpty()) {
            return s;
        }
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
        return s.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));
    }

    // -----------------------------------------------------------------------
    // Output
    // -----------------------------------------------------------------------

    /**
     * One JSON object. Rows are printed by the generated protobuf JSON printer;
     * AgentEvent carries no redacted field, so there is no "empty means redacted"
     * ambiguity to work around here (pull-task-activity had to spell TaskActivity's
     * rows out field by field for exactly that reason).
     */
    static String toJson(Result r, List<String> filterFields) throws Exception {
        JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
        StringBuilder sb = new StringBuilder();
        sb.append("{\"queryId\":\"").append(r.queryId).append('"')
          .append(",\"count\":").append(r.count)
          .append(",\"totalCount\":").append(r.totalCount)
          .append(",\"pages\":").append(r.pages)
          .append(",\"refinedSize\":").append(r.refinedSize)
          .append(",\"closedSize\":").append(r.closedSize)
          .append(",\"filterFields\":[");
        for (int i = 0; i < filterFields.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"').append(filterFields.get(i)).append('"');
        }
        sb.append("],\"items\":[");
        for (int i = 0; i < r.items.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(printer.print(r.items.get(i)));
        }
        return sb.append("]}").toString();
    }

    /**
     * Keeps stdout clean for the one JSON object this CLI prints.
     *
     * The service jar ships logback with no configuration file, and logback's
     * built-in default is DEBUG on the root logger to System.out — so netty's
     * connect-time chatter lands in the middle of the payload and nothing
     * downstream can parse it. Nothing else in the jar has this problem because
     * the service configures logging at startup and prints no data to stdout.
     */
    private static void quietLogsOnStdout() {
        org.slf4j.Logger root = org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        if (root instanceof ch.qos.logback.classic.Logger lb) {
            lb.setLevel(ch.qos.logback.classic.Level.WARN);
        }
    }

    public static void main(String[] args) {
        quietLogsOnStdout();
        Args a = parseArgs(args);
        if (a.printFilterFields) {
            System.out.println("{\"filterFields\":[\""
                    + String.join("\",\"", filterFieldNames()) + "\"]}");
            return;
        }
        ManagedChannel ch = null;
        try {
            ch = channel(a.target, a.plaintext);
            Result r = run(stub(ch, a.token), a.filter.build(),
                    a.refine == null ? null : a.refine.build(), a.pageSize);
            System.out.println(toJson(r, filterFieldNames()));
        } catch (Exception e) {
            System.err.println("pull-agent-event: " + e);
            System.exit(1);
        } finally {
            if (ch != null) {
                try {
                    ch.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
