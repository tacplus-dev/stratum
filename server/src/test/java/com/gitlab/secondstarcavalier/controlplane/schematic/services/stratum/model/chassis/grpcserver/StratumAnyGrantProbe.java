// Copyright (c) 2026 Second Star Cavalier LLC
//
// D3 diagnostic — NOT a surefire test (no "Test" suffix, main-method only).
// Run it by hand:
//
//   mvn -o -q -pl model dependency:build-classpath -Dmdep.outputFile=/tmp/cp.txt
//   javac -cp "model/target/classes:$(cat /tmp/cp.txt)" -d /tmp/out \
//     server/src/test/java/.../grpcserver/StratumAnyGrantProbe.java
//   java -cp "/tmp/out:model/target/classes:$(cat /tmp/cp.txt)" \
//     com...stratum.model.chassis.grpcserver.StratumAnyGrantProbe
//
// It drives the REAL generated Authorizer against the REAL generated Grants
// catalog, with a paired control (entitled vs unentitled callers) in one run.
// It lives in the chassis package because Authorizer.SERVICE_NAME and
// Authorizer.methodGrantToken are package-private.
//
// It covers BOTH halves of the record query plane:
//
//   /taskquery.RecordQuery/CreateTaskActivityQuery   c2s -- the user-facing half
//   /taskquery.RecordMap/CreateTaskActivityQuery     s2s -- the peer half
//
// Before the c2s half existed (tacplus@main, this file's first commit), the
// only surface was RecordMap and NO grant string authorized it. Not the
// wildcard STRATUM_ANY, and not even the exact token the Authorizer itself
// computed -- because tacplus builds the grant catalog from auth_model=="c2s"
// RPCs only (registry collectEntitlementGrants) while the Authorizer's C2S path
// enforces grants on EVERY method a user token reaches. stratum was s2s-only,
// so its whole surface sat outside the grant vocabulary and Grants.normalize
// dropped every token as unknown.
//
// What it shows now: TASKQUERY_ANY (and the exact c2s token) ALLOW the c2s
// method, unentitled callers are still DENIED, and the s2s method stays denied
// to user tokens on every input -- which is the point of keeping the surfaces
// split rather than making RecordMap itself c2s.
//
// The plane moved off stratum's own proto and onto the `query` trait
// (testapp/proto/vservices/query.proto, package taskquery), so the method
// paths and the grant vocabulary are keyed on the CONTRACT, not on the host:
// STRATUM_ANY no longer expands to any of these RPCs, and the wildcard that
// does is TASKQUERY_ANY. That is the shape the trait exists to produce --
// blast radius belongs to the contract, not to whoever implements it today.
//
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.grpcserver;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.key.ImmutableKey;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.key.KeyHelpers;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.token.TokenWrapper;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.grants.Grants;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.generated.entitlements.Entitlements;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.sys.jwk.JwkOuterClass;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.sys.key.KeyOuterClass;

import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Drives the REAL generated stratum Authorizer over the REAL generated Grants
 * catalog for the exact method the D3 report is about. No fakes: real Ed25519
 * key, real signed JWT, real verify + entitlement path.
 */
public class StratumAnyGrantProbe {

    private static final String C2S_METHOD = "/taskquery.RecordQuery/CreateTaskActivityQuery";
    private static final String S2S_METHOD = "/taskquery.RecordMap/CreateTaskActivityQuery";

    private static ImmutableKey newTestKey() throws Exception {
        return ImmutableKey.from(KeyHelpers.newEd25519Key());
    }

    private static JwkOuterClass.Jwk jwkForKey(ImmutableKey key) {
        byte[] spki = key.getPublicKey().getEncoded();
        byte[] raw = new byte[32];
        System.arraycopy(spki, spki.length - 32, raw, 0, 32);
        return JwkOuterClass.Jwk.newBuilder()
                .setKid(key.getProto().getId())
                .setKty(KeyOuterClass.KeyType.KEY_TYPE_OKP)
                .setCrv("Ed25519")
                .setX(Base64.getUrlEncoder().withoutPadding().encodeToString(raw))
                .build();
    }

    private static String userToken(ImmutableKey key, List<String> grants) throws Exception {
        Entitlements.TokenEntitlements te = Entitlements.TokenEntitlements.newBuilder()
                .addAllGrants(grants)
                .build();
        TokenWrapper tok = TokenWrapper.create()
                .setIssuer("test-issuer")
                .setAudience(Authorizer.SERVICE_NAME)
                .setSubject("alice@example.com")
                .setExpiration(System.currentTimeMillis() / 1000 + 3600);
        tok.setSubType("user");
        tok.setServiceEntitlements(te);
        tok.sign(key);
        return "Bearer " + tok.getBase64();
    }

    private static void attempt(Authorizer a, String method, String label, String bearer) {
        try {
            a.authorizeWithToken(bearer, method, null);
            System.out.println("ALLOWED  " + label);
        } catch (Exception e) {
            System.out.println("DENIED   " + label + "  ->  " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("c2s method under test: " + C2S_METHOD);
        System.out.println("  required exact grant (computed by the generated Authorizer): "
                + Authorizer.methodGrantToken(Authorizer.SERVICE_NAME, C2S_METHOD));
        System.out.println("s2s control method:    " + S2S_METHOD);
        System.out.println("  required exact grant: "
                + Authorizer.methodGrantToken(Authorizer.SERVICE_NAME, S2S_METHOD));
        System.out.println("Grants.lookup(TASKQUERY_ANY) present: " + Grants.lookup("TASKQUERY_ANY").isPresent());
        System.out.println("Grants.expand(TASKQUERY_ANY) = " + Grants.expand("TASKQUERY_ANY"));
        System.out.println("Grants.normalize([TASKQUERY_ANY]) = " + Grants.normalize(List.of("TASKQUERY_ANY")));
        System.out.println("Grants.unknownTokens([TASKQUERY_ANY]) = " + Grants.unknownTokens(List.of("TASKQUERY_ANY")));
        System.out.println("catalog size = " + Grants.tokens().size());
        System.out.println();

        ImmutableKey key = newTestKey();
        Map<String, JwkOuterClass.Jwk> keys = Map.of(key.getProto().getId(), jwkForKey(key));
        Authorizer a = new Authorizer(sink -> sink.update(keys));
        // Same policy the generated RecordMap wrapper registers at startup. The
        // generated RecordQuery wrapper registers none: it is c2s, so the
        // Authorizer takes the entitlement path.
        a.setMethodAuth("CreateTaskActivityQuery", List.of("argus"));

        System.out.println("--- c2s surface (taskquery.RecordQuery) ---");
        attempt(a, C2S_METHOD, "user token, grants=[TASKQUERY_ANY]  (the trait wildcard)",
                userToken(key, List.of("TASKQUERY_ANY")));
        attempt(a, C2S_METHOD, "user token, grants=[TASKQUERY_WRITE_ANY]  (capability wildcard)",
                userToken(key, List.of("TASKQUERY_WRITE_ANY")));
        attempt(a, C2S_METHOD, "user token, grants=[TASKQUERY_RECORD_QUERY_CREATE_TASK_ACTIVITY_QUERY] (exact)",
                userToken(key, List.of("TASKQUERY_RECORD_QUERY_CREATE_TASK_ACTIVITY_QUERY")));
        attempt(a, C2S_METHOD, "user token, grants=[TASKQUERY_READ_ANY]  (wrong capability control)",
                userToken(key, List.of("TASKQUERY_READ_ANY")));
        attempt(a, C2S_METHOD, "user token, grants=[STRATUM_ANY]  (host wildcard — no longer reaches the trait)",
                userToken(key, List.of("STRATUM_ANY")));
        attempt(a, C2S_METHOD, "user token, grants=[TASKS_ANY]  (unentitled control)",
                userToken(key, List.of("TASKS_ANY")));
        attempt(a, C2S_METHOD, "user token, grants=[]  (unentitled control)",
                userToken(key, List.of()));

        System.out.println();
        System.out.println("--- s2s surface (taskquery.RecordMap), unchanged ---");
        attempt(a, S2S_METHOD, "user token, grants=[TASKQUERY_ANY]",
                userToken(key, List.of("TASKQUERY_ANY")));
        attempt(a, S2S_METHOD, "user token, grants=[]",
                userToken(key, List.of()));
    }
}
