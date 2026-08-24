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
// catalog for /stratum.RecordMap/OpenTaskActivityQuery, with a paired control
// (entitled vs unentitled callers) in one run. It lives in the chassis package
// because Authorizer.SERVICE_NAME and Authorizer.methodGrantToken are
// package-private.
//
// What it shows today: NO grant string authorizes this RPC. Not the wildcard
// STRATUM_ANY, and not even the exact token the Authorizer itself computes
// (STRATUM_RECORD_MAP_OPEN_TASK_ACTIVITY_QUERY) -- because tacplus builds the
// grant catalog from auth_model=="c2s" RPCs only (registry.go
// collectEntitlementGrants) while the Authorizer's C2S path enforces grants on
// EVERY method a user token reaches. stratum is s2s-only, so its whole surface
// is outside the grant vocabulary and Grants.normalize drops every token as
// unknown. That is why every principal's stratum token carries grants:[].
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

    private static final String FULL_METHOD = "/stratum.RecordMap/OpenTaskActivityQuery";

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

    private static void attempt(Authorizer a, String label, String bearer) {
        try {
            a.authorizeWithToken(bearer, FULL_METHOD, null);
            System.out.println("ALLOWED  " + label);
        } catch (Exception e) {
            System.out.println("DENIED   " + label + "  ->  " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("method under test: " + FULL_METHOD);
        System.out.println("required exact grant (computed by the generated Authorizer): "
                + Authorizer.methodGrantToken(Authorizer.SERVICE_NAME, FULL_METHOD));
        System.out.println("Grants.lookup(STRATUM_ANY) present: " + Grants.lookup("STRATUM_ANY").isPresent());
        System.out.println("Grants.expand(STRATUM_ANY) = " + Grants.expand("STRATUM_ANY"));
        System.out.println("Grants.normalize([STRATUM_ANY]) = " + Grants.normalize(List.of("STRATUM_ANY")));
        System.out.println("Grants.unknownTokens([STRATUM_ANY]) = " + Grants.unknownTokens(List.of("STRATUM_ANY")));
        System.out.println("catalog size = " + Grants.tokens().size());
        System.out.println();

        ImmutableKey key = newTestKey();
        Map<String, JwkOuterClass.Jwk> keys = Map.of(key.getProto().getId(), jwkForKey(key));
        Authorizer a = new Authorizer(sink -> sink.update(keys));
        // Same policy the generated RecordMap wrapper registers at startup.
        a.setMethodAuth("OpenTaskActivityQuery", List.of("argus"));

        attempt(a, "user token, grants=[STRATUM_ANY]  (what OPA mints today)",
                userToken(key, List.of("STRATUM_ANY")));
        attempt(a, "user token, grants=[STRATUM_RECORD_MAP_OPEN_TASK_ACTIVITY_QUERY] (exact)",
                userToken(key, List.of("STRATUM_RECORD_MAP_OPEN_TASK_ACTIVITY_QUERY")));
        attempt(a, "user token, grants=[TASKS_ANY]  (unentitled control)",
                userToken(key, List.of("TASKS_ANY")));
        attempt(a, "user token, grants=[]  (unentitled control)",
                userToken(key, List.of()));
    }
}
