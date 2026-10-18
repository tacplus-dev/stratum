// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.s2sm.ServiceManager;

/**
 * Entry point for the stratum service.
 * Supervisor LLM stateful SQL workspace — provides working-set maps,
 * a durable scratchpad, async job management, and a Calcite SQL engine.
 */
public class Main {

    public static void main(String[] args) {
        try {
            ServiceManager sm = ServiceManager.create(args);
            sm.run(new StratumHandler(sm));
        } catch (Throwable t) {
            // Exit, rather than let the throw leave main.
            //
            // Startup creates a Hazelcast member, and Hazelcast's threads are NOT
            // daemons. A throw out of main therefore kills the main thread and
            // LEAVES THE JVM RUNNING: no gRPC listener is ever bound, nothing logs
            // again, and the process sits there looking like a hang. Observed live —
            // a SplitBrainProtectionException from opening a hot map on a cluster
            // that had not assembled yet produced a pod that printed a stack trace,
            // kept its Hazelcast member in the cluster, and was killed by the health
            // sidecar five minutes later with no indication of why.
            //
            // A startup that cannot complete must fail fast and loudly so the
            // restart is immediate and the reason is the last thing in the log.
            t.printStackTrace();
            System.err.println("stratum: startup failed — exiting");
            System.exit(1);
        }
    }
}
