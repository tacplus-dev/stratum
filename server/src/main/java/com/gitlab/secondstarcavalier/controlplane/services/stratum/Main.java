// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.services.stratum;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.tacplus.s2sm.ServiceManager;

/**
 * Entry point for the stratum service.
 * Supervisor LLM stateful SQL workspace — provides working-set maps,
 * a durable scratchpad, async job management, and a Calcite SQL engine.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        ServiceManager sm = ServiceManager.create(args);
        sm.run(new StratumHandler(sm));
    }
}
