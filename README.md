# stratum

Stratum is the platform's record store: one per tenant, it holds that tenant's tacplus-declared records
(`option (spec.record)`) in memory, serves the generated record plane over them (writes, Get, queries,
Watch), and makes every write durable through the redo service before applying it. See [design.md](design.md),
[RECORD_EVENT_ARCHITECTURE.md](RECORD_EVENT_ARCHITECTURE.md) and [docs/DURABILITY.md](docs/DURABILITY.md).

## Build

```
just generate                          # tacplus build: generated model under model/target/generated-sources
mvn -B -Dtacplus.skip=true install     # model, codegen (stratum-codegen Maven plugin), server and their tests
```

`tacplus.yaml` declares the compiler floor (`tacplus:`), the Java package (`com.github.tacplusdev.stratum`) and
the proto sources this build depends on, all fetched from github.com/tacplus-dev.

Where this code came from: [PROVENANCE.md](PROVENANCE.md).
