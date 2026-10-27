# Provenance

Copied **wholesale** on 2026-10-23 from `gitlab.com/seabeam/schematic/platform/services/stratum` at commit
`399b453` (local branch `stratum-archive`; its own nested git repository; history carried over), with
`proto/services/stratum.proto` and `proto/dependencies.yaml` from `platform/proto` at `ca7c322` on branch `big8-p1-messaging`.
Build outputs not copied. The record query plane trait (`proto/vservices/query.proto`, name `query`, id 71, host stratum) moved here from sonar/backend: it is the platform's record store's contract, not a product's.

Jody, 2026-10-23: *"We will copy kernel over to its github home and only make changes to it. we will
keep these as ships in the night for now. For the new stuff we want to do it the right way."* The private
copy runs the launch fleet unchanged; new-design changes land here. Stratum stays private.

Divergences: headers in tacplus.yaml (Jody Scott, Apache-2.0; LICENSE added); `src: proto` (this repo
holds the service's own proto); `proto/registry.yaml` is a lock of github.com/tacplus-dev/registry.

Ported through `864f70f247072405f78d703695dca06bcc2c220f` (the merge-base with SeaBeam `main`; `399b453` above is on SeaBeam's unpushed local branch `stratum-archive`, so the port scan cannot use it. Commits after the mark that the archive copy already carries are marked "already covered" on their port issue.)
