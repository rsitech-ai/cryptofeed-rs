# Read-only shared-feed bridge qualification

The 550-input source in `source.json` passes all eight local gates, including
1,580 workspace tests, Rust 1.85 bootstrap/public checks and the existing daemon
command. Three binary unit tests and four actual native bridge tests cover the
bounded protocol, exact groups, ACK fencing, paired resume and growing prefixes.

Rust native fixtures cover spot and perps. The exported perps fixture passes the
real Quants client through all 78 groups and 79 paired resume boundaries;
exceptions and async callback returns cannot publish an ACK. The actual observer
passes sealed 78-group/15-item and five-group stops. Quants has 37 combined tests;
its changes remain local to the isolated owner checkout.

`validation.json` binds the source, actual retained reader/native test artifacts,
commands, actual exits, raw-log hashes, independent reviews and preserved parser,
callback and legacy CLI failures. Local raw logs and retained executables are
ignored task evidence. The structural reader does not embed/attest the whole
native build; the separate source-test executable records its declared recipe.

The preceding SHA-256 performance source has separate all-17 hosted success in
`../sha256-debug-replay-20261020/hosted.json`. That result cannot qualify this new
reader revision. Own hosted reader checks, live causal transport, common consumer
pins, persisted feature-state/cursor binding, availability/admission and extended
Razer runs remain separate gates. No economics, orders, activation, deployment,
merge or release is granted.

Resume-check scope: Each of all 79 paired cursor boundaries reopens and checks exactly one next group (or final sealed idle); this is not a full remaining-suffix replay.

Publication constraint: GitHub rejected the initial push because the credential
lacks workflow-edit scope. The proposed extra hosted default-CLI step remains
in the preserved local patch; publication retains the parent workflow. All 550
compiled inputs and eight local check results are unchanged, including actual
default-CLI exit zero. The qualification and publication workflow hashes are
recorded separately; no extra hosted CLI regression step is claimed.
