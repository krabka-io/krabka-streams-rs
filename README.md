# krabka-streams-rs

Kafka Streams for Rust: a streams client with the DSL, state stores and
interactive queries, plus the schema-registry-aware serialisation it reads and
writes records through.

The Rust member of [krabka](https://github.com/krabka-io)'s streams family,
alongside [`krabka-streams-go`](https://github.com/krabka-io/krabka-streams-go)
and [`krabka-streams-java`](https://github.com/krabka-io/krabka-streams-java).

It builds on [`krabka-protocol`](https://github.com/krabka-io/krabka-protocol)
and [`krabka-client-rs`](https://github.com/krabka-io/krabka-client-rs), and its
integration suites boot a broker from
[`krabka-broker`](https://github.com/krabka-io/krabka-broker).

## Crates

| Crate | What it is |
| --- | --- |
| `krabka-client-streams` | The streams client: DSL, topology, state stores, interactive queries |
| `krabka-schema-serde` | Avro, JSON Schema and Protobuf serialisation against a schema registry |

## Build

```bash
cargo test --workspace
```

```bash
bazel test //...
```

Both are supported and both are gated in CI.

## Fixtures

Much of the suite compares against captured JVM behaviour: topology JSON, wire
bytes, and the fixtures under `crates/client-streams/tests/testdata`. Those are
found through a helper rather than a bare relative path, because the two build
systems run a test from different working directories — Cargo from the crate,
Bazel from the workspace root — and a path that assumes one silently finds
nothing under the other.

## Publishing

These crates are not published from here. `robot-head/crabka` still owns every
`krabka-*` name on crates.io; this repository is where the streams client is
developed.
