# Babel HyParView with Discovery

Partial-view membership protocol for [Babel](https://github.com/) applications,
with bootstrap discovery via the `DiscoverableProtocol` interface.
Provided and evolved independently of the original work.

**Group ID:** `pt.paradigmshift.babel`
**Artifact ID:** `hyparview`
**Current version:** `0.1.0`
**Tested with:** `pt.unl.fct.di.novasys.babel:babel-sc-core` (Babel-Swarm core) and
`babel-protocol-commons-j21` (shared dissemination / membership API surface).
**Source / target:** Java 17.

---

## What it does

HyParView maintains two views on each node:

- A small **active view** (default capacity 4) which is the gossip overlay —
  every peer in the active view holds an open TCP connection back to this
  node.
- A larger **passive view** (default capacity 7) of known peers that are
  candidates for promotion into the active view when an active peer leaves.

The two views together give:

- **Bounded degree.** Each node holds exactly the active-view-size open
  connections regardless of how big the network gets — perfect for
  thousands-of-nodes meshes built on commodity hardware.
- **Self-healing.** Random walks (Join, ForwardJoin) propagate fresh joins
  through the overlay; periodic shuffles (every `ShufflePeriod` ms) refresh
  the passive view so every node has a current sample of other peers it can
  promote when its active view drops below capacity.
- **No coordinator.** Bootstrap is via a single contact (a known peer's
  address) or — in this "with discovery" variant — via the
  `DiscoverableProtocol` callback that lets Babel-Swarm tell the protocol
  about discovered peers.

For the algorithmic detail, see Leitão et al.,
*HyParView: a membership protocol for reliable gossip-based broadcast*,
DSN 2007.

## Protocol & event identifiers

This module follows the Babel ID convention used across the ParadigmShift
workspace: protocol IDs at 100-multiples; events numbered per handler class
from `protocol_id + 1` upward, four independent pools.

`HyParView` claims **protocol slot 2500**.

| Type | Handler class | ID | Purpose |
|---|---|---|---|
| `JoinMessage`              | message | `2501` | New node introduces itself to a contact |
| `JoinReplyMessage`         | message | `2502` | Active-view-additions confirmation back to the joiner |
| `ForwardJoinMessage`       | message | `2503` | Random walk that propagates a new join through the overlay |
| `DisconnectMessage`        | message | `2504` | Notifies a peer that we have dropped it from active view |
| `NeighborRequestMessage`   | message | `2505` | Ask a passive peer to promote itself into our active view |
| `NeighborReplyMessage`     | message | `2506` | Accept / refuse a NeighborRequest |
| `ShuffleMessage`           | message | `2507` | Random walk that periodically refreshes the passive view |
| `ShuffleReplyMessage`      | message | `2508` | Reciprocal sample sent back to the shuffle's origin |
| `ShuffleTimer`             | timer   | `2501` | Periodic — fires one shuffle round |
| `CheckConnectivityTimeout` | timer   | `2502` | One-shot — re-check active view fill after a recent down event |

The upstream chose protocol slot 1400, which collides with
`DataProcessingProtocol` in `stoneflux-edgegateway`. This fork moves to 2500
to fit cleanly alongside the workspace's existing protocols.

## Configuration

| Property | Default | Description |
|---|---|---|
| `HyParView.ActiveView`           | `4`     | Active view capacity (overlay degree). |
| `HyParView.PassiveView`          | `7`     | Passive view capacity. |
| `HyParView.ARWL`                 | `4`     | Active random-walk length (TTL of the Join walk). |
| `HyParView.PRWL`                 | `2`     | Passive random-walk length (TTL after which a forward-joined peer is added to the passive view of the intermediate). |
| `HyParView.ShufflePeriod`        | `2000` ms | Shuffle interval. |
| `HyParView.CheckConnectivityPeriod` | `1000` ms | Initial backoff for active-view fill checks. Doubles on each failure up to 60 s. |
| `HyParView.kActive`              | `2`     | Active-view sample size to include in a Shuffle. |
| `HyParView.kPassive`             | `3`     | Passive-view sample size to include in a Shuffle. |
| `HyParView.contact`              | unset / `none` | Optional bootstrap contact `host:port`. If `none` or unset, the node is its own bootstrap and waits for incoming Joins. |
| `HyParView.Channel.Address`      | from `myself` | TCP bind address. |
| `HyParView.Channel.Port`         | from `myself` | TCP bind port. |

## How application protocols plug in

Subscribe to `NeighborUp` / `NeighborDown` to track active-view membership;
issue `GetNeighborsSampleRequest` to pull a random sample of current peers.
The protocol fires both notifications when the active view changes.

For dissemination, pair with `eager-gossip-broadcast`; for
reconciliation, pair with `broadcast-antientropy`.

## Cert-based peer authentication

HyParView itself is channel-agnostic — it inherits whatever authentication
the underlying Babel channel performs. To require X.509 mutual auth between
gateways (the StoneFlux trust model), pass `AuthChannel.NAME` as the
constructor's `channelName` argument and configure the channel's
KeyManager / TrustManager via the `HyParView.Channel.*` property pass-through:

```properties
# Channel selection happens at the call site:
#   new HyParView(AuthChannel.NAME, props, myself)

# Listen on the gateway's own address (defaults from `myself` if unset)
HyParView.Channel.address = 10.0.0.42
HyParView.Channel.port    = 7444

# Keystore + truststore — passed through to AuthChannel
HyParView.Channel.keystore_path     = /etc/stoneflux-gateway/identity.p12
HyParView.Channel.keystore_password = ${IDENTITY_KEYSTORE_PASSWORD}
HyParView.Channel.truststore_path   = /etc/stoneflux-gateway/ca-chain.pem
```

Any property whose key starts with `HyParView.Channel.` is forwarded to the
channel with the prefix stripped — so any future channel option works
without changes to HyParView itself.

**Organisation isolation (`OU=client:<UUID>` check).** The same trust
boundary the rest of the StoneFlux gateway enforces — refusing peers whose
gateway cert's `OU=client:<UUID>` doesn't match this gateway's own
organisation UUID — is implemented as a *custom* `X509TrustManager` that
the gateway code installs into its `AuthChannel` configuration. This keeps
the policy where it belongs (in the gateway, where the local organisation
UUID lives) and keeps HyParView's source code completely free of
StoneFlux-specific identity logic. The TrustManager rejects mismatched-OU
peers at the TLS handshake, so HyParView never sees them — same effect as
inlining the check inside the protocol, but with no special-case code path
through the membership state machine.

## Build

```bash
mvn clean install
```

Depends on `babel-sc-core` and `babel-protocol-commons-j21` from the NOVA SYS
Maven repository (`https://novasys.di.fct.unl.pt/packages/mvn`); the
repository is listed in `pom.xml`.

## Tuning notes

- The default active-view size of 4 gives a connected graph with high
  probability for meshes from a few dozen to a few thousand nodes; do not
  shrink below 3 for production.
- `ShufflePeriod` is a passive-view freshness ↔ chatter trade-off. For
  factory-scale deployments (tens of gateways) the default 2 s is fine;
  for larger meshes (hundreds) increase to ~10–30 s.
- `CheckConnectivityPeriod` is doubled on each failed attempt up to a 60 s
  cap, so a node that is briefly isolated retries less and less aggressively.

## Differences from the upstream

This is a ParadigmShift evolution of the original protocol. Headline changes:

- Maven coordinates moved to
  `pt.paradigmshift.babel:hyparview`. Java package
  moved to `pt.paradigmshift.babel.hyparview`.
- **Protocol ID bumped from 1400 to 2500** to avoid a collision with
  `DataProcessingProtocol` (1400) in `stoneflux-edgegateway`. Message IDs
  bumped from `1401–1408` to `2501–2508`; timer IDs from `401/402` to
  `2501/2502`.
- `uponRNeeighborRequest` (sic) renamed to `uponReceiveNeighborRequest`.
- **Real bug fix in `uponReceiveShuffleReply`:** the upstream mutated the
  message's internal `sample` list directly while iterating it during
  passive-view reconciliation, so the same message replayed (or logged)
  twice would see different contents. This fork takes a defensive copy.
- **Real bug fix in `uponCheckConnectivityTimer`:**
  `(short) (Math.min(timeout * 2, MAX_BACKOFF))` silently wrapped to a
  negative `short` once `timeout * 2` exceeded 32 767. Backoff now uses
  `int` arithmetic and `timeout` is widened to `int`.
- **Real bug fix in `uponGetSampleRequest`:** the upstream created a fresh
  `SecureRandom` per call and used an O(n²) `remove-by-index` pattern.
  Replaced with the protocol's existing `rnd` and Fisher-Yates sampling.
- **Time-interval property widening:** `HyParView.ShufflePeriod` and
  `HyParView.CheckConnectivityPeriod` are parsed as `long` (was `short`),
  so values above 32 767 ms (e.g. minute-scale shuffles) no longer throw.
- Source layout moved from upstream's mixed style to conventional Maven
  `src/main/java/pt/...`.
- Public javadoc on every public type and method.
- **Cert-based peer authentication:** the channel type used by HyParView is
  now driven by the {@code channelName} constructor argument the caller
  supplies (the upstream silently hardcoded {@code TCPChannel.NAME} even
  though the parameter existed). Together with the new
  `HyParView.Channel.*` property pass-through, this lets the gateway plug
  in `AuthChannel` / `TLSChannel` configured with the StoneFlux Private CA
  trust store. See the "Cert-based peer authentication" section above.
