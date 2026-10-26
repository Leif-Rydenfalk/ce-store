# ce-store + ce-wire v2 — storage and transport for the next three orders of magnitude

Status: design + working prototype (2026-10-26). Prototype: `ce-store` (Rust) — content ids,
manifests, Reed–Solomon, placement, frame codec, and a failure simulation. Numbers in §8 are from
that prototype, single core.

---

## 1. The target

| | today | target |
|---|---|---|
| bytes under management | ~10¹² | **10¹⁵** (a quadrillion) |
| devices | 10³ | **10⁹ users, 10¹⁰ devices** |
| topology | one region, one door | **global, many regions, no single door** |
| transport | HTTP + JSON-RPC | **ce-wire v2, binary, not HTTP** |
| naming | URLs and paths | **content ids (`b3:…`) and cells** |
| embedding | `<script>` from one host | **a shim that joins the mesh from anywhere** |

Three properties decide whether this works at that size, and everything below is in service of them:

1. **Content is addressed, never located.** A thing is its hash. Where it lives is a routing
   question, not an identity question, so any cell can answer for any object and caches are free.
2. **Storage is a fold, not a database.** Shards are immutable bytes with known hashes; placement
   and repair are idempotent functions of (cid, cell set).
3. **The wire is a frame, not a document.** No headers, no URLs, no text parsing in the hot path.

---

## 2. ce-wire v2 — the protocol

Binary frames over QUIC (native devices), WebRTC data channels (browsers), TCP (old devices).
Nothing on the wire is HTTP: no request line, no headers, no URLs, no chunked encoding.

```
frame := varint(body_len) body
body  := u8 op, u16 flags, varint stream, payload[body_len-4]
```

* `body_len > 8 MiB` is refused **before allocation** — a hostile length cannot exhaust memory.
* `stream` multiplexes; flow control is per stream (credit), so one slow subscriber never blocks a
  download.
* `flags`: `bit0 = end`, `bit1 = more`, `bit2 = error`, `bit3 = encrypted-payload`.

### 2.1 Ops

| op | dir | payload | meaning |
|---|---|---|---|
| `HELLO` | both | `{ver, node_id, caps[], nonce}` | open a connection, present capabilities |
| `OPEN` | both | `{cid, range?}` | start a stream for one object |
| `GET` | → | `{cid}` | fetch a manifest |
| `WANT` | → | `{cid, offset, len, prio}` | request a byte range of a shard |
| `DATA` | ← | `{offset, bytes}` | bytes; the receiver verifies the shard hash |
| `HAVE` | ← | bitfield | what a cell holds (for repair and anti-entropy) |
| `ACK` | → | `{stream, n}` | credit return |
| `SUB`/`PUB` | both | `{topic, frame}` | cell events: shard added, cell draining, tile published |
| `REDIRECT` | ← | `{cells[]}` | "not my shard; these hold it" |
| `PING`/`PONG` | both | — | liveness, RTT for placement |

### 2.2 Handshake and capabilities

`HELLO` carries a **ce-cap** token: Ed25519-signed, `{subject, scope, expiry, nonce}`, bound to the
connection nonce so it cannot be replayed. The responder replies `HELLO_ACK {sig}` over the same
nonce. Every op runs against one cap; there is no ambient authority. A `WANT` for a shard the cap
does not cover is `ERROR {code: 403, need: "shard:get:<cid>"}` — never a silent deny.

### 2.3 Why not HTTP

HTTP/2 and HTTP/3 solved documents. For storage traffic the cost is in the framing: text headers on
every request, URLs that cannot express "range of a shard of an object", no way to say "you are the
wrong cell, try these". A binary frame with a 13-op vocabulary carries the same semantics in 12–20
bytes of overhead, and lets a browser join over a data channel without a server that speaks HTTP to
the mesh at all.

---

## 3. Storage model

### 3.1 Identity

`b3:` + BLAKE3-256 hex. Same prefix ce-net already uses for bundles, so nothing is renamed.

### 3.2 Manifest

```
{ version: 2, size, k, m, shard_len, shards: [Cid; k+m] }
```

The manifest's own Cid names the file's layout. Two files with the same bytes but different k/m have
different manifests and therefore different ids — the parameters are part of the content, not
metadata.

### 3.3 Erasure coding

Default **12 data + 4 parity** (1.33× raw), because it is the cheapest point that survives four
simultaneous cell losses:

| profile | k+m | overhead | survives | use |
|---|---|---|---|---|
| hot | 6+3 | 1.50× | 3 | small objects, latency-bound |
| default | 12+4 | 1.33× | 4 | the corpus |
| cold | 20+6 | 1.30× | 6 | archives, large objects |

A file is split into `k` equal shards, so overhead is exactly (k+m)/k — no padding of a 4 KiB file
into twelve 64 KiB chunks. Byte-range reads are ranges inside a shard, not chunk ids.

### 3.4 Placement

Rendezvous (highest-random-weight) hashing over eligible cells, weighted by free capacity:

```
score(cell, shard) = u128(blake3(cell.id ‖ shard.cid)[..16]) × cell.capacity_gb
```

Constraints, in order: **one shard per cell**; at most `ceil(n / domains)` per failure domain
(region × rack); never two shards of one object in one domain when a domain is free. The result is
deterministic from the cell set, so every node computes the same placement without a coordinator,
and a cell that leaves only changes the objects that mapped to it.

### 3.5 Repair

`HAVE` bitfields + a per-cell Merkle tree over shard ids give anti-entropy in `O(log n)` per
exchange. A repair job: detect missing shard → fetch any `k` survivors → re-encode → place →
verify hash. Idempotent by construction; a duplicate repair is a no-op.

Targets: hot shards repaired within **1 hour**, cold within **24 hours**. A shard that is unhealthy
for longer than `m × repair_window` is a durability incident, not a background task.

### 3.6 Metadata

Manifests are immutable and small (~120 B + 32 B/shard). They are stored with **5 replicas** plus a
DHT pointer under `H(manifest_cid)`. Mutable names (a Trana post's current tile, a cell's manifest)
are LWW registers with version vectors — the only mutable state in the system.

---

## 4. Linking content everywhere

A reference is `ce://<cid>` or, in ce-ref's existing grammar, one of three new kinds:

| kind | resolves to | PASS when |
|---|---|---|
| `blob:<cid>` | a stored object | manifest known **and** ≥ k shards reachable **and** a random shard's hash verifies |
| `blob:<cid>@<start>-<end>` | a byte range | range inside `size` and the shards covering it are reachable |
| `tile:<surface>/<bucket>` | a ranking tile | tile hash matches the signed tile |
| `cell:<cell_id>` | a cell's health | reachable and signed by the cell key |

The three-verdict doctrine stays: **PASS** only after a check, **FAIL** when the namespace says the
name is well-formed but the object is absent, **CANNOT DETERMINE** when the store cannot be asked.
A ref never resolves to a URL — that is the whole point: a link that survives every host move.

ce-ref gains a `ce-store resolve <cid> --json` call with a 300 ms budget; timeout is
CANNOT DETERMINE, never a guess. `blob:` refs then work in docs, threads, skills and pages with no
new syntax for the writer.

---

## 5. Embedding

`ce-embed.js` (~18 KB) + a 90 KB WASM core:

```
<ce-embed cid="b3:…" mode="inline|player|thumb"></ce-embed>
```

1. Shard lookup via a local resolver (DHT walk, ≤3 hops typical).
2. Transport choice: **WebRTC data channel** in browsers (SCTP, not HTTP), **QUIC** on native,
   **TCP** as a last resort. Signaling for WebRTC rides the page's own channel; once open, every
   byte is ce-wire.
3. Range fetch → verify → decode with the manifest's k/m → render.
4. Cache the decoded object in the shim's local store (IndexedDB), keyed by cid, so a second page
   on the same origin is free.

The honest boundary: a browser must load the shim from *some* origin, and a page is served over
HTTP today. The **data plane** is not HTTP — that is the part that has to scale.

---

## 6. Scale

| quantity | design point | headroom |
|---|---|---|
| stored bytes | 10¹⁵ logical → 1.33×10¹⁵ raw | 3 profiles, tiering |
| objects | 4×10⁹ at 256 KiB average | manifests 120 B → 480 GB metadata, sharded DHT |
| cells | 13,300 × 100 TB usable | 10⁴–10⁵ cells, 3+ regions |
| reads | 5×10¹⁰/day ≈ 580k/s avg, 3M/s peak | 225 QPS per cell at peak |
| writes | 5×10⁹/day ≈ 58k/s | 4.4 QPS per cell |
| read bandwidth | ~150 GB/s avg before cache | edge caches absorb 80–90% |
| DHT lookups | 3M/s peak | 3-hop walk, hot-set cache at resolvers |

The system is **bandwidth- and cache-shaped**, not compute-shaped: 3M QPS across 13k cells is
ordinary, while 150 GB/s of egress is the real bill. That is why content addressing matters: a cache
key is the content, so every layer can serve the same object without coordination.

---

## 7. Reliability

Failure model: cells fail independently at ~2%/year, plus correlated events (rack, region, power).
EC(12,4) tolerates any 4 cell losses; placement keeps shards in different domains.

For one object, with 16 cells at AFR 2%: λ ≈ 16 × (0.02/8760) ≈ 3.7×10⁻⁵/h. With repair inside
1 h, P(≥5 failures inside the window) ≈ λ⁵/5! ≈ 1×10⁻²³. The real risk is correlated loss of a
domain, which is why the placement constraint is a domain constraint, not a cell budget — and why
the design keeps one spare domain when one is free.

Monitoring, in order of importance: **objects below k shards**, repair queue age, domain
concentration per object, cell capacity drift. An object below k is a page, not a graph.

---

## 8. Measured (prototype)

300 files, 87 MiB logical, k=12 m=4, 48 cells in 12 domains:

| step | result |
|---|---|
| ingest (split + encode + place) | 528 ms |
| raw overhead | **1.33×** (exactly (k+m)/k) |
| kill 4 cells (431 shards lost) | — |
| rebuild all files from survivors | **129 ms**, every content hash verified |
| repair 431 shards | 18 ms |
| wire codec | **6.4M frames/s**, 8.5 GiB/s single core |

Tests: frame roundtrip, two frames in one buffer, hostile length refused, every truncation refused,
EC survives exactly m losses and refuses m+1.

---

## 9. Migration (no flag day)

1. **Blobs behind today's media store.** `media:<sha256>` already names content by hash; add
   `blob:` as an alias that resolves through ce-store. Nothing else changes.
2. **Tiles become content.** Trana's 10-minute static tile becomes an immutable `tile:` object with
   a bucket id; trana.app reads `tile:feed/<bucket>` through the shim.
3. **Sites become bundles.** A published site is already a `b3:` bundle; store it as an object and
   serve it from the nearest cell, with the current host as a redirector.
4. **HTTP door becomes an adapter.** `/mcp` stays for MCP clients; internally it speaks ce-wire.

Each step is additive and reversible; no existing id changes.

---

## 10. ce-ref upgrade

Add to `KINDS`: `blob`, `tile`, `cell` (and `blob@range` in the parser). Resolvers call the local
ce-store daemon, never the network directly:

```
resolve("blob:b3:…")  ->  ce-store resolve b3:… --json --budget 300ms
```

* `blob:` PASS requires a manifest, ≥ k reachable shards, and one verified shard hash — the same
  "resolving means checking" rule ce-ref already lives by.
* `FAIL` when the manifest is known to the namespace but the object is not there.
* `CANNOT DETERMINE` when the store does not answer inside the budget, with the reason.

Nothing in the grammar's precedence changes: `@` still binds tighter than `#`, so
`blob:b3:…@0-1048575` is a range and `blob:b3:…#cover` is a fragment of the object's metadata.

---

## 11. Risks

1. **Browser path is the weak link.** WebRTC works but signaling, NAT and corporate networks are
   hostile; keep a TCP relay path and never make the shim a hard dependency for first paint.
2. **Erasure coding punishes small objects.** Below ~64 KiB, 1.33× plus metadata is worse than
   replicating; use the 6+3 profile and batch small objects into packs.
3. **DHT churn** at 10⁵ cells: provider records need short TTLs and aggressive caching; measure
   lookup latency, not hop count.
4. **Repair bandwidth** is the hidden cost: 1% annual churn of 1.33 PB is ~13 TB/day of rebuild
   traffic. Budget it.
5. **Two protocols forever.** HTTP stays at the edge for MCP and pages; the mesh speaks ce-wire. Do
   not pretend they are one thing.
