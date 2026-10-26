# ce-store

Content-addressed, erasure-coded storage for the ce mesh, plus ce-wire v2 — a binary frame protocol
that is not HTTP.

- [DESIGN.md](DESIGN.md) — targets (10¹⁵ B, 10⁹ users, 10¹⁰ devices), the protocol, placement,
  durability math, the scale table, migration, and the ce-ref upgrade.
- `src/lib.rs` — cid (BLAKE3 `b3:`), manifest, Reed–Solomon (k-of-n), rendezvous placement with
  failure domains, frame codec with hostile-length and truncation tests.
- `src/bin/sim.rs` — end-to-end: 300 files, 1.33× overhead, kill 4 cells, rebuild every file by
  content hash, repair, and the codec throughput.

```
cargo test --release
cargo run --release --bin sim
```

Measured: 87 MiB ingested in 528 ms; 4 cells lost (431 shards); 300/300 files rebuilt in 129 ms;
431 shards repaired in 18 ms; frame codec 6.4M frames/s (8.5 GiB/s) on one core.

MIT.
