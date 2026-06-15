# Measured results — Webinar 1 demo

All numbers from a single reference host (24 cores / 183 GB) against a **3-node
ScyllaDB 2026.1.5 cluster in Docker**, RF=3, `--smp 6 --memory 10G --overprovisioned 1
--developer-mode 1` per node. This is a *dev* cluster (developer-mode on,
overprovisioned, three nodes sharing one host) — numbers are conservative vs a
properly-sized ScyllaDB Cloud cluster. Re-run on Cloud once creds are wired.

> Methodology note: a single Python process is GIL-bound and becomes the client
> bottleneck. Benchmarks therefore drive load from **multiple processes** so we
> measure ScyllaDB, not the client. Where latency looks like pure queue depth
> (in-flight ÷ throughput), it is — flagged inline.

## Low-latency point reads (inference fast path) — *Objective 3*

`wallet_coin_features` single-partition point read, multiprocessing client.

We report **p99** — the tail is the SLA for online inference.

| concurrency | reads/s | p99 ms |
|------------:|--------:|-------:|
| 16  (4p×4t)   | 28,608 | **1.606** |
| 72  (12p×6t)  | 52,498 | 3.278 |
| 128 (16p×8t)  | 61,540 | 5.156 |

**Headline:** point reads hold **p99 ≈ 1.6 ms** at ~29k reads/s, and stay in the
low single-digit milliseconds (p99 ≈ 5 ms) as throughput scales to ~61k reads/s
on one client box.

## High write throughput — *Objective 2*

| path | writes/s | notes |
|------|---------:|-------|
| feature consumer (1 Python process) | ~17,000 | CPU-bound on feature math; this is the *freshness* rate, not a DB limit |
| write loadgen (12 processes, real fills, shard-aware, LOCAL_ONE) | **108,302** | 8.48M writes, 0 errors, 1 day of fills |

Write latency under the loadgen (p99 ~410 ms) is in-flight queue depth
(2048 in-flight × 12 procs ÷ 108k/s), not server latency.

**Headline:** 108k writes/s sustained into a 3-node dev cluster with zero errors;
linear with shards. ScyllaDB had headroom throughout. The path to >1M ops/s is
cluster sizing + a native (non-GIL) load driver on the **real** 385M-fill
dataset — no synthetic data required.

## Tuning before/after — *Objective 4*

On this fast, low-latency local cluster the read-path tuning levers
(token/shard-aware vs round-robin; LOCAL_ONE vs LOCAL_QUORUM; prepared vs
unprepared) did **not** separate at the load a Python client can produce — the
client is the bottleneck, so server-side inefficiencies stay hidden:

| read profile (12p×6t) | reads/s | p99 ms |
|------|--------:|-------:|
| tuned (prepared, shard-aware, LOCAL_ONE) | 52,152 | 3.311 |
| default (unprepared, round-robin, LOCAL_QUORUM) | 51,765 | 3.084 |

This is itself an honest teaching point. The before/after that *does* bite is on
the **write path under saturation** (consistency level + prepared statements) and
on a server that is actually the bottleneck — to be captured on the tuned Cloud
cluster, and via the `loadgen --tuning tuned|default` comparison under higher
in-flight. **TODO:** capture write-path tuned vs default + a Cloud run.

## Inference endpoint (`api.py`) — *Objective 1 + 3*

Server-measured `db_read_ms` returned in every response:

| endpoint | reads | db_read_ms |
|----------|-------|-----------:|
| `/features/wallet/{addr}/coin/{coin}` | 1 point read | **0.92–1.0** |
| `/archetype/wallet/{addr}` | 1 point read | **0.92** |
| `/score/coin/{coin}` | 3 window reads (1m/5m/1h) | **~2.1** |

## Live dashboard (`dashboard.py`) — reads stay fast *while* writing

Ingest runs in a separate process from the inference reader (so the GIL doesn't
make reads look slow). With the firehose writing live, the dashboard's point-read
widget held **p99 ~1.1–1.7 ms**.

**Feature freshness (write→read): ~0.9 ms (≈900 µs).** The dashboard probes the
store's freshness *floor* — write a feature, immediately read it back, time the
round trip. At LOCAL_ONE the write commits and is readable on that replica with no
quorum wait, so a freshly-computed feature is visible in **hundreds of
microseconds**. (Per-entity write-through features are therefore as fresh as the
firehose; the windowed coin features are bounded instead by their flush cadence —
0.25 s here, a tunable knob.)

**Read tail under a write burst (the BURST button).** Hitting BURST spikes replay
to max speed. In one capture writes rose **5,084 → 11,450 writes/s (2.25×)** while
read **p99 stayed flat — 1.62 → 1.58 ms**. The point
of the demo: feature retrieval does not degrade when ingestion spikes. (For a more
dramatic on-stage contrast, lower the base speed, e.g. `FS_SPEED=10`, so the burst
is a larger multiple.) The dashboard also shows per-coin write-load skew (BTC/ETH/
HYPE dominate — motivates partition-key design), taker buy/sell imbalance per coin,
and the live wallet-archetype mix (market-maker / directional / mixed).

## Reproduce

```bash
scripts/cluster.sh up && scripts/cluster.sh status          # 3× UN
PYTHONPATH=src .venv/bin/python -m feature_store.apply_schema --schema cql/schema.cql
PYTHONPATH=src .venv/bin/python -m feature_store.consumer --speed 0 --days 1 --sample-out sample_keys.csv
PYTHONPATH=src .venv/bin/python -m feature_store.bench read --keys sample_keys.csv --n 600000 --procs 12 --threads 6
PYTHONPATH=src .venv/bin/python -m feature_store.loadgen --procs 12 --days 1
```
