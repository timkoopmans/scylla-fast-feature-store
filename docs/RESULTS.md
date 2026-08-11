# Measured results — Webinar 1 demo

> Methodology note: a single Python process is GIL-bound and becomes the client
> bottleneck. Benchmarks therefore drive load from **multiple processes** so we
> measure ScyllaDB, not the client. Where latency looks like pure queue depth
> (in-flight ÷ throughput), it is — flagged inline. We report **p99** — the tail
> is the SLA for online inference.

## ScyllaDB Cloud (AWS us-east-1) — the real run

Client: one EC2 box (48 vCPU, ARM, us-east-1) → **3-node ScyllaDB Cloud cluster**
(`AWS_US_EAST_1`, RF=3, 15 shards/node = 45 shards). Same region as the client.
Driver verified: **shard + token aware** (15 connections/node, one per shard),
**ICS** on the point-read tables, **prepared statements**, **LOCAL_ONE**.

| metric | result |
|---|---|
| point-read p99 | **2.17 ms** @ 48 concurrent (48 single-thread procs), 0 errors |
| network floor (EC2→Cloud) | 1.28 ms p99 (so ~0.9 ms is the cluster) |
| read throughput (1 client box) | ~22–29k reads/s — **client-bound**, not the cluster |
| write throughput | **~230k writes/s** (88 procs), 8.38M writes, 0 errors |
| write scaling | 44 procs → 217k/s · 64 → 221k · 88 → 230k (all-Python, one box) |

**Headline:** sub-2.2 ms p99 point reads, and **~230k writes/s in pure Python
from a single box** (0 errors), against a managed 3-node Cloud cluster in the same
region. Still loader-bound — the 45-shard cluster was not the bottleneck. The path
beyond is more loader boxes or a native driver (`latte`/Rust); no synthetic data
needed.

### Pushing pure Python (what actually moved the write number)
- **Even per-worker split removes the finish "taper."** Sharding by `hash(addr)`
  gives whale wallets far more rows, so a few workers run long while the rest sit
  idle — `total ÷ max-window` then *understates* the peak (~100k). Splitting rows
  evenly (stride by index) makes workers finish together → true peak **~217–230k/s**.
- **Oversubscribe processes.** Writes are network-bound, so >cores helps a little:
  44 → 217k, 88 → 230k on a 48-vCPU box.
- **`execute_concurrent_with_args` was *slower*** (74k vs 94k at 44 procs, pre-fix):
  it returns results *in submission order*, so draining the generator suffers
  head-of-line blocking. Our **unordered** `Pipeline` (fire + release on whatever
  finishes first) is the better high-throughput path. The "recommended" helper
  optimises for ordered results, not raw throughput.
- The driver's **Cython** extensions are already compiled (`.so`) — the native
  fast path is on; the residual limit is the GIL on Python glue (→ multiprocessing).

### Read-latency methodology
> The bench's multi-*threaded* model (6 threads/proc) inflates the read tail over
> a real network (one driver reactor per process serialises the threads): it read
> ~29k/s but at p99 ~9 ms. **One thread per process** removes that and shows the
> true 2.17 ms p99. Use `--threads 1 --procs 48` for clean Cloud latency.

## Local dev cluster (Docker)

The numbers below are from a single reference host (24 cores / 183 GB) against a
**3-node ScyllaDB 2026.1.5 cluster in Docker**, RF=3, `--smp 6 --memory 10G
--overprovisioned 1 --developer-mode 1` per node — a *dev* cluster, useful for
the relative/before-after results and local iteration.

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

## Vector search (webinar 2) — ANN on the same cluster

Measured 2026-08-11 against **ScyllaDB Cloud**, `AWS_US_EAST_1`, RF=3:
**3 × i8g.2xlarge Data Store** + **2 × r7g.xlarge Vector Search**. Client: the
same 48-vCPU ARM EC2 box, same region. Query: `ORDER BY embedding ANN OF ?
LIMIT ?` over `wallet_features` (`wallet_embedding_idx`, COSINE, 16-dim), k=10,
driven by `bench.py ann` — the same multiprocess client shape as the point-read
bench, so the two p99s are directly comparable.

> The bench queries the index **directly**, not via `similar_wallets()` — that
> helper adds a seed point-read plus k neighbour point-reads per call, which
> would measure the feature store instead of the index. Empty and short (`<k`)
> neighbour lists are counted separately from latency: an empty result is *fast*
> but wrong, and must never be averaged into a latency win.

### Index population

`vecgen` backfills the whole dataset: two streaming polars group-bys turn 385M
fills into per-wallet aggregates in **5.7 s**, then one upsert per wallet.

| | |
|---|---|
| wallets vectorized | **224,135** (0 errors, 61 s to write) |
| write rate | 3,649 vectors/s (single process) |

224k is this dataset's **ceiling**, not a sampling choice: 47,162 distinct
wallets trade on day 1 and only ~12k genuinely new ones appear per additional
day, saturating well before the 46 days are up. "A vector for every wallet that
traded on the exchange" is the honest framing — not millions.

### Query scaling (quiet cluster)

Concurrency sweep, no write load:

| concurrency | ANN queries/s | p50 ms | p99 ms | max ms |
|------------:|--------------:|-------:|-------:|-------:|
| 1    | 352    | **1.020** | 1.587  | 2.6 |
| 16   | 2,106  | 1.616 | 3.690  | 16 |
| 96   | 9,561  | 2.947 | 9.911  | 27 |
| 256  | 19,730 | 3.262 | 11.087 | 32 |
| 480  | 29,459 | 5.159 | 20.471 | 50 |
| 800  | 37,967 | 8.959 | 35.336 | 98 |
| 1232 | **45,632** | 13.641 | 48.266 | 145 |

0 errors, 0 empty, 0 short throughout. Never saturated — throughput was still
climbing at 1232 with p99 under 50 ms; at that point the Python client (44 procs
× 28 threads on 48 vCPU) is a plausible limit too.

**Headline:** a single ANN neighbour lookup over the whole wallet population
costs **p50 1.02 ms** — the same order as the feature point read, on the *same*
cluster, with no second database and no sync ETL.

> These ran with the pre-`vecgen` index (~33k vectors). Once settled, the 224k
> index measured p50 1.650 / p99 3.909 ms at concurrency 16 — statistically the
> same as 33k did (1.616 / 3.690). **Index size is not what costs latency here.**

### What costs ANN latency: CDC re-index, not writes or index size

Everything below at ~100k writes/s, 224k vectors, ANN concurrency 16. The only
variable is how many blasters carry behaviour vectors (`FS_BLAST_EMBED`):

| embedding blasters | writes/s | p50 ms | p99 ms |
|-------------------:|---------:|-------:|-------:|
| 0  | 103.5k | **2.280** | 14.308 |
| 1  | 101.1k | 12.269 | 20.941 |
| 2  | 101.2k | 12.453 | 22.274 |
| 2 (12 blasters) | 70.9k | 12.054 | 21.561 |
| 4 (12 blasters) | 71.8k | 12.024 | 21.589 |
| 2 (24 blasters) | 132.5k | 12.929 | 23.023 |

Three findings, each counter to the obvious guess:

- **Raw write volume is nearly free to the ANN path.** 71k → 132k writes/s moved
  latency not at all. Feature upserts don't touch the vector index.
- **Embedding writes act as a switch, not a dial.** *Any* active re-indexing puts
  ANN into a ~12 ms p50 regime; one embedding blaster costs as much as four.
- **The tail is driven by re-index, the median by everything else.** With zero
  embedding blasters the paced ingest still flushed vectors every
  `FS_EMB_FLUSH_SECS=2.0`; that alone held p99 at 13.354 ms while leaving p50 at
  1.936 ms. Pausing it dropped p99 to 5.542 ms.

### Demo configuration — single-digit p99 under load

```
FS_BLASTERS=18  FS_BURST_BLASTERS=16  FS_BLAST_EMBED=0  FS_EMB_FLUSH_SECS=3600
FS_ANN_PROCS=4  FS_ANN_THREADS=4  FS_ANN_K=10          # ANN concurrency 16
```

At **~105k writes/s** streaming underneath:

| concurrency | ANN queries/s | p50 ms | p95 ms | p99 ms | |
|------------:|--------------:|-------:|-------:|-------:|---|
| 16 | 3,546 | 1.887 | 3.620 | **5.542** | ✅ |
| 24 | 3,817 | 2.456 | 4.585 | **8.131** | ✅ |
| 32 | 3,907 | 3.074 | 5.593 | 11.506 | ❌ |
| 48 | 4,376 | 3.149 | 5.584 | 11.283 | ❌ |

**Concurrency 24 is the ceiling for a single-digit p99** — past it the tail
crosses 10 ms for ~2% more throughput. Live values from the dashboard's own
prober fleet at concurrency 16: **100.6k writes/s, ANN p99 5.978 ms, 7,869 ANN
queries/s, feature-read p99 2.876 ms, 0 empty results** — three workloads, one
cluster.

> **Trade-off to stage around:** single-digit ANN p99 requires a quiescent index,
> so the "fresh vectors / index build rate" beat and the "ANN latency" beat
> cannot share a moment. Because the effect is switch-like there is no useful
> middle setting — run them as two beats.

**TODO:** the Vector Search tier (2 × r7g.xlarge) was never scaled; the Data
Store resize (i8g.xlarge → i8g.2xlarge) alone took peak ANN throughput from
16,334 → 45,632 q/s, so the ANN path is gated by coordinator capacity more than
by the vector tier. Worth re-running after a VS resize.

## Reproduce

```bash
scripts/cluster.sh up && scripts/cluster.sh status          # 3× UN
PYTHONPATH=src .venv/bin/python -m feature_store.apply_schema --schema cql/schema.cql
PYTHONPATH=src .venv/bin/python -m feature_store.consumer --speed 0 --days 1 --sample-out sample_keys.csv
PYTHONPATH=src .venv/bin/python -m feature_store.bench read --keys sample_keys.csv --n 600000 --procs 12 --threads 6
PYTHONPATH=src .venv/bin/python -m feature_store.loadgen --procs 12 --days 1

# webinar 2 — vector search
PYTHONPATH=src .venv/bin/python -m feature_store.apply_schema --schema cql/schema_vector.cql
PYTHONPATH=src .venv/bin/python -m feature_store.vecgen                    # 224k vectors
PYTHONPATH=src .venv/bin/python -m feature_store.bench ann --n 40000 --procs 4 --threads 4
```
