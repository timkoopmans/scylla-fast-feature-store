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
**3 × i8g.2xlarge Data Store** + **2 × r7g.xlarge Vector Search**. Client: one
48-vCPU ARM EC2 box, same region.

### How these numbers are measured (this section differs from the rest)

Everything above is **client-observed**. That breaks down here: driving ~100k
writes/s from Python needs ~20 processes, and any latency probe running on the
same box then competes with them for CPU and reports its own queuing as database
latency. At 137k writes/s the client-side read p99 read 13.5 ms while the
cluster itself was serving 5-8 ms.

So the numbers below come from the cluster's own metrics, via the **ScyllaDB
Cloud Prometheus proxy** (`Actions > Enable Cluster Metrics`), scraped by
`cloudmetrics.py`:

- `scylla_storage_proxy_coordinator_{read,write}_latency` (scheduling group
  `sl:default` — user workload, not gossip/compaction)
- the Vector Store's own `request_latency_seconds`, `index_size`,
  `index_modified`

Three properties worth knowing before quoting them:

- **They answer "what did the database do", not "what did the app see."**
  Server-side latency excludes network RTT and client queuing. A single ANN
  query measured 1.87 ms client-side while the Vector Store recorded 140 µs for
  the same work — both true, ~93% of the client-observed time is transport.
- **Percentiles are windowed and interpolated.** The exported histograms are
  cumulative since node start, so `cloudmetrics` differences two scrapes and
  interpolates within the bucket exactly as Prometheus `histogram_quantile`
  does. Returning the bucket's upper bound instead reported 10.00 ms where
  Grafana showed 6.91 ms on identical data.
- **The floor is a 20 s cadence.** ScyllaDB Cloud collects on a fixed ~20 s
  interval (verified: sample timestamps step in exactly 20,000 ms), so polling
  the proxy faster returns byte-identical samples. The node exporters (9180 /
  9100) are not reachable — Cloud exposes only 9042 and the metrics proxy. The
  dashboard therefore draws sparkline *shape* from its own client-side stream
  (~3/s) and prints server-measured *numbers* beside them.

### Load model: an exchange simulator, not blasters

The webinar-1 "blasters" preload one slice of day 1 and re-upsert it forever:
high ops/s, but the same keys, so the wallet population never grows and no
vector ever changes. `_sim_proc` instead shards the **46 day files** across
workers and streams each day once, computing real features and real behaviour
vectors as it goes — genuine exchange traffic over the whole population.

For scale: the real exchange runs ~60-97 fills/s (385M fills / 46 days), so a
production deployment of this system is ~300-500 writes/s. The configuration
below is roughly **200× production traffic**.

### Index population (`vecgen`)

Two streaming polars group-bys turn 385M fills into per-entity aggregates in
seconds, then one upsert each — reusing `WalletState` and `wallet_vector()`, so
the backfilled vectors are identical to what the streaming path produces.

| scope | entities | aggregate time | write time |
|---|---|---|---|
| per wallet | 224,135 | 5.7 s | 61 s (3,649/s) |
| **per (wallet, coin)** | **981,366** | 3.4 s | 204 s (4,802/s) |

Both are dataset **ceilings**, not samples. Only 47,162 distinct wallets trade
on day 1 and ~12k genuinely new ones appear per additional day, saturating at
224,135 — a bigger *wallet* index is not possible from this data. The
(wallet, coin) grain is 4.4× larger and is what the demo searches.

### Demo configuration — single digit across the board

```
FS_SIM_PROCS=20  FS_DAYS=46  FS_EMBED_ON=0
FS_ANN_PROCS=4   FS_ANN_THREADS=4   FS_ANN_INDEX=wallet-coin
```

| metric (server-measured) | value |
|---|---|
| coordinator write ops/s | **83k-114k** |
| write p99 | **3.58-4.28 ms** |
| read p99 | **6.59-7.17 ms** |
| ANN p99 (in the Vector Store) | **0.50-4.75 ms** |
| ANN queries/s | 2.8k-3.9k |
| vectors searched | **981,366** |
| empty / short ANN results | 0 |

**Headline:** one cluster sustaining ~100k coordinator writes/s of real exchange
traffic while answering thousands of ANN queries/s over ~1M vectors, with every
tail in single-digit milliseconds — no second database, no sync ETL.

**Ceiling:** 36 sim procs reach ~137k write ops/s, but the cluster's own write
p99 goes to **40.96 ms** and read p99 to 57.3 ms. ~85-115k is this cluster's
single-digit envelope with the vector index live.

### What CDC costs

Creating a vector index enables CDC on the base table. Same 20-process load,
same ~83.5k coordinator write ops/s, only the index differs:

| | write p99 | read p99 |
|---|---|---|
| `wallet_coin_embedding_idx` present | 3.584 ms | 7.168 ms |
| index dropped | **2.048 ms** | **3.072 ms** |

**~1.75× on write tail, ~2.3× on read tail.** Reads suffer more: the Vector
Store is concurrently *reading* the CDC log back out while it competes with
user reads. (The log table survives the index drop — CDC stays enabled until
`ALTER TABLE ... WITH cdc = {'enabled': false}`; latency recovered because
nothing was consuming it.)

This also reconciles the dashboard with the Cloud's Grafana, which report
different layers:

| | |
|---|---|
| coordinator writes (what the app issued) | 88.4k/s |
| × RF=3 → base-table replica writes | 265.2k/s |
| CDC operations | 96.2k/s (≈1 per base write) |
| × RF=3 → CDC-log replica writes | 288.6k/s |
| **replica writes (what storage did)** | **≈554k/s** (Grafana: 563k) |

So the storage layer does **~6.4× the application's write count** — `RF × (base
+ CDC)`. That is the price of an always-fresh vector index with no ETL, and at
production rates it is unmeasurable.

### Three findings that were counter to the obvious guess

- **Index size costs nothing.** Settled at 224k vectors the index served p99
  3.909 ms; at 33k it served 3.690 ms; at 981k, 4.5-4.75 ms. What *did* cost
  7.27 ms was querying an index still digesting a backfill — a transient, not a
  size effect.
- **Raw write volume is nearly free to the ANN path; embedding writes are not.**
  71k vs 132k writes/s moved ANN latency not at all. But *any* active
  re-indexing put ANN into a ~12 ms p50 regime, and one embedding writer cost as
  much as four — switch-like, not proportional. Even the paced ingest's 2 s
  flush held p99 at 13.4 ms while leaving p50 at 1.9 ms. Hence the EMBEDDING
  WRITES toggle: the "fresh vectors" beat and the "low ANN tail" beat cannot
  share a moment, and there is no useful middle setting.
- **The ANN path is gated by coordinator capacity, not the vector tier.**
  Resizing the Data Store (i8g.xlarge → i8g.2xlarge) took peak ANN throughput
  from 16,334 → 45,632 q/s. The Vector Search tier was never scaled.

### ANN query scaling (client-observed, quiet cluster)

Concurrency sweep with no write load, on the settled 3-node cluster. These are
client-side, so they include network and client queuing:

| concurrency | ANN queries/s | p50 ms | p99 ms |
|------------:|--------------:|-------:|-------:|
| 1    | 352    | **1.020** | 1.587 |
| 16   | 2,106  | 1.616 | 3.690 |
| 96   | 9,561  | 2.947 | 9.911 |
| 256  | 19,730 | 3.262 | 11.087 |
| 480  | 29,459 | 5.159 | 20.471 |
| 1232 | **45,632** | 13.641 | 48.266 |

0 errors, 0 empty, 0 short throughout; never saturated. A single ANN lookup over
the whole population costs **p50 1.02 ms** — the same order as the feature point
read, on the same cluster.

**TODO:** re-run after scaling the Vector Search tier (still 2 × r7g.xlarge);
and past ~137k writes/s the Data Store is the limit, so genuinely "hundreds of
thousands with single-digit tails" is a data-node sizing question.

## Reproduce

```bash
scripts/cluster.sh up && scripts/cluster.sh status          # 3× UN
PYTHONPATH=src .venv/bin/python -m feature_store.apply_schema --schema cql/schema.cql
PYTHONPATH=src .venv/bin/python -m feature_store.consumer --speed 0 --days 1 --sample-out sample_keys.csv
PYTHONPATH=src .venv/bin/python -m feature_store.bench read --keys sample_keys.csv --n 600000 --procs 12 --threads 6
PYTHONPATH=src .venv/bin/python -m feature_store.loadgen --procs 12 --days 1

# webinar 2 — vector search (on the demo host, against Cloud)
PYTHONPATH=src .venv/bin/python -m feature_store.apply_schema --schema cql/schema_vector.cql
PYTHONPATH=src .venv/bin/python -m feature_store.vecgen --scope both   # 224k + 981k vectors
PYTHONPATH=src .venv/bin/python -m feature_store.bench ann --n 40000 --procs 4 --threads 4
PYTHONPATH=src .venv/bin/python -m feature_store.cloudmetrics          # server-side snapshot

# the demo itself (from the laptop; needs FS_REMOTE + ~/.fs-cloud.env on the host)
just cloud-dashboard          # 20 sim procs, ANN concurrency 16, embedding writes off
```
