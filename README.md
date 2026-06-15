# Webinar 1 — Powering a fast feature store with ScyllaDB

A real-time feature store that ingests a live exchange **fills firehose**,
maintains fresh per-entity features, and serves them with **sub-millisecond
point reads** to a lightweight inference step. The latency and freshness of
feature retrieval is the thing we showcase.

Measured numbers: [`docs/RESULTS.md`](docs/RESULTS.md) ·
Why this dataset: [`docs/DATASET.md`](docs/DATASET.md)

## What it does

```
 parquet fills          incremental            pipelined            point reads
 (Hyperliquid     ──►   feature state   ──►    blind upserts  ──►   (inference
  node_fills)           (in-memory)            (shard-aware)         fast path)
   replay.py            features.py            writer.py            api.py / dashboard.py
                                                   │
                                              ScyllaDB  ◄── cql/schema.cql
```

Online features maintained:
- **per (wallet, coin):** net position, average entry price, realized PnL, fill
  count, last-seen timestamp.
- **per coin, rolling 1m/5m/1h:** trade volume, taker buy/sell imbalance,
  active-wallet count, net-position concentration (HHI), large-wallet net flow.
- **per wallet:** cumulative realized PnL, trade frequency, an archetype tag
  (market-maker vs directional).

Inference demo: a scorer that flags a coin for **"unusual accumulation"** (and
tags a wallet's archetype) by point-reading that entity's fresh features from
ScyllaDB. The feature fetch is the fast path.

## Measured headlines (3-node dev cluster — see `docs/RESULTS.md`)

| | result |
|---|---|
| point-read latency | **p99 1.6 ms** @ 29k reads/s (scales to 61k/s) |
| write throughput | **108k writes/s** sustained, 0 errors (12-proc loadgen) |
| feature-pipeline rate | ~17k fills/s single Python process (freshness rate) |
| freshness | feature row reflects the active window; last write within it |

## Layout

```
cql/schema.cql              one table per feature group; PK shaped for 1-partition reads
src/feature_store/
  replay.py                 parquet fills firehose, adjustable replay speed
  features.py               incremental feature computation (the stream state)
  writer.py                 pipelined, shard-aware, bounded-in-flight write path
  statements.py             prepared CQL
  consumer.py               replay -> features -> upserts (the ingestion job)
  scorer.py                 unusual-accumulation + archetype (the inference)
  api.py                    FastAPI inference endpoint (point reads, reports db_read_ms)
  dashboard.py              live web dashboard (firehose + scoreboard + read latency)
  loadgen.py                multi-process write-throughput load test
  bench.py                  multi-process read-latency benchmark (tuned vs default)
  apply_schema.py           apply the CQL schema
  config.py                 local / cloud profiles; tuned / default driver settings
docker/docker-compose.yml   3-node ScyllaDB cluster
scripts/                    cluster.sh, monitoring.sh, sync.sh, run.sh
docs/                       DATASET.md (why this data), RESULTS.md (measured numbers)
```

## Run it

Prereqs on the demo host: Docker, and `fs.aio-max-nr >= 1048576`
(`sudo sysctl -w fs.aio-max-nr=1048576`). Python env via `uv`:

```bash
uv venv --python 3.13 .venv
uv pip install --python .venv/bin/python -e .          # or: scylla-driver polars pyarrow fastapi uvicorn websockets
```

Stage the dataset under `data/fills/*.parquet` (gitignored) or point
`FS_DATA_GLOB` at it.

The quickest path is the [`justfile`](justfile) (`just --list` to see all
recipes); the `scripts/*.sh` below are the no-`just` equivalents.

```bash
just cluster-up && just cluster-status                  # 3x UN
just schema                                             # apply CQL
just consume 1 0                                        # ingest 1 day at max speed
just bench 600000 12 6                                  # read latency (add 'default' arg for before/after)
just loadgen 12 1                                       # write-throughput ceiling
just dashboard 30 1                                     # live dashboard on :8090

# equivalent without just:
scripts/cluster.sh up && scripts/cluster.sh status      # 3x UN
scripts/run.sh schema                                   # apply CQL

# stream fills -> features -> ScyllaDB (max speed; writes sample keys for the bench)
scripts/run.sh consume --speed 0 --days 1 --sample-out sample_keys.csv

# inference fast-path latency (run again with --tuning default for before/after)
scripts/run.sh bench --keys sample_keys.csv --n 600000 --procs 12 --threads 6

# write-throughput ceiling
scripts/run.sh loadgen --procs 12 --days 1

# inference endpoint  ->  http://<host>:8080/score/coin/BTC
scripts/run.sh api

# live dashboard      ->  http://<host>:8090   (FS_SPEED, FS_DAYS to taste)
FS_SPEED=30 FS_DAYS=1 scripts/run.sh dashboard
```

Optional ScyllaDB Monitoring (Grafana): `scripts/monitoring.sh up`.

## ScyllaDB Cloud

The code is Cloud-portable. Set the `cloud` profile env (`FS_CLOUD_BUNDLE` for a
connect bundle, or `FS_CLOUD_HOSTS`/`FS_CLOUD_USER`/`FS_CLOUD_PASS`) and pass
`--profile cloud`. Re-run the benchmarks there to capture Cloud numbers.
