# Webinar 1 — Powering a fast feature store with ScyllaDB

**Runtime:** ~30 minutes. **Audience:** engineers building AI inference pipelines.
**Demo repo:** [the repo root](..) ·
**Measured numbers:** [`docs/RESULTS.md`](../docs/RESULTS.md)

## Four learning objectives (the spine of the talk)

1. **Build a real-time AI pipeline** — firehose → fresh features → inference.
2. **High write throughput** — absorb a bursty event stream.
3. **Low-latency retrieval of fresh features** — sub-ms point reads on the path.
4. **Tuning for max performance** — driver + schema + consistency, with a
   before/after result.

Each section below ends with the objective it lands. Numbers in **`<<...>>`**
are pulled from `docs/RESULTS.md` (don't read placeholders on stage).

---

## 0 · Cold open (0:00–2:00)

> "This is every trade on a live crypto exchange. 239 million fills, 45 days,
> ~60 a second, bursting to thousands. We're going to turn it into fresh
> features and score it in real time — and the only thing standing between a new
> fill and a trading signal is one ScyllaDB read. Let's see how fast that is."

On screen: the replayer streaming, a live counter of fills/s, and a terminal
hitting `/score/coin/BTC` showing `db_read_ms`.

**Why this dataset (30 sec):** a live exchange is an honest stress test — bursty,
skewed to a few hot symbols (BTC/HYPE/ETH dominate), millions of entities. If a
feature store survives this, it survives your recommender / fraud / ranking load.
Full framing in the repo's `docs/DATASET.md`.

## 1 · What a feature store has to do (2:00–6:00) → *Objective 1*

- The shape of online ML serving: **features computed from a stream**, stored
  hot, **point-read at inference time**. Freshness and tail latency are the SLA.
- The features we maintain (show the list, tie each to a real trading question):
  - per **(wallet, coin)**: net position, avg entry, realized PnL, fills, last-seen.
  - per **coin / rolling 1m·5m·1h**: volume, taker buy/sell imbalance, active
    wallets, net-position concentration (HHI), large-wallet net flow.
  - per **wallet**: cumulative PnL, trade frequency, archetype (market-maker vs
    directional).
- The pipeline diagram: `replay.py` (firehose) → `features.py` (incremental
  state) → `writer.py` (pipelined upserts) → **ScyllaDB** → `api.py` (inference
  point-reads) → scorer.
- **Land it:** this is a real-time AI pipeline; the store is the contract between
  the streaming and the serving halves.

## 2 · Schema design (6:00–12:00) → *Objective 3 (sets up the fast read)*

Walk `cql/schema.cql`. The one rule: **shape every table so the inference read
hits exactly one partition.**

- `wallet_coin_features` — `PRIMARY KEY ((addr), coin)`. Partition = wallet →
  millions of partitions, naturally spread. Inference read = one row.
- `coin_window_features` — `PRIMARY KEY ((coin, window), bucket_ts)` clustered
  `bucket_ts DESC`. Latest bucket = `LIMIT 1`. **TTL + TimeWindowCompaction** so
  expiry is a whole-SSTable drop and reads stay on a few recent SSTables.
- `wallet_features` — one row per wallet.
- **Running aggregates: compute-in-stream + blind upsert, not DB counters.** The
  consumer already holds authoritative state, so a blind `INSERT` is the cheapest
  write, idempotent on replay, and safe at LOCAL_ONE. Show the `coin_counters`
  table as the contrast and say *why* we don't use it (read-before-write on the
  replica, not idempotent).
- **Land it:** the schema is what makes the read a single-partition lookup.

## 3 · High-write ingestion (12:00–18:00) → *Objective 2*

- Show `consumer.py`: per fill, update in-memory state, **blind upsert** the
  wallet-coin feature; window snapshots flush on bucket close + periodically;
  wallet features coalesced. **Prepared statements, pipelined `execute_async`,
  LOCAL_ONE.**
- Run it live at `--speed 0` (max). Watch fills/s and writes/s climb.
- **The honest version of the throughput story:** one Python process is
  CPU-bound on feature math at **`<<consumer_fills_per_s>>` fills/s**. That's a
  *client* limit, not a ScyllaDB limit — prove it by scaling the load generator
  out to **`<<loadgen_writes_per_s>>` writes/s** sustained into the same cluster
  (`bench`/`loadgen`, N processes, shard-aware). ScyllaDB had headroom the whole
  time (show Monitoring: load spread evenly across shards).
- **Land it:** the database is not the bottleneck for write-heavy ingestion.

## 4 · Hot-partition avoidance (18:00–22:00) → *Objective 2/4*

- The trap: a few coins carry most of the volume. `PRIMARY KEY ((coin), ...)`
  (`fills_by_coin_hot`) sends every BTC write to the **one** shard that owns
  `token(BTC)` → one hot shard, the rest idle.
- The fix: fold a coarse time bucket into the key — `((coin, time_bucket), ts)`
  (`fills_by_coin_bucket`) — so each coin's writes rotate across partitions and
  spread across the ring, while "recent BTC" reads still touch 1–2 partitions.
- **Demo:** ingest into both tables, show ScyllaDB Monitoring per-shard load:
  hot table = one shard pinned; bucketed table = flat. Quote the max-shard
  imbalance **`<<hot_vs_bucket>>`**.
- **Land it:** partition-key design, not hardware, is what keeps load flat.

## 5 · Low-latency retrieval (22:00–26:00) → *Objective 3*

- The inference fast path: `api.py` point-reads the entity's fresh features and
  the scorer does microseconds of arithmetic. The **feature fetch is the fast
  path** and we measure it.
- Live: hit `/score/coin/BTC` and `/archetype/wallet/<addr>`; show `db_read_ms`
  and `fresh_bucket_ts` (freshness — last write within the active window).
- The benchmark: single-partition point reads, multiprocessing client so the
  *server* is measured. **p99 1.6 ms** at **29k reads/s** (scales to 61k/s). Talk
  in p99 — the tail is the SLA for online inference.
- **The money shot — open the live dashboard and hit ⚡ BURST.** Replay spikes to
  max; the writes/s line jumps (one capture: 5.1k → 11.5k/s, 2.25×) while the read
  p99 line **stays flat** (1.62 → 1.58 ms). Feature retrieval does not degrade when
  ingestion spikes. (Run with `FS_SPEED=10` so the burst is a bigger multiple.)
- **Land it:** fresh features, retrieved in well under a millisecond — even under
  a write burst.

## 6 · Tuning & observability (26:00–29:00) → *Objective 4*

- The one before/after slide. Same workload, two driver profiles:
  - compare **p99** before vs after (the tail is what tuning moves). On a fast
    local cluster the read-path levers don't separate (client-bound) — say so;
    the p99 win shows on the **write path under saturation** and on Cloud.
- Also mention: prepared statements (skip re-parsing), bounded in-flight
  pipelining, LCS for overwrite-heavy point-read tables, TWCS+TTL for windows.
- ScyllaDB Monitoring dashboards: per-shard load, p99 read/write, cache hits,
  compaction. This is how you'd run it in prod.
- **Land it:** the tuning levers are small and the payoff is the tail latency.

## 7 · Close (29:00–30:00)

- Recap the four objectives against the numbers on one slide.
- Tee up Webinar 2: *those same features will also need vector search — and the
  usual answer is a second database. Next time we collapse that into ScyllaDB too.*

---

### Live-demo checklist
- [ ] Cluster up (`just cluster-status` → 3× UN), schema applied (`just schema`).
- [ ] Monitoring up (`just monitoring-up`), Grafana on `:3000`.
- [ ] Pre-warm a consume run so reads hit populated partitions; `sample_keys.csv` exists.
- [ ] API running (`just api`); `/score/coin/BTC` returns < 1 ms `db_read_ms`.
- [ ] Dashboard running (`FS_SPEED=10 just dashboard 10 1`) on `:8090`; ⚡ BURST works.
- [ ] Two benchmark terminals ready (`just bench` tuned vs default) for the before/after reveal.

### Dashboard panels (what each one is for)
- firehose fills/s sparkline → objective 1 (the live pipeline)
- read-tail-vs-write-load chart + ⚡ BURST → objectives 3 & 4 (fast under load)
- feature freshness (write→read, ~900 µs) → objective 3 (the store's freshness floor)
- write-load-by-coin skew bars → objective 2 / section 4 (hot-partition motivation)
- accumulation scoreboard + taker imbalance → the inference, live (read below)
- wallet-archetype mix → per-wallet features are real

### Reading the "UNUSUAL ACCUMULATION" panel (have this ready for Q&A)

Each row is the inference step running live: "is someone unusually accumulating
this coin right now?", scored from the fresh features just point-read out of
ScyllaDB. Row anatomy:

```
 COIN        [····red◄ | ►green····]      0.88     120ms
 ^name        ^taker buy/sell imbalance   ^score   ^freshness
 (colored)    (red=selling, green=buying) (0–1)    (1m bucket age)
```

- **Name colour** = severity: 🔴 `unusual-accumulation` (score ≥ 0.6), 🟡
  `elevated` (0.4–0.6), ⚪ `normal` (< 0.4).
- **Diverging bar** = raw 1m taker imbalance `(taker_buy−taker_sell)/(buy+sell)`,
  −1…+1. Green-right = aggressive buying; red-left = aggressive selling.
- **Score** = blended 0–1; rows sorted by it. **Freshness** = age of the latest
  1m bucket (~0 for active coins → scoring on current data).
- Listed coins are the **busiest ~14 by 1m volume**, then ranked by score.

Score = weighted blend (`src/feature_store/scorer.py`):

| component | weight | meaning |
|---|---|---|
| vol_spike | 0.35 | 1m volume rate vs the 1h baseline (3× → 0.5) — unusually busy? |
| imbalance | 0.30 | `max(0, buy_imbalance)` — **only buying counts** toward accumulation |
| concentration | 0.20 | HHI of wallet flow — a few wallets driving it, not the crowd |
| big_flow | 0.15 | large-wallet net flow vs volume — are whales net buying? |

How to read it on stage:
- 🔴 **high score + long green bar** = textbook: busy + one-sided buying +
  concentrated/whales (e.g. `FARTCOIN 0.88`, imbalance +0.90).
- 🟡 **elevated score + long *red* bar** = the honest gotcha: score came from a
  volume spike + concentration, but the bar shows it's being *sold*, not bought
  (e.g. `HBAR 0.55`, imbalance −0.69).
- ⚪ **BTC/ETH usually grey** = high volume but it's their *normal* volume, flow is
  two-sided and spread across thousands of wallets. "Unusual" is relative to each
  coin's own baseline — that's why a micro-cap lights up before BTC does.

**Say the caveat out loud:** it's a deliberately transparent demo heuristic — the
weights and the 0.6/0.4 thresholds are illustrative, not a calibrated trading
signal. The point isn't the model; it's that the features are fresh and the fetch
is sub-ms — the scorer is microseconds of arithmetic on top of a fast ScyllaDB
read. Tune `scorer.py` to surface the coins you want during the talk.
