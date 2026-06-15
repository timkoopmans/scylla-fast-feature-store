---
marp: true
paginate: true
theme: uncover
class: invert
title: Powering a fast feature store with ScyllaDB
description: Webinar 1 — real-time feature store on a live exchange firehose
---

<!--
This is a Marp deck. Render it with:
  npx @marp-team/marp-cli@latest 01-fast-feature-store.deck.md -o 01.html   # or .pdf / .pptx
  npx @marp-team/marp-cli@latest -s .                                       # live preview server
Presenter notes live in HTML comments like this one (visible in Marp presenter view).
Numbers come from ../docs/RESULTS.md — keep them in sync.
Companion long-form notes: 01-fast-feature-store.md
-->

<style>
:root {
  --c-bg: #0c0f14; --c-fg: #dfe6f0; --c-accent: #7fd1ff;
  --c-green: #2bd47a; --c-red: #ff6b6b; --c-amber: #ffd166;
}
section {
  background: var(--c-bg); color: var(--c-fg);
  font-family: -apple-system, "Segoe UI", Roboto, sans-serif;
  font-size: 30px; text-align: left;
}
h1, h2 { color: var(--c-accent); }
strong { color: #fff; }
code { background: #141a23; color: var(--c-green); }
section.lead { text-align: center; }
table { font-size: 24px; }
.big { font-size: 64px; color: #fff; font-weight: 800; }
.green { color: var(--c-green); } .red { color: var(--c-red); } .amber { color: var(--c-amber); }
.small { font-size: 20px; color: #8aa; }
</style>

<!-- _class: invert lead -->

![w:90](assets/scylladb-monster.svg)

# ⚡ Powering a fast feature store

### Fresh features from a live exchange firehose, served in **p99 < 2 ms**

<span class="small">Webinar 1 of 3 · real-time AI infrastructure on ScyllaDB</span>

<!--
Cold open. Don't sell yet — show. Have the live dashboard up behind you:
firehose pouring in, scoreboard moving, p99 read number sitting near 1.5 ms.
One sentence: "Every trade on a live exchange, turned into fresh features and
scored in real time — and the only thing between a new fill and a trading
signal is one ScyllaDB read. Let's see how fast that read is."
-->

---

![w:56](assets/hyperliquid-logo.png)

## The firehose

- Hyperliquid `node_fills` — **every trade fill** on the exchange
- **~239M+ fills, 45 days**, ~60/s average, **bursting to thousands/s**
- Replayed in timestamp order at adjustable speed → a live stream

<span class="small">A real exchange: bursty, skewed to a few hot symbols, millions of entities.</span>

<!--
Set the stage. This is real data, replayed as a firehose (replay.py). The bursts
and the skew are the point — they're what break naive designs.
-->

---

## Why this dataset is an *honest* stress test

- **Bursty** — spikes like a flash sale / viral event / fraud burst
- **Skewed** — BTC, HYPE, ETH dominate; long tail barely trades → **hot partitions**
- **High cardinality** — millions of wallets × hundreds of coins
- **Real semantics** — price, size, side, taker flag, realized PnL → *real signals*

> If it survives every trade on a live exchange, it survives your recommender / fraud / ranking load.

<!--
The framing for the whole series: fresh, fast-retrieved features drive real
inference. Synthetic even traffic is the workload a DB finds easy; this isn't.
-->

---

<!-- _class: invert lead -->

## Four learning objectives

1. Build a **real-time AI pipeline**
2. **High write throughput**
3. **Low-latency retrieval** of fresh features
4. **Tuning** for max performance

<span class="small">Every section lands one of these.</span>

---

## What a feature store has to do

```
 fills firehose      incremental         pipelined         point reads
 (node_fills)   ──►  feature state  ──►  blind upserts ──► (inference
   replay.py         features.py         writer.py          fast path)
                                            │
                                       ScyllaDB ◄── schema.cql
```

- Features computed **from a stream**, stored hot, **point-read at inference**
- The SLA is **freshness** + **tail latency**

<!--
Objective 1. The store is the contract between the streaming half and the
serving half. Walk the boxes; each maps to a module in the repo.
-->

---

## The features we maintain

- **per (wallet, coin):** net position, avg entry, realized PnL, fills, last-seen
- **per coin · 1m/5m/1h:** volume, taker buy/sell imbalance, active wallets,
  concentration (HHI), large-wallet net flow
- **per wallet:** cumulative PnL, trade frequency, **archetype** (market-maker vs directional)

<span class="small">Each ties to a real trading question — not toy aggregates.</span>

---

## Schema design — one partition per read

```sql
CREATE TABLE wallet_coin_features (
  addr text, coin text,
  net_pos double, avg_entry double, realized_pnl double,
  fill_count bigint, last_ts timestamp,
  PRIMARY KEY ((addr), coin)            -- partition = wallet
);
-- inference read = ONE row in ONE partition:
SELECT * FROM wallet_coin_features WHERE addr=? AND coin=?;
```

- Partition = wallet → **millions of partitions, naturally spread**

<!--
Objective 3 setup. The single rule: shape every table so the inference read hits
exactly one partition. Millions of wallets = no hot spot here.
-->

---

## Schema — rolling windows, cheap expiry

```sql
CREATE TABLE coin_window_features (
  coin text, window text, bucket_ts timestamp, ...
  PRIMARY KEY ((coin, window), bucket_ts)
) WITH CLUSTERING ORDER BY (bucket_ts DESC)
  AND default_time_to_live = 172800
  AND compaction = {'class':'TimeWindowCompactionStrategy', ...};

SELECT * FROM coin_window_features WHERE coin=? AND window=? LIMIT 1;  -- freshest
```

- **TTL + TWCS** → expiry is a whole-SSTable drop; reads stay on a few SSTables

---

## Running aggregates: compute-in-stream, **not** counters

- The consumer already holds authoritative state in memory
- So a **blind `INSERT`** is the cheapest write — idempotent on replay, LOCAL_ONE-safe
- Counters force a **read-before-write** on the replica → avoid on the hot path

<span class="small">We show the counter table only as the contrast.</span>

<!--
Common question: "why not counters?" Answer here. This is also why writes are so
cheap — no coordination, no RMW.
-->

---

## High-write ingestion

- Per fill: update in-memory state → **blind upsert** the feature
- **Prepared statements · pipelined `execute_async` · shard-aware · LOCAL_ONE**
- Window snapshots flush on bucket close + periodically; wallet features coalesced

<!--
Objective 2. Run consume live at --speed 0. The honest framing on the next slide.
-->

---

## Write throughput — the database is not the bottleneck

<div class="big green">108,302 writes/s</div>

sustained · 0 errors · 3-node dev cluster

- single Python feature process: ~17k fills/s (a **client/GIL** limit)
- ScyllaDB had headroom throughout — scales with shards
- path to **>1M ops/s** = cluster sizing + a native driver, on the **real** data

<!--
Be honest: 17k is the Python consumer (feature math, GIL). 108k is the loadgen
(12 procs). Neither is Scylla's ceiling. No synthetic data needed — 385M fills,
>1B write-ops with fan-out.
-->

---

## Hot-partition avoidance

```sql
-- ❌ anti-pattern: every BTC write hammers ONE shard
PRIMARY KEY ((coin), ts, addr)

-- ✅ fold a time bucket into the key → writes rotate across the ring
PRIMARY KEY ((coin, time_bucket), ts, addr)
```

- A few coins carry most volume → coin-only key = **one hot shard**
- Bucketed key spreads load; "recent BTC" reads still touch 1–2 partitions

<!--
Objective 2/4. Show the dashboard "write load by coin" bars — BTC/ETH/HYPE
dwarf the tail. That skew is exactly what makes partition-key design matter.
In Monitoring: hot table = one shard pinned; bucketed = flat.
-->

---

## Low-latency retrieval — the fast path

<div class="big green">p99 &lt; 2 ms</div>

point reads · ~29k reads/s (scales to 61k/s)

- the scorer is microseconds of arithmetic on top of **one ScyllaDB read**
- **feature freshness (write→read) ≈ 900 µs** — a computed feature is readable in
  hundreds of microseconds (LOCAL_ONE, no quorum wait)
- `/score/coin/BTC` returns `db_read_ms` live

<!--
Objective 3. p99 is the number we talk in — the tail is the SLA. Hit the API
live; show db_read_ms under 1 ms for a single-row read.
-->

---

## The money shot — reads stay fast under a write burst

- Hit **⚡ BURST**: writes **5.1k → 11.5k/s (2.25×)**
- read **p99 stays flat: 1.62 → 1.58 ms**

> Feature retrieval does **not** degrade when ingestion spikes.

<span class="small">Run with `FS_SPEED=10` so the burst is a bigger multiple on screen.</span>

<!--
This is the slide to linger on. Trigger the burst live on the dashboard; the
writes line jumps, the p99 line doesn't move. Objectives 3 + 4 in one gesture.
-->

---

## Tuning & observability

- Compare **p99** before/after — the tail is what tuning moves
- Levers: prepared statements, shard/token-aware routing, bounded pipelining,
  LCS for point-read tables, TWCS+TTL for windows
- **ScyllaDB Monitoring**: per-shard load, p99 read/write, cache hits, compaction

<span class="small">Honest note: on a fast local cluster the read-path levers are client-bound — the p99 win shows on the write path under saturation and on Cloud.</span>

<!--
Objective 4. Don't overclaim the local before/after; the teaching point is real.
-->

---

<!-- _class: invert lead -->

## Recap

| objective | result |
|---|---|
| real-time pipeline | firehose → fresh features → inference |
| high write throughput | **108k writes/s**, 0 errors |
| low-latency retrieval | **p99 < 2 ms** point reads |
| tuning | p99 holds under burst; levers shown |

---

<!-- _class: invert lead -->

## Next: collapse the stack

Those same features will also need **vector search**.

The usual answer is a *second database* + a sync pipeline.

### Webinar 2 — we collapse that into ScyllaDB too.

![w:90](assets/scylladb-monster.svg)

<span class="small">Thanks — questions?</span>

<!--
Tee up the series. Webinar 2 (Aug): feature store + vector search in one engine.
Webinar 3 (Sep): slashing the cost. Then take questions — have the "reading the
accumulation panel" notes ready.
-->
