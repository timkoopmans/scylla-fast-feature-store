# Why this dataset

**A live exchange firehose is an honest stress test for a feature store.**

Most feature-store demos run on synthetic, evenly-distributed traffic — which is
exactly the workload a database finds easy. Real online-ML traffic is none of
those things, and the Hyperliquid `node_fills` stream reproduces every hard part:

- **Genuinely bursty.** ~60 fills/second on average, bursting to thousands/second
  when the market moves. A feature store has to absorb the spike without dropping
  freshness — the same shape as a flash sale, a viral event, or a fraud burst.
- **Heavily skewed.** A handful of symbols (BTC, HYPE, ETH) carry most of the
  volume while a long tail of micro-caps barely trade. That skew is what creates
  **hot partitions** if you design keys naively — so the dataset forces the
  partition-key lesson rather than letting us dodge it.
- **High cardinality.** Millions of wallets × hundreds of coins = millions of
  feature entities, which is what makes single-partition point reads matter.
- **Real semantics.** Each fill carries price, size, side, taker/maker flag,
  direction, and exchange-computed realized PnL — so the features we derive
  (positions, PnL, taker imbalance, concentration, archetypes) are *real trading
  signals*, not toy aggregates. The inference step ("unusual accumulation",
  wallet archetype) is something you'd actually want to compute live.

That last point is the framing for the whole series: **fresh, fast-retrieved
features drive real trading-signal inference.** If a feature store can keep up
with every trade on a live exchange and still answer "score this coin now" in
under a millisecond, it can keep up with your recommender, your fraud model, or
your ranking pipeline.

## The data

- Source: Hyperliquid `node_fills` — every trade fill on the exchange. Raw form
  is hourly lz4 ndjson of `{block_number, events:[[address, {coin,px,sz,side,
  time,dir,closedPnl,fee,oid}]]}`.
- Decoded to **daily parquet** (one file per day) for the demo. ~8.4M fills/day,
  **46 days (2025-11-23 → 2026-01-08), ~385M fills, 5.3 GB**.
- Columns used: `addr, coin, px, sz, side (B/A), time (ms), closedPnl, crossed
  (taker flag)`; also present: `dir, fee, startPosition, block`.

Replayed in timestamp order at adjustable speed (`--speed`), so it behaves like a
live firehose: `--speed 1` is real time, `--speed 0` is max throughput.

> No synthetic padding is used or needed. At 1M ops/s you would consume ~7 days
> of fills in a single sustained minute; we have 46 days, and each fill fans out
> to several feature writes (>1B write-ops available). Throughput ceilings are a
> function of cluster sizing and the load driver, not the data.

Dataset is staged under `data/` (gitignored) and will be published via object
storage; point the demo at it with `FS_DATA_GLOB` if it lives elsewhere.
