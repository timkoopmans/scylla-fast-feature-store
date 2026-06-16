-- ============================================================================
-- Feature store schema for "Powering a fast feature store with ScyllaDB"
--
-- Design principles on display:
--   * one table per feature group ("entity id"), each shaped so the inference
--     point-read touches exactly ONE partition (and usually one row);
--   * clustering by time-window where a history is needed;
--   * TTL + TimeWindowCompactionStrategy so windowed data expires cheaply and
--     reads stay on a small number of recent SSTables;
--   * partition keys chosen to spread load (no single hot partition) even when
--     a handful of coins (BTC/HYPE/ETH) carry most of the volume.
--
-- Running aggregates are computed in the stream consumer and written as blind
-- upserts (last-writer-wins). We deliberately AVOID counter columns / DB-side
-- read-modify-write on the hot path: the consumer already holds authoritative
-- in-memory state, so a blind INSERT is the cheapest possible write and keeps
-- LOCAL_ONE writes idempotent on replay. (A counter variant is shown at the
-- bottom for contrast in the "schema design" section of the talk.)
-- ============================================================================

CREATE KEYSPACE IF NOT EXISTS feature_store
  WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3};

USE feature_store;

-- ----------------------------------------------------------------------------
-- 1) Per (wallet, coin) live features.
--    Partition = wallet  ->  millions of partitions, naturally spread.
--    Inference on a new fill reads exactly one row:
--        SELECT * FROM wallet_coin_features WHERE addr=? AND coin=?;
--    Clustering by coin lets us also fetch a wallet's whole book in one read.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wallet_coin_features (
    addr          text,
    coin          text,
    net_pos       double,      -- signed net position (size units)
    avg_entry     double,      -- size-weighted entry price of the open position
    realized_pnl  double,      -- cumulative realized PnL on this (wallet,coin)
    fill_count    bigint,
    last_ts       timestamp,   -- last fill seen (freshness probe)
    PRIMARY KEY ((addr), coin)
) WITH compaction = {'class': 'IncrementalCompactionStrategy'};
-- ICS: ScyllaDB's incremental compaction — STCS write efficiency without the
-- ~2x space amplification, and bounded read amplification. The right default for
-- this overwrite-heavy, point-read table.

-- ----------------------------------------------------------------------------
-- 2) Per-coin rolling window features (1m / 5m / 1h snapshots).
--    Partition = (coin, window). One row per bucket, clustered newest-first so
--    the inference read is:
--        SELECT * FROM coin_window_features WHERE coin=? AND window=? LIMIT 1;
--    -> single partition, first row = freshest bucket.
--    TTL keeps each partition tiny; TWCS makes expiry a whole-SSTable drop.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS coin_window_features (
    coin              text,
    window            text,       -- '1m' | '5m' | '1h'
    bucket_ts         timestamp,  -- start of the tumbling bucket
    volume            double,     -- notional traded in bucket
    taker_buy         double,     -- taker (crossed) buy notional
    taker_sell        double,     -- taker (crossed) sell notional
    buy_sell_imbalance double,    -- (taker_buy - taker_sell)/(taker_buy + taker_sell)
    active_wallets    int,
    hhi               double,     -- Herfindahl index of net-position concentration
    large_flow        double,     -- net signed notional from large wallets
    PRIMARY KEY ((coin, window), bucket_ts)
) WITH CLUSTERING ORDER BY (bucket_ts DESC)
  AND default_time_to_live = 172800           -- 2 days
  AND compaction = {
        'class': 'TimeWindowCompactionStrategy',
        'compaction_window_unit': 'HOURS',
        'compaction_window_size': 1
      };

-- ----------------------------------------------------------------------------
-- 3) Per-wallet features (one row per wallet).
--    Partition = wallet. Point read:
--        SELECT * FROM wallet_features WHERE addr=?;
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wallet_features (
    addr              text PRIMARY KEY,
    cum_realized_pnl  double,
    total_fills       bigint,
    gross_volume      double,     -- sum |sz*px|
    net_volume        double,     -- |sum signed sz*px|  (directionality)
    churn             double,     -- closes / (opens+closes)
    archetype         text,       -- 'market-maker' | 'directional' | 'mixed'
    last_ts           timestamp
) WITH compaction = {'class': 'IncrementalCompactionStrategy'};  -- ICS (see above)

-- ----------------------------------------------------------------------------
-- 4) Raw-fill sink used ONLY to demonstrate hot-partition avoidance (section 3).
--
--    Anti-pattern (do NOT do this): PRIMARY KEY ((coin), ts, addr)
--      -> the BTC partition is unbounded and every BTC write hammers the three
--         replicas that own token(BTC). One coin = one hot shard.
--
--    Fix below: fold a coarse time bucket into the partition key so each coin's
--    writes rotate across a new partition every COIN_BUCKET_SECONDS, spreading
--    load across the ring while reads for "recent BTC" still hit 1-2 partitions.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fills_by_coin_bucket (
    coin         text,
    time_bucket  timestamp,   -- floor(ts / COIN_BUCKET_SECONDS)
    ts           timestamp,
    addr         text,
    px           double,
    sz           double,
    side         text,
    crossed      boolean,
    closed_pnl   double,
    PRIMARY KEY ((coin, time_bucket), ts, addr)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND default_time_to_live = 86400            -- 1 day
  AND compaction = {
        'class': 'TimeWindowCompactionStrategy',
        'compaction_window_unit': 'HOURS',
        'compaction_window_size': 1
      };

-- ----------------------------------------------------------------------------
-- 4b) The anti-pattern table, created so the talk can show the contrast in
--     ScyllaDB Monitoring (per-shard load). Same data, coin-only partition key.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fills_by_coin_hot (
    coin         text,
    ts           timestamp,
    addr         text,
    px           double,
    sz           double,
    side         text,
    crossed      boolean,
    closed_pnl   double,
    PRIMARY KEY ((coin), ts, addr)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND default_time_to_live = 86400
  AND compaction = {
        'class': 'TimeWindowCompactionStrategy',
        'compaction_window_unit': 'HOURS',
        'compaction_window_size': 1
      };

-- ----------------------------------------------------------------------------
-- 5) (Contrast only — not on the hot path) counter-based running aggregate.
--    Demonstrates why we prefer compute-in-stream + blind upsert: counters
--    force a read-before-write on the replica and are not idempotent on replay.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS coin_counters (
    coin         text,
    window       text,
    bucket_ts    timestamp,
    fills        counter,
    PRIMARY KEY ((coin, window), bucket_ts)
) WITH CLUSTERING ORDER BY (bucket_ts DESC);
