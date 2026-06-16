"""Prepared CQL statements (centralised so writer, consumer, api share them)."""
from __future__ import annotations

UPSERT_WALLET_COIN = """
INSERT INTO wallet_coin_features (addr, coin, net_pos, avg_entry, realized_pnl, fill_count, last_ts)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

UPSERT_COIN_WINDOW = """
INSERT INTO coin_window_features
  (coin, window, bucket_ts, volume, taker_buy, taker_sell, buy_sell_imbalance,
   active_wallets, hhi, large_flow, smart_flow)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

UPSERT_WALLET = """
INSERT INTO wallet_features
  (addr, cum_realized_pnl, total_fills, gross_volume, net_volume, churn, archetype, last_ts)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_FILL_BUCKET = """
INSERT INTO fills_by_coin_bucket (coin, time_bucket, ts, addr, px, sz, side, crossed, closed_pnl)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_FILL_HOT = """
INSERT INTO fills_by_coin_hot (coin, ts, addr, px, sz, side, crossed, closed_pnl)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

# point reads (inference fast path)
READ_WALLET_COIN = "SELECT * FROM wallet_coin_features WHERE addr=? AND coin=?"
READ_COIN_WINDOW_LATEST = (
    "SELECT * FROM coin_window_features WHERE coin=? AND window=? LIMIT 1"
)
READ_WALLET = "SELECT * FROM wallet_features WHERE addr=?"


def prepare_all(session) -> dict:
    return {
        "wallet_coin": session.prepare(UPSERT_WALLET_COIN),
        "coin_window": session.prepare(UPSERT_COIN_WINDOW),
        "wallet": session.prepare(UPSERT_WALLET),
        "fill_bucket": session.prepare(INSERT_FILL_BUCKET),
        "fill_hot": session.prepare(INSERT_FILL_HOT),
        "read_wallet_coin": session.prepare(READ_WALLET_COIN),
        "read_coin_window": session.prepare(READ_COIN_WINDOW_LATEST),
        "read_wallet": session.prepare(READ_WALLET),
    }
