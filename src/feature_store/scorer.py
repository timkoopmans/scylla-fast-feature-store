"""Lightweight inference: score a coin for 'unusual accumulation', tag a wallet.

These are deliberately simple, transparent functions over the *fresh features*
read from ScyllaDB. The point of the demo is that the feature fetch is the fast
path; the scorer itself is microseconds of arithmetic.
"""
from __future__ import annotations

import math


def _rate(window_row, seconds):
    if not window_row:
        return 0.0
    return (window_row.get("volume") or 0.0) / seconds


def unusual_accumulation(w1m, w5m, w1h) -> dict:
    """Score in [0,1] that a coin is seeing unusual one-sided accumulation.

    Components (each squashed to [0,1]):
      * vol_spike  : short-term volume rate vs the 1h baseline
      * imbalance  : taker buy/sell imbalance, biased to the buy side
      * concentr.  : HHI -> a few wallets driving flow
      * big_flow   : positive large-wallet net flow relative to volume
    """
    base_rate = _rate(w1h, 3600) or 1e-9
    short_rate = _rate(w1m, 60)
    vol_spike = _sig(short_rate / base_rate, k=1.0, mid=3.0)  # 3x baseline -> 0.5

    imb = (w1m or {}).get("buy_sell_imbalance", 0.0)
    imbalance = max(0.0, imb)  # only buy-side imbalance counts as accumulation

    hhi = (w1m or {}).get("hhi", 0.0)
    concentration = min(1.0, hhi / 0.2)  # HHI 0.2 (~5 equal wallets) -> 1.0

    vol_1m = (w1m or {}).get("volume", 0.0) or 1e-9
    big = (w1m or {}).get("large_flow", 0.0)
    big_flow = max(0.0, min(1.0, big / vol_1m))

    score = (
        0.35 * vol_spike
        + 0.30 * imbalance
        + 0.20 * concentration
        + 0.15 * big_flow
    )
    return {
        "score": round(score, 4),
        "components": {
            "vol_spike": round(vol_spike, 4),
            "imbalance": round(imbalance, 4),
            "concentration": round(concentration, 4),
            "big_flow": round(big_flow, 4),
        },
        "label": "unusual-accumulation" if score >= 0.6 else
                 "elevated" if score >= 0.4 else "normal",
    }


def wallet_archetype(wallet_row) -> dict:
    if not wallet_row:
        return {"archetype": "unknown"}
    return {
        "archetype": wallet_row.get("archetype", "mixed"),
        "churn": round(wallet_row.get("churn") or 0.0, 4),
        "cum_realized_pnl": wallet_row.get("cum_realized_pnl"),
        "total_fills": wallet_row.get("total_fills"),
    }


def _sig(x, k=1.0, mid=1.0):
    """Logistic squash centred at `mid`."""
    try:
        return 1.0 / (1.0 + math.exp(-k * (x - mid)))
    except OverflowError:
        return 0.0 if x < mid else 1.0
