"""Hand-crafted behaviour embeddings (webinar 2).

No model, no training run — every dimension is a normalized feature the
audience already met in webinar 1, so a neighbour list is *explainable* on
stage ("these two wallets are close because same directionality, same size
class, same hours"). Learned sequence embeddings are the upgrade path.

One vector:
  * wallet_vector : 16-dim behavioural fingerprint of a wallet

All dimensions are squashed into [0, 1] (log-scale where values span orders
of magnitude) and compared with COSINE similarity — see cql/schema_vector.cql.
Dimensions here MUST match the vector<float, N> declarations there.
"""
from __future__ import annotations

import math

WALLET_DIM = 16

# stage-friendly names, index-aligned with wallet_vector()
WALLET_DIM_NAMES = [
    "directionality",      # |net| / gross volume
    "maker_share",         # closes / (opens + closes)
    "taker_buy_bias",      # taker buy / taker (buy + sell)
    "activity",            # log fills
    "avg_trade_size",      # log gross / fills
    "coin_diversity",      # entropy of per-coin gross
    "top_coin_share",      # concentration on favourite coin
    "pnl_profile",         # signed log realized PnL
    "is_market_maker",
    "is_directional",
    "is_mixed",
    "whale_scale",         # log gross volume
    "hours_00_06",
    "hours_06_12",
    "hours_12_18",
    "hours_18_24",
]


def _log01(x: float, cap: float) -> float:
    """log1p-squash x >= 0 into [0, 1], saturating at cap."""
    if x <= 0:
        return 0.0
    return min(math.log1p(x) / math.log1p(cap), 1.0)


def _signed_log01(x: float, cap: float) -> float:
    """log1p-squash a signed value into [0, 1] with 0.5 = neutral."""
    s = math.copysign(_log01(abs(x), cap), x)
    return 0.5 * (s + 1.0)


def wallet_vector(w) -> list[float]:
    """Behavioural fingerprint of a WalletState. Similar traders land close:
    market-makers cluster away from directional whales; coordinated rings
    (near-identical bots) collapse onto almost the same point."""
    v = [0.0] * WALLET_DIM
    gross = w.gross_volume
    v[0] = abs(w.signed_volume) / gross if gross > 0 else 0.0
    v[1] = w.churn
    taker = w.taker_buy_vol + w.taker_sell_vol
    v[2] = w.taker_buy_vol / taker if taker > 0 else 0.5
    v[3] = _log01(w.total_fills, 1e6)
    v[4] = _log01(gross / w.total_fills if w.total_fills else 0.0, 1e6)
    shares = [g / gross for g in w.coin_gross.values() if g > 0] if gross > 0 else []
    if len(shares) > 1:
        v[5] = -sum(s * math.log(s) for s in shares) / math.log(len(shares))
    v[6] = max(shares) if shares else 0.0
    v[7] = _signed_log01(w.cum_realized_pnl, 1e6)
    v[{"market-maker": 8, "directional": 9, "mixed": 10}[w.archetype]] = 1.0
    v[11] = _log01(gross, 1e9)
    total_h = sum(w.hours)
    if total_h:
        for i in range(4):
            v[12 + i] = sum(w.hours[i * 6:(i + 1) * 6]) / total_h
    return v


def cosine(a, b) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na > 0 and nb > 0 else 0.0


def explain(seed: list[float], other: list[float], top: int = 3):
    """Top contributing/diverging dims between two wallet vectors — powers the
    'why are these similar' beat without hand-waving."""
    diffs = sorted(
        ((abs(s - o), name, s, o) for s, o, name in zip(seed, other, WALLET_DIM_NAMES)),
        key=lambda t: t[0],
    )
    closest = [{"dim": n, "seed": round(s, 3), "other": round(o, 3)} for _, n, s, o in diffs[:top]]
    furthest = [{"dim": n, "seed": round(s, 3), "other": round(o, 3)} for _, n, s, o in diffs[-top:]]
    return {"most_alike": closest, "most_different": furthest}
