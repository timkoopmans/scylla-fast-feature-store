"""Incremental feature computation.

The consumer holds authoritative state in memory and writes blind upserts to
ScyllaDB. This is the standard streaming-feature-store pattern and the reason
writes are cheap: no read-before-write, idempotent on replay, LOCAL_ONE-safe.

Three feature groups, matching the schema:
  * WalletCoinState      -> wallet_coin_features
  * CoinWindowAggregator -> coin_window_features (tumbling 1m/5m/1h buckets)
  * WalletState          -> wallet_features
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .replay import Fill

WINDOWS = {"1m": 60, "5m": 300, "1h": 3600}
LARGE_NOTIONAL = 100_000.0   # a "large wallet" move in a bucket, USD notional


def wallet_weight(ws) -> float:
    """Smart-money weight for a wallet: profitable, directional wallets count
    most; market-makers (churn, no net view) count ~0. Used to turn raw per-coin
    flow into 'smart-money flow' — the signal-generation join."""
    if ws is None:
        return 0.0
    arch_w = {"directional": 1.0, "mixed": 0.5}.get(ws.archetype, 0.0)
    pnl_w = 1.0 if ws.cum_realized_pnl > 0 else 0.25   # follow the ones making money
    return arch_w * pnl_w


# ---------------------------------------------------------------------------
# 1) per (wallet, coin)
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class WalletCoinState:
    net_pos: float = 0.0
    avg_entry: float = 0.0
    realized_pnl: float = 0.0
    fill_count: int = 0
    last_ts: int = 0

    def update(self, f: Fill) -> None:
        delta = f.signed_sz
        new_pos = self.net_pos + delta
        # Size-weighted average entry: only re-weight when the position grows in
        # its current direction; a reduction/flip leaves avg_entry of remainder.
        same_dir = (self.net_pos >= 0 and delta > 0) or (self.net_pos <= 0 and delta < 0)
        if self.net_pos == 0 or same_dir:
            denom = abs(new_pos)
            if denom > 0:
                self.avg_entry = (
                    abs(self.net_pos) * self.avg_entry + abs(delta) * f.px
                ) / denom
        elif new_pos != 0 and (new_pos > 0) != (self.net_pos > 0):
            # flipped through zero -> new leg opens at this fill's price
            self.avg_entry = f.px
        self.net_pos = new_pos
        self.realized_pnl += f.closed_pnl   # exchange-provided per-fill realized PnL
        self.fill_count += 1
        self.last_ts = f.ts_ms


# ---------------------------------------------------------------------------
# 3) per wallet
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class WalletState:
    cum_realized_pnl: float = 0.0
    total_fills: int = 0
    gross_volume: float = 0.0
    signed_volume: float = 0.0
    opens: int = 0
    closes: int = 0
    last_ts: int = 0

    def update(self, f: Fill) -> None:
        notional = f.notional
        self.cum_realized_pnl += f.closed_pnl
        self.total_fills += 1
        self.gross_volume += notional
        self.signed_volume += notional if f.is_buy else -notional
        if f.crossed:
            self.opens += 1      # taker = aggressive entry/exit
        else:
            self.closes += 1     # maker = passive / providing liquidity
        self.last_ts = f.ts_ms

    @property
    def churn(self) -> float:
        tot = self.opens + self.closes
        return self.closes / tot if tot else 0.0

    @property
    def archetype(self) -> str:
        """market-maker vs directional, from net/gross ratio and maker share.

        Market makers churn both sides (low |net|/gross, high maker share);
        directional traders push net exposure (high |net|/gross).
        """
        if self.gross_volume <= 0:
            return "mixed"
        net_gross = abs(self.signed_volume) / self.gross_volume
        maker_share = self.closes / (self.opens + self.closes or 1)
        if net_gross < 0.15 and maker_share > 0.6:
            return "market-maker"
        if net_gross > 0.4:
            return "directional"
        return "mixed"


# ---------------------------------------------------------------------------
# 2) per-coin rolling windows (tumbling buckets)
# ---------------------------------------------------------------------------
@dataclass
class Bucket:
    bucket_ts: int
    volume: float = 0.0
    taker_buy: float = 0.0
    taker_sell: float = 0.0
    wallet_flow: dict = field(default_factory=dict)   # addr -> signed notional

    def add(self, f: Fill) -> None:
        n = f.notional
        self.volume += n
        if f.crossed:
            if f.is_buy:
                self.taker_buy += n
            else:
                self.taker_sell += n
        self.wallet_flow[f.addr] = self.wallet_flow.get(f.addr, 0.0) + (
            n if f.is_buy else -n
        )

    def snapshot(self, wallets: dict | None = None) -> dict:
        tb, ts_ = self.taker_buy, self.taker_sell
        imb = (tb - ts_) / (tb + ts_) if (tb + ts_) > 0 else 0.0
        flows = self.wallet_flow
        gross = sum(abs(v) for v in flows.values())
        hhi = sum((abs(v) / gross) ** 2 for v in flows.values()) if gross > 0 else 0.0
        large_flow = sum(v for v in flows.values() if abs(v) >= LARGE_NOTIONAL)
        # smart-money flow: net notional weighted by each wallet's "smartness"
        # (profitable + directional count most; market-makers ~0). This is the
        # per-coin × per-wallet join that turns raw flow into a trade signal.
        smart_flow = 0.0
        if wallets is not None:
            smart_flow = sum(v * wallet_weight(wallets.get(a)) for a, v in flows.items())
        return {
            "bucket_ts": self.bucket_ts,
            "volume": self.volume,
            "taker_buy": tb,
            "taker_sell": ts_,
            "buy_sell_imbalance": imb,
            "active_wallets": len(flows),
            "hhi": hhi,
            "large_flow": large_flow,
            "smart_flow": smart_flow,
        }


class CoinWindowAggregator:
    """Maintains tumbling buckets per (coin, window).

    A fill advancing past a bucket boundary closes the previous bucket (yielded
    for a final write). Open buckets can be snapshotted at any time for freshness.
    """

    def __init__(self) -> None:
        # (coin, window) -> Bucket
        self.open: dict[tuple[str, str], Bucket] = {}

    def update(self, f: Fill, wallets: dict | None = None):
        """Return list of (coin, window, snapshot) for buckets that just closed."""
        closed = []
        sec = f.ts_ms // 1000
        for win, size in WINDOWS.items():
            b_ts = (sec // size) * size
            key = (f.coin, win)
            cur = self.open.get(key)
            if cur is None:
                self.open[key] = Bucket(b_ts)
            elif cur.bucket_ts != b_ts:
                if b_ts > cur.bucket_ts:           # rolled forward -> close old
                    closed.append((f.coin, win, cur.snapshot(wallets)))
                    self.open[key] = Bucket(b_ts)
                else:
                    continue                        # late event for a past bucket
            self.open[key].add(f)
        return closed

    def open_snapshots(self, wallets: dict | None = None):
        """Yield (coin, window, snapshot) for all currently-open buckets."""
        for (coin, win), b in self.open.items():
            yield coin, win, b.snapshot(wallets)


class FeatureEngine:
    """Top-level state holder tying the three groups together."""

    def __init__(self) -> None:
        self.wc: dict[tuple[str, str], WalletCoinState] = {}
        self.wallets: dict[str, WalletState] = {}
        self.coins = CoinWindowAggregator()

    def apply(self, f: Fill):
        wc = self.wc.get((f.addr, f.coin))
        if wc is None:
            wc = self.wc[(f.addr, f.coin)] = WalletCoinState()
        wc.update(f)

        w = self.wallets.get(f.addr)
        if w is None:
            w = self.wallets[f.addr] = WalletState()
        w.update(f)

        closed = self.coins.update(f, self.wallets)
        return wc, w, closed

    def open_snapshots(self):
        """Open window snapshots, smart-flow-weighted by current wallet state."""
        return self.coins.open_snapshots(self.wallets)
