"""Fills replayer.

Treats the 45-day decoded Hyperliquid node_fills sample as a live firehose by
streaming fills in timestamp order at an adjustable speed:

    speed = 1.0   -> wall-clock real time (honour inter-arrival gaps)
    speed = 100   -> 100x faster than real time
    speed = 0     -> MAX: no sleeping, as fast as the disk/CPU allow (for the
                     write-throughput benchmark)

A fill is a lightweight tuple to keep per-event allocation cheap on the hot path.
"""
from __future__ import annotations

import glob
import os
import time
from dataclasses import dataclass
from typing import Iterator

import polars as pl

# Daily parquet files of decoded fills. Override with FS_DATA_GLOB; otherwise
# look for the staged copy under <repo>/data/fills (gitignored — see README for
# how to fetch the dataset).
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_DEFAULT_LOCAL = os.path.join(_REPO_ROOT, "data", "fills", "*.parquet")
DATA_GLOB = os.environ.get("FS_DATA_GLOB") or _DEFAULT_LOCAL

COLUMNS = ["addr", "coin", "px", "sz", "side", "time", "closedPnl", "crossed"]


@dataclass(slots=True)
class Fill:
    addr: str
    coin: str
    px: float
    sz: float
    side: str        # 'B' buy / 'A' sell
    ts_ms: int
    closed_pnl: float
    crossed: bool    # True => this address was the taker (aggressor)

    @property
    def is_buy(self) -> bool:
        return self.side == "B"

    @property
    def signed_sz(self) -> float:
        return self.sz if self.side == "B" else -self.sz

    @property
    def notional(self) -> float:
        return self.px * self.sz


def day_files(limit_days: int | None = None) -> list[str]:
    files = sorted(glob.glob(DATA_GLOB))
    return files[:limit_days] if limit_days else files


def _iter_day(path: str) -> Iterator[tuple]:
    """Yield rows of one day file, sorted by time. Returns raw tuples (fast)."""
    df = pl.read_parquet(path, columns=COLUMNS).sort("time")
    # iter_rows over the selected columns in COLUMNS order.
    yield from df.iter_rows()


def iter_fills(limit_days: int | None = None, max_fills: int | None = None) -> Iterator[Fill]:
    """Iterate fills across day files in global timestamp order.

    Day files are already time-partitioned and disjoint, so concatenating
    per-day sorted streams preserves global order.
    """
    n = 0
    for path in day_files(limit_days):
        for row in _iter_day(path):
            addr, coin, px, sz, side, ts, cpnl, crossed = row
            yield Fill(addr, coin, px, sz, side, ts, cpnl or 0.0, bool(crossed))
            n += 1
            if max_fills and n >= max_fills:
                return


def replay(
    speed: float = 0.0,
    limit_days: int | None = None,
    max_fills: int | None = None,
) -> Iterator[Fill]:
    """Yield fills paced by `speed`. speed<=0 means max speed (no sleeping)."""
    if speed <= 0:
        yield from iter_fills(limit_days, max_fills)
        return

    wall_start = time.monotonic()
    data_start: int | None = None
    for fill in iter_fills(limit_days, max_fills):
        if data_start is None:
            data_start = fill.ts_ms
        # target wall-clock offset for this event
        target = (fill.ts_ms - data_start) / 1000.0 / speed
        now = time.monotonic() - wall_start
        if target > now:
            time.sleep(target - now)
        yield fill
