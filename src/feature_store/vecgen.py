"""Bulk behaviour-embedding populate (webinar 2).

The consumer computes features by iterating fills one at a time in Python —
right for a streaming demo, far too slow to backfill the whole dataset (385M
fills). But a wallet's embedding only depends on per-wallet AGGREGATES, and
those are a vectorized group-by. So: aggregate every day file with polars, then
build one vector per wallet and upsert it.

Same `wallet_vector()` the streaming path uses — the vectors this writes are
identical to what the consumer would eventually produce, not an approximation.

    python -m feature_store.vecgen --days 46 --profile cloud

Use it to give the ANN index a realistic population (~500-600k wallets over the
full 46 days) before demoing similarity search.
"""
from __future__ import annotations

import argparse
import math
import os
import time

from .config import KEYSPACE, make_cluster
from .embeddings import wallet_vector
from .features import WalletState
from .statements import prepare_vector
from .writer import Pipeline


def _aggregate(paths: list[str]):
    """One lazy pass over the day files -> per-wallet aggregates.

    Two group-bys: one per (addr, coin) for the diversity/concentration dims,
    one per addr for everything else. Streaming so the whole dataset never has
    to fit in memory.
    """
    import polars as pl

    notional = pl.col("px") * pl.col("sz")
    is_buy = pl.col("side") == "B"
    taker = pl.col("crossed")

    lf = pl.scan_parquet(paths).with_columns(
        notional.alias("notional"),
        ((pl.col("time") // 3_600_000) % 24).alias("hour"),
    )

    per_wallet = lf.group_by("addr").agg(
        pl.len().alias("total_fills"),
        pl.col("notional").sum().alias("gross_volume"),
        pl.when(is_buy).then(pl.col("notional")).otherwise(-pl.col("notional"))
          .sum().alias("signed_volume"),
        pl.col("closedPnl").sum().alias("cum_realized_pnl"),
        pl.when(taker & is_buy).then(pl.col("notional")).otherwise(0.0)
          .sum().alias("taker_buy_vol"),
        pl.when(taker & ~is_buy).then(pl.col("notional")).otherwise(0.0)
          .sum().alias("taker_sell_vol"),
        taker.sum().alias("opens"),
        (~taker).sum().alias("closes"),
        pl.col("time").max().alias("last_ts"),
        *[((pl.col("hour") == h).sum()).alias(f"h{h}") for h in range(24)],
    )

    # per-coin gross per wallet -> collapsed to one list column per wallet
    per_coin = (
        lf.group_by(["addr", "coin"]).agg(pl.col("notional").sum().alias("g"))
          .group_by("addr").agg(pl.col("g").alias("coin_gross"))
    )

    return per_wallet.join(per_coin, on="addr", how="left").collect(engine="streaming")


def run(args):
    from .replay import day_files

    paths = day_files(args.days)
    if not paths:
        raise SystemExit("no parquet day files found (see FS_DATA_GLOB)")
    print(f"aggregating {len(paths)} day file(s)…", flush=True)
    t0 = time.perf_counter()
    df = _aggregate(paths)
    print(f"aggregated {df.height:,} wallets in {time.perf_counter()-t0:.1f}s", flush=True)

    cluster = make_cluster(args.profile, "tuned")
    session = cluster.connect(KEYSPACE)
    vps = prepare_vector(session)
    if vps is None:
        raise SystemExit("vector schema not applied — run: just schema-vector")
    pipe = Pipeline(session, max_inflight=args.max_inflight, sample_every=4096)

    import datetime as dt

    UTC = dt.timezone.utc
    hcols = [f"h{h}" for h in range(24)]
    t1 = time.perf_counter()
    n = 0
    for row in df.iter_rows(named=True):
        w = WalletState()
        w.total_fills = row["total_fills"]
        w.gross_volume = row["gross_volume"] or 0.0
        w.signed_volume = row["signed_volume"] or 0.0
        w.cum_realized_pnl = row["cum_realized_pnl"] or 0.0
        w.taker_buy_vol = row["taker_buy_vol"] or 0.0
        w.taker_sell_vol = row["taker_sell_vol"] or 0.0
        w.opens = row["opens"]
        w.closes = row["closes"]
        w.last_ts = row["last_ts"]
        w.hours = [row[c] for c in hcols]
        w.coin_gross = {i: g for i, g in enumerate(row["coin_gross"] or [])}
        pipe.execute(vps["wallet_vec"], (
            row["addr"], w.cum_realized_pnl, w.total_fills, w.gross_volume,
            abs(w.signed_volume), w.churn, w.archetype,
            dt.datetime.fromtimestamp(w.last_ts / 1000.0, tz=UTC),
            wallet_vector(w),
        ))
        n += 1
        if n % 100_000 == 0:
            el = time.perf_counter() - t1
            print(f"[vecgen] {n:,} vectors  {n/el:,.0f}/s  errs={pipe.errors}", flush=True)
    pipe.drain(args.max_inflight)
    el = time.perf_counter() - t1
    print(f"\n==== vecgen summary ====")
    print(f"wallets written : {n:,}  errors={pipe.errors}")
    print(f"elapsed         : {el:.1f}s  ({n/el:,.0f} vectors/s)")
    session.shutdown()
    cluster.shutdown()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default=os.environ.get("FS_PROFILE", "local"),
                    choices=["local", "cloud"])
    ap.add_argument("--days", type=int, default=None,
                    help="number of day files (default: all)")
    ap.add_argument("--max-inflight", type=int, default=4096)
    run(ap.parse_args())


if __name__ == "__main__":
    main()
