"""Streaming consumer: replay fills -> compute features -> upsert to ScyllaDB.

Run on the demo host inside the venv:

    python -m feature_store.consumer --speed 0 --days 1 --raw-sink off

Write model
-----------
* wallet_coin_features : write-through, one blind upsert per processed fill.
  This is the firehose write rate we report.
* coin_window_features : upserted when a tumbling bucket closes, plus a periodic
  snapshot of open buckets so reads stay fresh mid-bucket.
* wallet_features      : coalesced -- dirty wallets flushed every FLUSH_SECS.
"""
from __future__ import annotations

import argparse
import datetime as dt
import time
from datetime import timezone

from .config import make_cluster, KEYSPACE
from .features import FeatureEngine
from .replay import replay
from .statements import prepare_all
from .writer import Pipeline

UTC = timezone.utc
COIN_BUCKET_SECONDS = 10  # partition rotation for fills_by_coin_bucket


def _ts(ms: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=UTC)


def run(args) -> None:
    cluster = make_cluster(args.profile, args.tuning)
    session = cluster.connect(KEYSPACE)
    ps = prepare_all(session)
    pipe = Pipeline(session, max_inflight=args.max_inflight)
    engine = FeatureEngine()

    flush_secs = args.flush_secs
    last_flush = time.monotonic()
    last_report = time.monotonic()
    t_start = time.monotonic()
    processed = 0
    report_every = 200_000
    sample_keys: list[tuple[str, str]] = []

    def flush_wallets_and_open_windows():
        # coin window open snapshots (freshness)
        for coin, win, snap in engine.open_snapshots():
            pipe.execute(ps["coin_window"], (
                coin, win, _ts(snap["bucket_ts"] * 1000), snap["volume"],
                snap["taker_buy"], snap["taker_sell"], snap["buy_sell_imbalance"],
                snap["active_wallets"], snap["hhi"], snap["large_flow"], snap["smart_flow"],
            ))
        # wallet features (coalesced full flush of all known wallets)
        for addr, w in engine.wallets.items():
            pipe.execute(ps["wallet"], (
                addr, w.cum_realized_pnl, w.total_fills, w.gross_volume,
                abs(w.signed_volume), w.churn, w.archetype, _ts(w.last_ts),
            ))

    for f in replay(speed=args.speed, limit_days=args.days, max_fills=args.max_fills):
        wc, w, closed = engine.apply(f)

        # 1) write-through per-fill wallet-coin feature
        pipe.execute(ps["wallet_coin"], (
            f.addr, f.coin, wc.net_pos, wc.avg_entry, wc.realized_pnl,
            wc.fill_count, _ts(wc.last_ts),
        ))

        # 2) closed window buckets
        for coin, win, snap in closed:
            pipe.execute(ps["coin_window"], (
                coin, win, _ts(snap["bucket_ts"] * 1000), snap["volume"],
                snap["taker_buy"], snap["taker_sell"], snap["buy_sell_imbalance"],
                snap["active_wallets"], snap["hhi"], snap["large_flow"], snap["smart_flow"],
            ))

        # 3) optional raw-fill sinks for the hot-partition section
        if args.raw_sink == "bucket":
            tb = (f.ts_ms // 1000 // COIN_BUCKET_SECONDS) * COIN_BUCKET_SECONDS
            pipe.execute(ps["fill_bucket"], (
                f.coin, _ts(tb * 1000), _ts(f.ts_ms), f.addr, f.px, f.sz,
                f.side, f.crossed, f.closed_pnl,
            ))
        elif args.raw_sink == "hot":
            pipe.execute(ps["fill_hot"], (
                f.coin, _ts(f.ts_ms), f.addr, f.px, f.sz, f.side, f.crossed, f.closed_pnl,
            ))

        processed += 1
        if len(sample_keys) < 50_000 and (processed % 97 == 0):
            sample_keys.append((f.addr, f.coin))

        now = time.monotonic()
        if now - last_flush >= flush_secs:
            flush_wallets_and_open_windows()
            last_flush = now

        if processed % report_every == 0:
            dtt = now - last_report
            rate = report_every / dtt if dtt else 0
            print(f"[consume] {processed:,} fills  {rate:,.0f} fills/s  "
                  f"inflight_errs={pipe.errors}", flush=True)
            last_report = now

    flush_wallets_and_open_windows()
    pipe.drain(args.max_inflight)
    elapsed = time.monotonic() - t_start

    # write sample keys for the read benchmark
    if args.sample_out:
        with open(args.sample_out, "w") as fh:
            for addr, coin in sample_keys:
                fh.write(f"{addr},{coin}\n")

    lat = pipe.write_latency_ms()
    p = lambda q: lat[int(len(lat) * q)] if lat else 0.0
    total_writes = pipe.count
    print("\n==== consume summary ====")
    print(f"profile={args.profile} tuning={args.tuning} raw_sink={args.raw_sink}")
    print(f"fills processed : {processed:,}")
    print(f"writes acked    : {total_writes:,}  errors={pipe.errors}")
    print(f"elapsed         : {elapsed:.1f}s")
    print(f"fill throughput : {processed/elapsed:,.0f} fills/s")
    print(f"write throughput: {total_writes/elapsed:,.0f} writes/s")
    if lat:
        print(f"write latency   : p99 {p(0.99):.2f} ms (sampled n={len(lat):,})")

    session.shutdown()
    cluster.shutdown()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="local", choices=["local", "cloud"])
    ap.add_argument("--tuning", default="tuned", choices=["tuned", "default"])
    ap.add_argument("--speed", type=float, default=0.0,
                    help="replay speed multiplier; 0 = max (no sleep)")
    ap.add_argument("--days", type=int, default=1, help="number of day files")
    ap.add_argument("--max-fills", type=int, default=None)
    ap.add_argument("--raw-sink", default="off", choices=["off", "bucket", "hot"])
    ap.add_argument("--max-inflight", type=int, default=4096)
    ap.add_argument("--flush-secs", type=float, default=2.0)
    ap.add_argument("--sample-out", default="sample_keys.csv")
    run(ap.parse_args())


if __name__ == "__main__":
    main()
