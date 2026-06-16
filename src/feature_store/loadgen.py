"""Write-throughput load generator.

The feature `consumer` is single-process and CPU-bound on feature math, so its
sustained rate is a *client* limit, not ScyllaDB's. This load generator shows the
database's write ceiling: N worker processes, each replaying a disjoint shard of
real fills (sharded by hash(addr) so wallet/(wallet,coin) partitioning is
preserved) and issuing pipelined, prepared, shard-aware blind upserts to
wallet_coin_features at LOCAL_ONE.

Parquet is read and materialised first; only the write loop is timed.

    python -m feature_store.loadgen --procs 12 --days 1 --max-inflight 2048
"""
from __future__ import annotations

import os

# Cap polars' threadpool BEFORE it is imported. Each loadgen worker only reads a
# subset of one parquet file, so it doesn't need a 48-thread (per-core) pool; with
# dozens of worker processes that would spawn thousands of idle threads (~139 per
# worker) and oversubscribe the box. Honour an explicit override if set.
os.environ.setdefault("POLARS_MAX_THREADS", "2")

import argparse
import datetime as dt
import multiprocessing as mp
import time
from datetime import timezone

import polars as pl

from .config import make_cluster, KEYSPACE
from .replay import day_files, COLUMNS
from .statements import prepare_all
from .writer import Pipeline

UTC = timezone.utc


SIMPLE_WRITE = (
    "INSERT INTO wallet_coin_features "
    "(addr, coin, net_pos, avg_entry, realized_pnl, fill_count, last_ts) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
)


def _worker(wid, procs, files, max_inflight, tuning, profile, barrier, out_q):
    # Load this shard's fills into memory (untimed).
    rows = []
    sample_keys = []
    for path in files:
        df = pl.read_parquet(path, columns=COLUMNS)
        for addr, coin, px, sz, side, t, cpnl, crossed in df.iter_rows():
            if (hash(addr) % procs) != wid:
                continue
            net = sz if side == "B" else -sz
            rows.append((addr, coin, net, px, (cpnl or 0.0),
                         dt.datetime.fromtimestamp(t / 1000.0, tz=UTC)))
            if len(sample_keys) < 2000 and (len(rows) % 53 == 0):
                sample_keys.append((addr, coin))

    cluster = make_cluster(profile, tuning)
    session = cluster.connect(KEYSPACE)
    pipe = Pipeline(session, max_inflight=max_inflight, sample_every=512)

    if tuning == "tuned":
        stmt = prepare_all(session)["wallet_coin"]      # prepared, shard-aware, LOCAL_ONE
    else:
        from cassandra.query import SimpleStatement     # unprepared, RR, LOCAL_QUORUM

        stmt = SimpleStatement(SIMPLE_WRITE)

    # All workers finish loading (and connecting) before any starts writing, so
    # the timed write loops run concurrently and throughput = total / window is a
    # true concurrent measurement, not skewed by load-phase stagger.
    barrier.wait()

    fc = 0
    t0 = time.perf_counter()
    for addr, coin, net, px, cpnl, ts in rows:
        fc += 1
        pipe.execute(stmt, (addr, coin, net, px, cpnl, fc, ts))
    pipe.drain(max_inflight)
    elapsed = time.perf_counter() - t0

    out_q.put({"writes": pipe.count, "errors": pipe.errors, "elapsed": elapsed,
               "lat": pipe.write_latency_ms(), "keys": sample_keys})
    session.shutdown()
    cluster.shutdown()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--procs", type=int, default=12)
    ap.add_argument("--days", type=int, default=1)
    ap.add_argument("--max-inflight", type=int, default=2048)
    ap.add_argument("--tuning", default="tuned", choices=["tuned", "default"])
    ap.add_argument("--profile", default="local", choices=["local", "cloud"])
    ap.add_argument("--sample-out", default=None,
                    help="write a sample of loaded (addr,coin) keys for the read bench")
    args = ap.parse_args()

    files = day_files(args.days)
    print(f"loadgen: {len(files)} day file(s), {args.procs} procs, "
          f"loading shards into memory ...", flush=True)

    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    barrier = ctx.Barrier(args.procs)
    procs = [ctx.Process(target=_worker,
                         args=(w, args.procs, files, args.max_inflight,
                               args.tuning, args.profile, barrier, q))
             for w in range(args.procs)]
    for p in procs:
        p.start()
    results = [q.get() for _ in procs]
    for p in procs:
        p.join()

    total_writes = sum(r["writes"] for r in results)
    total_errors = sum(r["errors"] for r in results)
    # wall-clock write window = max worker elapsed (they run concurrently)
    wall = max(r["elapsed"] for r in results)
    lat = sorted(x for r in results for x in r["lat"])
    qf = lambda p: lat[min(len(lat) - 1, int(len(lat) * p))] if lat else 0.0
    if args.sample_out:
        seen = set()
        with open(args.sample_out, "w") as fh:
            for r in results:
                for addr, coin in r.get("keys", []):
                    if (addr, coin) not in seen:
                        seen.add((addr, coin))
                        fh.write(f"{addr},{coin}\n")
        print(f"wrote {len(seen):,} sample keys -> {args.sample_out}")

    print("\n==== write load test ====")
    print(f"profile={args.profile} procs={args.procs} "
          f"max_inflight/proc={args.max_inflight} tuning={args.tuning}")
    print(f"writes acked : {total_writes:,}  errors={total_errors:,}")
    print(f"write window : {wall:.2f}s (max worker)")
    print(f"throughput   : {total_writes/wall:,.0f} writes/s")
    if lat:
        print(f"write latency: p99 {qf(0.99):.2f} ms (sampled n={len(lat):,})")


if __name__ == "__main__":
    main()
