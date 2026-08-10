"""Load + latency benchmark for the inference fast path.

Read benchmark: fire single-partition point reads against wallet_coin_features
(the exact inference read) and report p99 latency + throughput.

The client is driven with MULTIPROCESSING. A single Python process is GIL-bound
and becomes the bottleneck (adding client-side queuing that masks ScyllaDB's
true latency), so we fan out across `--procs` worker processes, each running a
few synchronous-reader threads with its own shard-aware session. This measures
the server, not the client.

    # tuned (shard/token-aware, LOCAL_ONE)
    python -m feature_store.bench read --keys sample_keys.csv --n 600000 --procs 12 --threads 6
    # the "before" baseline (round-robin, LOCAL_QUORUM)
    python -m feature_store.bench read --keys sample_keys.csv --n 600000 --procs 12 --threads 6 --tuning default

ANN benchmark (webinar 2): same client shape, but the query is the vector-index
neighbour lookup instead of the point read, so the two p99s are measured the
same way and can sit side by side on a slide.

    python -m feature_store.bench ann --n 200000 --procs 12 --threads 8 --k 10
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import random
import threading
import time

from .config import make_cluster, KEYSPACE
from .statements import prepare_all, prepare_vector


def _load_keys(path):
    keys = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                addr, coin = line.split(",", 1)
                keys.append((addr, coin))
    return keys


SIMPLE_READ = "SELECT * FROM wallet_coin_features WHERE addr=%s AND coin=%s"


def _worker(profile, tuning, keys, n_per_proc, threads, out_q):
    """One process: `threads` synchronous readers sharing a session.

    tuned   -> prepared statement + token/shard-aware routing + LOCAL_ONE.
    default -> UNprepared SimpleStatement (server parses every query) +
               round-robin + LOCAL_QUORUM. The naive first attempt.
    """
    cluster = make_cluster(profile, tuning)
    session = cluster.connect(KEYSPACE)
    nkeys = len(keys)

    if tuning == "tuned":
        stmt = prepare_all(session)["read_wallet_coin"]
        run_one = lambda a, c: session.execute(stmt, (a, c))
    else:
        from cassandra.query import SimpleStatement

        ss = SimpleStatement(SIMPLE_READ)
        run_one = lambda a, c: session.execute(ss, (a, c))

    # warm
    for a, c in keys[: min(1000, nkeys)]:
        run_one(a, c)

    per_thread = n_per_proc // threads
    parts: list[list[float]] = []
    miss = [0]
    err = [0]
    lock = threading.Lock()

    def run(wid):
        local = []
        m = e = 0
        base = wid * per_thread
        for i in range(per_thread):
            a, c = keys[(base + i) % nkeys]
            t = time.perf_counter()
            try:
                rs = run_one(a, c)
                local.append((time.perf_counter() - t) * 1000.0)
                if rs.one() is None:
                    m += 1
            except Exception:
                e += 1
        with lock:
            parts.append(local)
            miss[0] += m
            err[0] += e

    ts = [threading.Thread(target=run, args=(w,)) for w in range(threads)]
    t0 = time.perf_counter()
    for th in ts:
        th.start()
    for th in ts:
        th.join()
    elapsed = time.perf_counter() - t0

    lat = [x for p in parts for x in p]
    out_q.put({"lat": lat, "miss": miss[0], "err": err[0], "elapsed": elapsed})
    session.shutdown()
    cluster.shutdown()


def read_bench(args):
    keys = _load_keys(args.keys)
    if not keys:
        raise SystemExit("no sample keys; run the consumer with --sample-out first")

    n_per_proc = args.n // args.procs
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    procs = [
        ctx.Process(
            target=_worker,
            args=(args.profile, args.tuning, keys, n_per_proc, args.threads, q),
        )
        for _ in range(args.procs)
    ]
    t0 = time.perf_counter()
    for p in procs:
        p.start()
    results = [q.get() for _ in procs]
    for p in procs:
        p.join()
    elapsed = time.perf_counter() - t0

    lat = sorted(x for r in results for x in r["lat"])
    miss = sum(r["miss"] for r in results)
    err = sum(r["err"] for r in results)
    n = len(lat)
    qf = lambda p: lat[min(n - 1, int(n * p))] if n else 0.0
    conc = args.procs * args.threads
    print("\n==== read benchmark ====")
    print(f"profile={args.profile} tuning={args.tuning} "
          f"procs={args.procs} threads/proc={args.threads} concurrency={conc}")
    print(f"reads        : {n:,}  misses={miss:,}  errors={err:,}")
    print(f"elapsed      : {elapsed:.2f}s")
    print(f"throughput   : {n/elapsed:,.0f} reads/s")
    if n:
        print(f"latency      : p99 {qf(0.99):.3f} ms")
    return {"p99": qf(0.99), "tps": n / elapsed if elapsed else 0}


# --------------------------------------------------------------------------- #
# ANN benchmark — load-tests the vector index, not the feature fast path
# --------------------------------------------------------------------------- #
def _load_seed_vectors(profile, limit):
    """Pull a pool of real wallet embeddings to use as query points.

    Done ONCE in the parent and handed to the workers, so no worker pays a
    scan at startup and every process queries the same distribution.
    """
    cluster = make_cluster(profile, "tuned")
    session = cluster.connect(KEYSPACE)
    stmt = f"SELECT embedding FROM wallet_features LIMIT {limit}"
    seeds = [list(r.embedding) for r in session.execute(stmt) if r.embedding]
    session.shutdown()
    cluster.shutdown()
    return seeds


def _ann_worker(profile, tuning, seeds, n_per_proc, threads, k, wid, out_q):
    """One process: `threads` synchronous ANN queries sharing a session.

    Deliberately NOT similar_wallets() — that does a seed point-read plus k
    neighbour point-reads per call, which would measure the feature store
    instead of the index. Here one query == one ANN round trip.
    """
    cluster = make_cluster(profile, tuning)
    session = cluster.connect(KEYSPACE)
    vps = prepare_vector(session)
    if vps is None:
        out_q.put({"lat": [], "empty": 0, "short": 0, "err": 0, "elapsed": 0.0,
                   "fatal": "vector schema not applied"})
        return
    stmt = vps["ann_wallets"]
    nseeds = len(seeds)

    for v in seeds[: min(200, nseeds)]:   # warm the connection pool
        session.execute(stmt, (v, k))

    per_thread = n_per_proc // threads
    parts: list[list[float]] = []
    empty = [0]
    short = [0]
    err = [0]
    lock = threading.Lock()

    def run(tid):
        local = []
        e = s = x = 0
        # stagger each thread into a different region of the seed pool so the
        # fleet doesn't hammer one point of the index in lockstep
        rnd = random.Random(wid * 1000 + tid)
        for _ in range(per_thread):
            v = seeds[rnd.randrange(nseeds)]
            t = time.perf_counter()
            try:
                rows = session.execute(stmt, (v, k))
                local.append((time.perf_counter() - t) * 1000.0)
                got = sum(1 for _ in rows)
                # an empty/short neighbour list is FAST but wrong — the failure
                # mode seen when the index is re-building under write pressure,
                # so it must be counted, never averaged into the latency win
                if got == 0:
                    e += 1
                elif got < k:
                    s += 1
            except Exception:
                x += 1
        with lock:
            parts.append(local)
            empty[0] += e
            short[0] += s
            err[0] += x

    ts = [threading.Thread(target=run, args=(t,)) for t in range(threads)]
    t0 = time.perf_counter()
    for th in ts:
        th.start()
    for th in ts:
        th.join()
    elapsed = time.perf_counter() - t0

    out_q.put({"lat": [x for p in parts for x in p], "empty": empty[0],
               "short": short[0], "err": err[0], "elapsed": elapsed})
    session.shutdown()
    cluster.shutdown()


def ann_bench(args):
    seeds = _load_seed_vectors(args.profile, args.seeds)
    if not seeds:
        raise SystemExit("no wallet embeddings found — run the consumer first")
    print(f"seed pool: {len(seeds):,} vectors")

    n_per_proc = args.n // args.procs
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    procs = [
        ctx.Process(
            target=_ann_worker,
            args=(args.profile, args.tuning, seeds, n_per_proc, args.threads,
                  args.k, w, q),
        )
        for w in range(args.procs)
    ]
    t0 = time.perf_counter()
    for p in procs:
        p.start()
    results = [q.get() for _ in procs]
    for p in procs:
        p.join()
    elapsed = time.perf_counter() - t0

    fatal = next((r["fatal"] for r in results if r.get("fatal")), None)
    if fatal:
        raise SystemExit(fatal)

    lat = sorted(x for r in results for x in r["lat"])
    empty = sum(r["empty"] for r in results)
    short = sum(r["short"] for r in results)
    err = sum(r["err"] for r in results)
    n = len(lat)
    qf = lambda p: lat[min(n - 1, int(n * p))] if n else 0.0
    conc = args.procs * args.threads
    print("\n==== ANN benchmark ====")
    print(f"profile={args.profile} k={args.k} "
          f"procs={args.procs} threads/proc={args.threads} concurrency={conc}")
    print(f"queries      : {n:,}  errors={err:,}")
    print(f"empty results: {empty:,} ({100.0*empty/n if n else 0:.2f}%)  "
          f"short (<k): {short:,}")
    print(f"elapsed      : {elapsed:.2f}s")
    print(f"throughput   : {n/elapsed:,.0f} ANN queries/s")
    if n:
        print(f"latency      : p50 {qf(0.50):.3f} ms  p95 {qf(0.95):.3f} ms  "
              f"p99 {qf(0.99):.3f} ms  max {lat[-1]:.3f} ms")
    return {"p50": qf(0.50), "p99": qf(0.99), "tps": n / elapsed if elapsed else 0,
            "empty": empty, "short": short, "err": err, "conc": conc}


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("read")
    r.add_argument("--profile", default=os.environ.get("FS_PROFILE", "local"),
                    choices=["local", "cloud"])
    r.add_argument("--tuning", default="tuned", choices=["tuned", "default"])
    r.add_argument("--keys", default="sample_keys.csv")
    r.add_argument("--n", type=int, default=600_000)
    r.add_argument("--procs", type=int, default=12)
    r.add_argument("--threads", type=int, default=6)

    a = sub.add_parser("ann")
    a.add_argument("--profile", default=os.environ.get("FS_PROFILE", "local"),
                   choices=["local", "cloud"])
    a.add_argument("--tuning", default="tuned", choices=["tuned", "default"])
    a.add_argument("--n", type=int, default=200_000)
    a.add_argument("--procs", type=int, default=12)
    a.add_argument("--threads", type=int, default=8)
    a.add_argument("--k", type=int, default=10)
    a.add_argument("--seeds", type=int, default=5_000,
                   help="size of the query-vector pool sampled from the table")

    args = ap.parse_args()
    if args.cmd == "read":
        read_bench(args)
    elif args.cmd == "ann":
        ann_bench(args)


if __name__ == "__main__":
    main()
