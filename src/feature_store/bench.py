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
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import threading
import time

from .config import make_cluster, KEYSPACE
from .statements import prepare_all


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


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("read")
    r.add_argument("--profile", default="local", choices=["local", "cloud"])
    r.add_argument("--tuning", default="tuned", choices=["tuned", "default"])
    r.add_argument("--keys", default="sample_keys.csv")
    r.add_argument("--n", type=int, default=600_000)
    r.add_argument("--procs", type=int, default=12)
    r.add_argument("--threads", type=int, default=6)
    args = ap.parse_args()
    if args.cmd == "read":
        read_bench(args)


if __name__ == "__main__":
    main()
