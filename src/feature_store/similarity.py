"""Similarity queries — the webinar-2 headline: ONE engine, TWO query types.

An ANN neighbour lookup (`ORDER BY embedding ANN OF`) and the webinar-1
feature point-read run in the same CQL session against the same table. No
second database, no sync pipeline.

CLI (inside the venv, PYTHONPATH=src):

    python -m feature_store.similarity smoke                # pick a seed, run both lenses
    python -m feature_store.similarity wallet 0xabc… --k 10
    python -m feature_store.similarity coin BTC --k 5
"""
from __future__ import annotations

import argparse
import json
import os
import time

from .config import make_cluster, KEYSPACE
from .embeddings import cosine, explain
from .statements import prepare_all, prepare_vector

# neighbours this close are behaviourally near-identical — the ring heuristic
RING_COSINE = 0.995
RING_MIN_NEIGHBOURS = 3


def _row(rs):
    one = rs.one()
    return dict(one._asdict()) if one else None


def similar_wallets(session, ps, vps, addr: str, k: int = 10) -> dict | None:
    """k nearest wallets by behaviour + their live features, with timings and
    a ring-tightness verdict."""
    seed = _row(session.execute(ps["read_wallet"], (addr,)))
    if not seed or seed.get("embedding") is None:
        return None
    seed_vec = list(seed["embedding"])

    t0 = time.perf_counter()
    rows = session.execute(vps["ann_wallets"], (seed_vec, k + 1))
    addrs = [r.addr for r in rows if r.addr != addr][:k]
    ann_ms = (time.perf_counter() - t0) * 1000.0

    t1 = time.perf_counter()
    neighbours = []
    for a in addrs:
        row = _row(session.execute(ps["read_wallet"], (a,)))
        if not row:
            continue
        vec = list(row.pop("embedding") or [])
        cos = cosine(seed_vec, vec) if vec else 0.0
        neighbours.append({**row, "cosine": round(cos, 4),
                           "why": explain(seed_vec, vec) if vec else None})
    reads_ms = (time.perf_counter() - t1) * 1000.0

    tight = [n for n in neighbours if n["cosine"] >= RING_COSINE]
    seed.pop("embedding", None)
    return {
        "seed": seed,
        "neighbours": neighbours,
        "ann_ms": round(ann_ms, 2),
        "point_reads_ms": round(reads_ms, 2),
        "ring_suspect": len(tight) >= RING_MIN_NEIGHBOURS,
        "ring_size": len(tight),
    }


def similar_coins(session, ps, vps, coin: str, k: int = 5) -> dict | None:
    """Coins whose live flow signature looks like this coin's, right now."""
    seed = _row(session.execute(vps["read_coin_flow"], (coin,)))
    if not seed or seed.get("embedding") is None:
        return None
    seed_vec = list(seed["embedding"])

    t0 = time.perf_counter()
    rows = session.execute(vps["ann_coins"], (seed_vec, k + 1))
    coins = [r.coin for r in rows if r.coin != coin][:k]
    ann_ms = (time.perf_counter() - t0) * 1000.0

    neighbours = []
    for c in coins:
        row = _row(session.execute(vps["read_coin_flow"], (c,)))
        if not row:
            continue
        vec = list(row.pop("embedding") or [])
        neighbours.append({"coin": c, "cosine": round(cosine(seed_vec, vec), 4),
                           "updated_ts": str(row.get("updated_ts"))})
    return {"coin": coin, "neighbours": neighbours, "ann_ms": round(ann_ms, 2)}


def _smoke(session, ps, vps, k: int) -> int:
    """End-to-end sanity: grab any wallet with an embedding and any coin
    vector, run both similarity lenses, print the results."""
    seed_addr = None
    for r in session.execute(
            "SELECT addr, embedding FROM wallet_features LIMIT 200"):
        if r.embedding is not None:
            seed_addr = r.addr
            break
    if not seed_addr:
        print("smoke: no wallet embeddings found — run the consumer first")
        return 1
    print(f"smoke: seed wallet {seed_addr}")
    out = similar_wallets(session, ps, vps, seed_addr, k)
    print(json.dumps(out, indent=2, default=str))

    row = session.execute("SELECT coin FROM coin_flow_vectors LIMIT 1").one()
    if row:
        print(f"\nsmoke: seed coin {row.coin}")
        print(json.dumps(similar_coins(session, ps, vps, row.coin, k),
                         indent=2, default=str))
    else:
        print("smoke: no coin flow vectors yet")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["wallet", "coin", "smoke"])
    ap.add_argument("id", nargs="?", help="wallet address or coin symbol")
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--profile", default=os.environ.get("FS_PROFILE", "local"),
                    choices=["local", "cloud"])
    args = ap.parse_args()

    cluster = make_cluster(args.profile, "tuned")
    session = cluster.connect(KEYSPACE)
    ps = prepare_all(session)
    vps = prepare_vector(session)
    if vps is None:
        raise SystemExit("vector schema not applied — run: just schema-vector")

    try:
        if args.mode == "smoke":
            raise SystemExit(_smoke(session, ps, vps, args.k))
        if not args.id:
            raise SystemExit("wallet/coin mode needs an id argument")
        fn = similar_wallets if args.mode == "wallet" else similar_coins
        out = fn(session, ps, vps, args.id, args.k)
        if out is None:
            raise SystemExit(f"no embedding for {args.mode} {args.id}")
        print(json.dumps(out, indent=2, default=str))
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
