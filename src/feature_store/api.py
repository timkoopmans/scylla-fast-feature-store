"""Inference endpoint: point-read fresh features from ScyllaDB and score.

    uvicorn feature_store.api:app --host 0.0.0.0 --port 8080

Every response carries the server-measured DB read latency so you can see, live,
that the feature fetch is the fast path (sub-millisecond point reads).
"""
from __future__ import annotations

import time

from fastapi import FastAPI, HTTPException

from .config import make_cluster, KEYSPACE
from .statements import prepare_all, prepare_vector
from . import scorer, similarity

app = FastAPI(title="ScyllaDB feature store — inference")

_state = {}


@app.on_event("startup")
def _startup():
    cluster = make_cluster(
        profile=_env("FS_PROFILE", "local"), tuning=_env("FS_TUNING", "tuned")
    )
    session = cluster.connect(KEYSPACE)
    _state["cluster"] = cluster
    _state["session"] = session
    _state["ps"] = prepare_all(session)
    _state["vps"] = prepare_vector(session)  # None until schema_vector.cql applied


@app.on_event("shutdown")
def _shutdown():
    _state["session"].shutdown()
    _state["cluster"].shutdown()


def _row_to_dict(rs):
    one = rs.one()
    return dict(one._asdict()) if one else None


@app.get("/score/coin/{coin}")
def score_coin(coin: str):
    s, ps = _state["session"], _state["ps"]
    t0 = time.perf_counter()
    rows = {
        win: _row_to_dict(s.execute(ps["read_coin_window"], (coin, win)))
        for win in ("1m", "5m", "1h")
    }
    db_ms = (time.perf_counter() - t0) * 1000.0
    result = scorer.unusual_accumulation(rows["1m"], rows["5m"], rows["1h"])
    return {
        "coin": coin,
        "db_read_ms": round(db_ms, 3),
        "fresh_bucket_ts": (rows["1m"] or {}).get("bucket_ts"),
        **result,
        "windows": rows,
    }


@app.get("/archetype/wallet/{addr}")
def archetype(addr: str):
    s, ps = _state["session"], _state["ps"]
    t0 = time.perf_counter()
    row = _row_to_dict(s.execute(ps["read_wallet"], (addr,)))
    db_ms = (time.perf_counter() - t0) * 1000.0
    if not row:
        raise HTTPException(404, "wallet not found")
    return {"addr": addr, "db_read_ms": round(db_ms, 3), **scorer.wallet_archetype(row)}


@app.get("/features/wallet/{addr}/coin/{coin}")
def wallet_coin(addr: str, coin: str):
    s, ps = _state["session"], _state["ps"]
    t0 = time.perf_counter()
    row = _row_to_dict(s.execute(ps["read_wallet_coin"], (addr, coin)))
    db_ms = (time.perf_counter() - t0) * 1000.0
    if not row:
        raise HTTPException(404, "no features for (wallet, coin)")
    return {"db_read_ms": round(db_ms, 3), **row}


# --- webinar 2: similarity endpoints (ANN + point-reads, one engine) ---------

def _vps():
    vps = _state.get("vps")
    if vps is None:
        raise HTTPException(503, "vector schema not applied (just schema-vector)")
    return vps


@app.get("/similar/wallet/{addr}")
def similar_wallet(addr: str, k: int = 10):
    out = similarity.similar_wallets(_state["session"], _state["ps"], _vps(), addr, k)
    if out is None:
        raise HTTPException(404, "no embedding for wallet (consumer not run yet?)")
    return out


@app.get("/similar/coin/{coin}")
def similar_coin(coin: str, k: int = 5):
    out = similarity.similar_coins(_state["session"], _state["ps"], _vps(), coin, k)
    if out is None:
        raise HTTPException(404, "no flow vector for coin")
    return out


def _env(k, d):
    import os

    return os.environ.get(k, d)
