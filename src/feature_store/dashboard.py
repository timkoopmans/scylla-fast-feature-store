"""Live web dashboard for the replay + feature store.

Architecture matters here: the replay+ingest loop is a tight Python loop that
holds the GIL almost continuously, so if the inference reads share its process
they look artificially slow. We therefore run **ingest in a separate process**
and do the inference point-reads in the web process, where the GIL is free — so
the latency widget shows ScyllaDB's true sub-millisecond reads while the firehose
hammers writes from the other process.

  * ingest process : replay -> features -> upserts to ScyllaDB; paces itself from
    a shared speed (so the BURST button can spike it live); publishes firehose
    stats, the busiest coins (with volume), and the wallet-archetype mix.
  * web process    : point-reads coin_window_features for those busy coins (the
    inference retrieval path), scores them, records DB read latency + freshness,
    and streams everything to an HTML page over a websocket.

    uvicorn feature_store.dashboard:app --host 0.0.0.0 --port 8090
    # then open http://<demo-host>:8090  (set FS_SPEED, FS_DAYS to taste)

Complements ScyllaDB Monitoring: Grafana shows the DB internals; this shows the
feature-store semantics — the firehose, fresh features, live inference, the
read-tail-under-write-burst, and the per-coin load skew that motivates the
partition-key design.
"""

from __future__ import annotations

import asyncio
import base64
import datetime as dt
import heapq
import multiprocessing as mp
import os
import threading
import time
from collections import deque
from datetime import timezone

from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.responses import HTMLResponse

# Cap polars threads — the ingest and the write-blaster procs read parquet; set
# before polars is imported (via .features/.replay below).
os.environ.setdefault("POLARS_MAX_THREADS", "2")

from .config import KEYSPACE, make_cluster
from .embeddings import coin_vector, cosine, wallet_vector
from .features import FeatureEngine
from .replay import iter_fills
from .scorer import unusual_accumulation
from .statements import prepare_all, prepare_vector
from .writer import Pipeline
from . import similarity

UTC = timezone.utc
SPEED = float(os.environ.get("FS_SPEED", "30"))
DAYS = int(os.environ.get("FS_DAYS", "3"))
PROFILE = os.environ.get("FS_PROFILE", "local")
BURST_SECS = float(os.environ.get("FS_BURST_SECS", "15"))
# Background write-load fleet: N processes re-upserting fills at max speed so the
# dashboard shows ScyllaDB absorbing real write load (the single paced ingest is
# GIL-bound at ~5k/s). 0 = off.
# Baseline blasters run flat-out (steady writes/s). BURST blasters sit idle until
# ⚡ BURST is pressed, then write full-speed for the burst window — so BURST *adds*
# write load on top of the baseline (no throttling of anything).
BLASTERS = int(os.environ.get("FS_BLASTERS", "0"))
BURST_BLASTERS = int(os.environ.get("FS_BURST_BLASTERS", "0"))
BLAST_INFLIGHT = int(os.environ.get("FS_BLAST_INFLIGHT", "2048"))
# behaviour-embedding flush cadence (webinar 2); only dirty wallets are written
EMB_FLUSH_SECS = float(os.environ.get("FS_EMB_FLUSH_SECS", "2.0"))

app = FastAPI(title="ScyllaDB feature store — live")

_ASSETS = os.path.join(os.path.dirname(__file__), "..", "..", "assets")


def _data_uri(name: str, mime: str) -> str:
    try:
        with open(os.path.join(_ASSETS, name), "rb") as fh:
            return f"data:{mime};base64," + base64.b64encode(fh.read()).decode()
    except OSError:
        return ""


_LOGO_SCYLLA = _data_uri("scylladb-monster.svg", "image/svg+xml")
_LOGO_HL = _data_uri("hyperliquid-logo.png", "image/png")

STATS = {
    "fills_total": 0,
    "writes_total": 0,
    "fills_per_s": 0.0,
    "writes_per_s": 0.0,
    "data_time": None,
    "scoreboard": [],
    "read_p99_ms": 0.0,
    "fresh_us": 0.0,
    "active_coins": 0,
    "wallets": 0,
    "hot": [],
    "arch": {},
    "bursting": False,
    "seeds": {},
    "vector_on": False,
}
_lock = threading.Lock()
_proc = {}


def _ts(ms: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=UTC)


# --------------------------------------------------------------------------- #
# ingest process — self-paced so the BURST button can spike replay speed live
# --------------------------------------------------------------------------- #
def _ingest_proc(shared, base_speed, days, profile):
    engine = FeatureEngine()
    cluster = make_cluster(profile, "tuned")
    session = cluster.connect(KEYSPACE)
    # Each demo run replays day-1 from the start; clear stale window rows from
    # prior runs so the inference read always sees THIS run's fresh buckets.
    try:
        session.execute("TRUNCATE coin_window_features")
    except Exception:
        pass
    ps = prepare_all(session)
    vps = prepare_vector(session)  # None until cql/schema_vector.cql is applied
    pipe = Pipeline(session, max_inflight=2048)
    now0 = time.monotonic()
    last_t = last_flush = last_speedchk = last_emb = now0
    last_fills = last_writes = 0
    n = 0
    next_emit = now0
    prev_ts = None
    eff_speed = base_speed
    dirty: set[str] = set()   # wallets touched since the last embedding flush

    def flush_open():
        for coin, win, snap in engine.open_snapshots():
            pipe.execute(
                ps["coin_window"],
                (
                    coin,
                    win,
                    _ts(snap["bucket_ts"] * 1000),
                    snap["volume"],
                    snap["taker_buy"],
                    snap["taker_sell"],
                    snap["buy_sell_imbalance"],
                    snap["active_wallets"],
                    snap["hhi"],
                    snap["large_flow"],
                    snap["smart_flow"],
                ),
            )

    def flush_embeddings():
        # wallet features + behaviour vector in ONE upsert (one CDC row image
        # carrying the vector -> the ANN index refreshes); dirty wallets only,
        # so the extra write load tracks activity, not population size.
        for addr in dirty:
            w = engine.wallets.get(addr)
            if w is None:
                continue
            pipe.execute(vps["wallet_vec"], (
                addr, w.cum_realized_pnl, w.total_fills, w.gross_volume,
                abs(w.signed_volume), w.churn, w.archetype, _ts(w.last_ts),
                wallet_vector(w),
            ))
        dirty.clear()
        coin_snaps: dict[str, dict[str, dict]] = {}
        for coin, win, snap in engine.open_snapshots():
            coin_snaps.setdefault(coin, {})[win] = snap
        now_utc = dt.datetime.now(UTC)
        for coin, snaps in coin_snaps.items():
            pipe.execute(vps["coin_flow"], (coin, coin_vector(snaps), now_utc))

    for f in iter_fills(limit_days=days):
        now = time.monotonic()

        # refresh effective speed / burst state every 0.25s (Manager reads are IPC)
        if now - last_speedchk >= 0.25:
            base = shared.get("speed", base_speed)
            bursting = time.time() < shared.get("burst_until", 0.0)
            eff_speed = 1e9 if bursting else base
            last_speedchk = now
            shared["bursting"] = bursting

        # inter-arrival pacing (handles dynamic speed cleanly)
        if prev_ts is not None and eff_speed > 0:
            next_emit += (f.ts_ms - prev_ts) / 1000.0 / eff_speed
            slp = next_emit - time.monotonic()
            if slp > 0:
                time.sleep(slp)
            elif slp < -1.0:  # fell behind; resync to now
                next_emit = time.monotonic()
        prev_ts = f.ts_ms

        wc, w, closed = engine.apply(f)
        if vps:
            dirty.add(f.addr)
        pipe.execute(
            ps["wallet_coin"],
            (
                f.addr,
                f.coin,
                wc.net_pos,
                wc.avg_entry,
                wc.realized_pnl,
                wc.fill_count,
                _ts(wc.last_ts),
            ),
        )
        for coin, win, snap in closed:
            pipe.execute(
                ps["coin_window"],
                (
                    coin,
                    win,
                    _ts(snap["bucket_ts"] * 1000),
                    snap["volume"],
                    snap["taker_buy"],
                    snap["taker_sell"],
                    snap["buy_sell_imbalance"],
                    snap["active_wallets"],
                    snap["hhi"],
                    snap["large_flow"],
                    snap["smart_flow"],
                ),
            )
        n += 1

        if now - last_flush >= 0.25:
            flush_open()
            last_flush = now
        if vps and now - last_emb >= EMB_FLUSH_SECS:
            flush_embeddings()
            last_emb = now
        if now - last_t >= 0.5:
            dt_s = now - last_t
            hot = sorted(
                (
                    (b.volume, coin)
                    for (coin, win), b in engine.coins.open.items()
                    if win == "1m"
                ),
                reverse=True,
            )[:14]
            arch = {"market-maker": 0, "directional": 0, "mixed": 0}
            for ws in engine.wallets.values():
                arch[ws.archetype] = arch.get(ws.archetype, 0) + 1
            # seed candidates for the Similar-wallets tab: biggest wallets by
            # gross notional ("whales") and profitable directional wallets
            # ("smart money") — the two lenses the ANN demo starts from.
            whales = heapq.nlargest(
                8, engine.wallets.items(), key=lambda kv: kv[1].gross_volume
            )
            smart = heapq.nlargest(
                8,
                (
                    kv for kv in engine.wallets.items()
                    if kv[1].archetype == "directional" and kv[1].cum_realized_pnl > 0
                ),
                key=lambda kv: kv[1].cum_realized_pnl,
            )
            shared["seeds"] = {
                grp: [
                    {
                        "addr": a,
                        "gross": ws_.gross_volume,
                        "pnl": ws_.cum_realized_pnl,
                        "arch": ws_.archetype,
                    }
                    for a, ws_ in lst
                ]
                for grp, lst in (("whales", whales), ("smart", smart))
            }
            shared["fills_total"] = n
            shared["writes_total"] = pipe.count
            shared["fills_per_s"] = (n - last_fills) / dt_s
            shared["writes_per_s"] = (pipe.count - last_writes) / dt_s
            shared["data_time_ms"] = f.ts_ms
            shared["wallets"] = len(engine.wallets)
            shared["active_coins"] = len(engine.coins.open) // 3
            shared["hot"] = [{"coin": c, "vol": v} for v, c in hot]
            shared["arch"] = arch
            last_fills, last_writes, last_t = n, pipe.count, now

    flush_open()
    pipe.drain(2048)
    session.shutdown()
    cluster.shutdown()


# --------------------------------------------------------------------------- #
# write-blaster process — sustains real write load to ScyllaDB
# --------------------------------------------------------------------------- #
def _blaster_proc(wid, nblast, days, profile, max_inflight, burst_only, shared):
    import polars as pl

    from .replay import COLUMNS, day_files

    # this worker's even share of fills (stride by index), loaded once
    rows = []
    gi = 0
    for path in day_files(days):
        df = pl.read_parquet(path, columns=COLUMNS)
        for addr, coin, px, sz, side, t, cpnl, crossed in df.iter_rows():
            if gi % nblast == wid:
                net = sz if side == "B" else -sz
                rows.append(
                    (
                        addr,
                        coin,
                        net,
                        px,
                        (cpnl or 0.0),
                        dt.datetime.fromtimestamp(t / 1000.0, tz=UTC),
                    )
                )
            gi += 1
    cluster = make_cluster(profile, "tuned")
    session = cluster.connect(KEYSPACE)
    stmt = prepare_all(session)["wallet_coin"]
    pipe = Pipeline(session, max_inflight=max_inflight, sample_every=4096)
    key = f"bw_{wid}"
    BATCH = 1024
    fc = 0
    last_pub = time.monotonic()
    last_chk = last_pub
    bursting = False
    while True:  # loop the data forever (idempotent upserts)
        # burst-only blasters idle (no writes) until BURST is active — they ADD
        # load on top of the baseline rather than throttling anything.
        if burst_only and not bursting:
            time.sleep(0.1)
            now = time.monotonic()
            if now - last_chk >= 0.25:
                bursting = time.time() < shared.get("burst_until", 0.0)
                last_chk = now
            shared[key] = pipe.count
            continue
        for addr, coin, net, px, cpnl, ts in rows:
            fc += 1
            pipe.execute(stmt, (addr, coin, net, px, cpnl, fc, ts))
            if fc % BATCH == 0:
                now = time.monotonic()
                if now - last_chk >= 0.25:
                    bursting = time.time() < shared.get("burst_until", 0.0)
                    last_chk = now
                if now - last_pub >= 0.4:
                    shared[key] = pipe.count
                    last_pub = now
                if burst_only and not bursting:  # burst ended -> go idle
                    break


# --------------------------------------------------------------------------- #
# reader thread (web process) — the inference retrieval path
# --------------------------------------------------------------------------- #
def _reader_thread(shared):
    cluster = make_cluster(PROFILE, "tuned")
    session = cluster.connect(KEYSPACE)
    ps = prepare_all(session)
    lat = deque(maxlen=3000)
    whist = deque()  # (t, total_writes) over a sliding window, for a smooth rate
    while _proc.get("on", True):
        # Freshness probe: write a sentinel feature, immediately read it back, and
        # time the write->visible round trip. This is the store's freshness floor —
        # how long after a feature is computed it becomes readable. At LOCAL_ONE
        # the write commits and is readable on that replica with no quorum wait, so
        # this lands in the hundreds of microseconds.
        ptok = int(time.time() * 1000)
        tp = time.perf_counter()
        session.execute(
            ps["wallet_coin"],
            ("__probe__", "__fresh__", 0.0, 0.0, 0.0, ptok, _ts(ptok)),
        )
        session.execute(ps["read_wallet_coin"], ("__probe__", "__fresh__"))
        fresh_us = (time.perf_counter() - tp) * 1e6

        hot = list(shared.get("hot", []))
        board = []
        for h in hot:
            coin = h["coin"]
            rows = {}
            for win in ("1m", "5m", "1h"):
                t0 = time.perf_counter()
                r = session.execute(ps["read_coin_window"], (coin, win)).one()
                lat.append((time.perf_counter() - t0) * 1000.0)
                rows[win] = dict(r._asdict()) if r else None
            sc = unusual_accumulation(rows["1m"], rows["5m"], rows["1h"])
            # smart-money flow over 5m (steadier than 1m), normalized by 5m volume
            b5 = rows["5m"] or {}
            smart = b5.get("smart_flow", 0.0) or 0.0
            vol5 = (b5.get("volume", 0.0) or 0.0) or 1e-9
            smart_norm = max(-1.0, min(1.0, smart / vol5))  # -1..1 for the bar
            # signal = accumulation + smart-money agreeing on direction
            if sc["score"] >= 0.5 and smart_norm > 0.05:
                action = "LONG"
            elif smart_norm < -0.15:
                action = "SHORT"
            else:
                action = "—"
            board.append(
                {
                    "coin": coin,
                    "score": sc["score"],
                    "label": sc["label"],
                    "smart": round(smart_norm, 4),
                    "action": action,
                    "vol": h["vol"],
                }
            )
        # rank by conviction: |smart-money flow| then score
        board.sort(key=lambda x: (abs(x["smart"]), x["score"]), reverse=True)
        s = sorted(lat)
        # grand total writes = paced ingest + the blaster fleet; rate from delta
        snap = dict(shared)
        total_w = snap.get("writes_total", 0) + sum(
            v for k, v in snap.items() if k.startswith("bw_")
        )
        now_w = time.monotonic()
        whist.append((now_w, total_w))
        while len(whist) > 1 and now_w - whist[0][0] > 2.5:  # ~2.5s window
            whist.popleft()
        wps = (
            (whist[-1][1] - whist[0][1]) / (whist[-1][0] - whist[0][0])
            if len(whist) > 1 and whist[-1][0] > whist[0][0]
            else 0.0
        )
        with _lock:
            STATS["scoreboard"] = board
            STATS["hot"] = sorted(hot, key=lambda x: x["vol"], reverse=True)
            STATS["fills_total"] = snap.get("fills_total", 0)
            STATS["writes_total"] = total_w
            STATS["fills_per_s"] = snap.get("fills_per_s", 0.0)
            STATS["writes_per_s"] = max(0.0, wps)
            STATS["wallets"] = snap.get("wallets", 0)
            STATS["active_coins"] = snap.get("active_coins", 0)
            STATS["arch"] = dict(snap.get("arch", {}))
            STATS["seeds"] = dict(snap.get("seeds", {}))
            STATS["vector_on"] = _proc.get("vps") is not None
            STATS["bursting"] = bool(snap.get("bursting", False))
            STATS["fresh_us"] = round(fresh_us, 1)
            dm = snap.get("data_time_ms")
            STATS["data_time"] = _ts(dm).isoformat() if dm else None
            if s:
                STATS["read_p99_ms"] = round(s[min(len(s) - 1, int(len(s) * 0.99))], 3)
        time.sleep(0.3)
    session.shutdown()
    cluster.shutdown()


@app.on_event("startup")
def _startup():
    ctx = mp.get_context("spawn")
    mgr = ctx.Manager()
    shared = mgr.dict()
    shared["hot"] = []
    shared["speed"] = SPEED
    shared["burst_until"] = 0.0
    p = ctx.Process(
        target=_ingest_proc, args=(shared, SPEED, DAYS, PROFILE), daemon=True
    )
    p.start()
    # baseline blasters (always full-speed) + burst-only blasters (idle until
    # BURST). Even-split over the TOTAL so each gets a distinct share.
    total_b = BLASTERS + BURST_BLASTERS
    blasters = []
    for w in range(total_b):
        burst_only = w >= BLASTERS
        b = ctx.Process(
            target=_blaster_proc,
            args=(w, total_b, DAYS, PROFILE, BLAST_INFLIGHT, burst_only, shared),
            daemon=True,
        )
        b.start()
        blasters.append(b)
    _proc.update(on=True, p=p, blasters=blasters, mgr=mgr, shared=shared)
    # web-process session for the on-demand similarity endpoint (the reader
    # thread keeps its own; driver sessions are thread-safe either way)
    cluster = make_cluster(PROFILE, "tuned")
    session = cluster.connect(KEYSPACE)
    _proc["cluster"] = cluster
    _proc["session"] = session
    _proc["ps"] = prepare_all(session)
    _proc["vps"] = prepare_vector(session)
    time.sleep(1.5)
    threading.Thread(target=_reader_thread, args=(shared,), daemon=True).start()


@app.on_event("shutdown")
def _shutdown():
    _proc["on"] = False
    for b in _proc.get("blasters", []):
        b.terminate()
    if _proc.get("p"):
        _proc["p"].terminate()
    if _proc.get("session"):
        _proc["session"].shutdown()
        _proc["cluster"].shutdown()


@app.post("/burst")
def burst(secs: float = BURST_SECS):
    """Spike replay to max speed for `secs` — drives a write burst on demand."""
    _proc["shared"]["burst_until"] = time.time() + secs
    return {"bursting_for_s": secs}


@app.get("/stats")
def stats():
    with _lock:
        return dict(STATS)


@app.get("/similar/{addr}")
def similar(addr: str, k: int = 8):
    """ANN neighbours + feature point-reads for the Similar-wallets tab."""
    vps = _proc.get("vps")
    if vps is None:
        raise HTTPException(503, "vector schema not applied (just schema-vector)")
    out = similarity.similar_wallets(_proc["session"], _proc["ps"], vps, addr, k)
    if out is None:
        raise HTTPException(404, "no embedding for this wallet yet")
    return out


@app.get("/graph")
def graph(k: int = 5, min_cos: float = 0.985):
    """Behaviour-similarity network for the GRAPH tab: fan the ANN neighbour
    lookup over the current whale/smart-money seeds and return nodes + edges.
    Edges are behavioural links (trades alike), NOT counterparty flows."""
    vps = _proc.get("vps")
    if vps is None:
        raise HTTPException(503, "vector schema not applied (just schema-vector)")
    session, ps = _proc["session"], _proc["ps"]
    with _lock:
        seeds = dict(STATS.get("seeds") or {})
    kinds: dict[str, str] = {}
    for w in seeds.get("whales", []):
        kinds[w["addr"]] = "whale"
    for w in seeds.get("smart", []):
        kinds[w["addr"]] = "both" if w["addr"] in kinds else "smart"

    t0 = time.perf_counter()
    nodes: dict[str, dict] = {}
    edges: dict[frozenset, dict] = {}

    def add_node(addr: str, row: dict, kind: str) -> None:
        nodes.setdefault(addr, {"id": addr, "kind": kind}).update(
            gross=row.get("gross_volume") or 0.0,
            pnl=row.get("cum_realized_pnl") or 0.0,
            arch=row.get("archetype") or "?",
        )

    for addr, kind in kinds.items():
        srow = session.execute(ps["read_wallet"], (addr,)).one()
        if not srow:
            continue
        srow = dict(srow._asdict())
        emb = srow.pop("embedding", None)
        if emb is None:
            continue
        seed_vec = list(emb)
        add_node(addr, srow, kind)
        tight = 0
        for r in session.execute(vps["ann_wallets"], (seed_vec, k + 1)):
            if r.addr == addr:
                continue
            nrow = session.execute(ps["read_wallet"], (r.addr,)).one()
            if not nrow:
                continue
            nrow = dict(nrow._asdict())
            nvec = nrow.pop("embedding", None)
            if nvec is None:
                continue
            cos = cosine(seed_vec, list(nvec))
            if cos < min_cos:
                continue
            if r.addr not in kinds:
                add_node(r.addr, nrow, "neighbour")
            edges[frozenset((addr, r.addr))] = {"a": addr, "b": r.addr,
                                                "cos": round(cos, 4)}
            if cos >= similarity.RING_COSINE:
                tight += 1
        if tight >= similarity.RING_MIN_NEIGHBOURS:
            nodes[addr]["ring"] = True
            for e in edges.values():
                if addr in (e["a"], e["b"]) and e["cos"] >= similarity.RING_COSINE:
                    other = e["b"] if e["a"] == addr else e["a"]
                    if other in nodes:
                        nodes[other]["ring"] = True
    return {
        "nodes": list(nodes.values()),
        "edges": list(edges.values()),
        "ms": round((time.perf_counter() - t0) * 1000, 1),
    }


@app.get("/", response_class=HTMLResponse)
def index():
    return HTML.replace("__SCYLLA__", _LOGO_SCYLLA).replace("__HL__", _LOGO_HL)


HTML = """
<!doctype html><html><head><meta charset=utf-8>
<title>ScyllaDB Feature Store — LIVE</title>
<style>
 :root{--bg:#0b0b0d;--card:#131316;--bd:#26262b;--fg:#e4e4e7;--mut:#71717a;
       --dim:#3f3f46;--up:#26a69a;--dn:#ef5350}
 *{box-sizing:border-box}
 body{background:var(--bg);color:var(--fg);font:13px/1.45 -apple-system,system-ui,Segoe UI,Roboto,sans-serif;margin:0;padding:22px;font-variant-numeric:tabular-nums}
 header{display:flex;justify-content:space-between;align-items:center;margin:0 0 18px;max-width:1180px}
 .brand{display:flex;align-items:center;gap:12px}
 .brand img{display:block}
 .title{font-size:16px;font-weight:600;letter-spacing:.2px}
 .title b{color:var(--mut);font-weight:600}
 .ctl{display:flex;align-items:center;gap:10px}
 .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;max-width:1180px}
 .card{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:16px}
 .big{font-size:34px;font-weight:700;color:var(--fg)}.unit{font-size:12px;color:--mut;color:var(--mut)}
 .row{display:flex;justify-content:space-between;align-items:center;margin:5px 0}
 .coin{width:96px;font-weight:600}
 .score{width:40px;text-align:right;color:var(--mut)}
 canvas{width:100%;display:block}
 .sub{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.6px}
 .pill{display:inline-block;padding:2px 8px;border-radius:6px;border:1px solid var(--bd);color:var(--mut);font-size:11px}
 button{background:var(--fg);color:var(--bg);border:0;border-radius:8px;padding:8px 14px;font-weight:700;cursor:pointer;font-size:13px;letter-spacing:.3px}
 button:hover{filter:brightness(.92)} .burston{outline:2px solid var(--fg);outline-offset:2px}
 .legend{font-size:10px;color:var(--mut);text-transform:none;letter-spacing:0}
 .gbar{height:12px;background:var(--dim);border-radius:3px}
 .flow{display:flex;flex:1;height:12px;margin:0 10px}
 .fhalf{flex:1;display:flex;height:12px}.fbar{height:12px}
 .sell{background:var(--dn);border-radius:3px 0 0 3px;margin-left:auto}
 .buy{background:var(--up);border-radius:0 3px 3px 0}
 .chip{width:52px;text-align:center;font-size:11px;font-weight:700;border-radius:5px;padding:2px 0;margin-left:12px}
 .long{background:var(--up);color:var(--bg)}.short{background:var(--dn);color:var(--bg)}
 .flat{background:transparent;color:var(--mut);border:1px solid var(--bd)}
 .seg{height:18px;display:inline-block}.achip{font-size:11px;margin-right:14px}
 .ampend{position:absolute;top:50%;transform:translateY(-50%)}
 .ampx{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);font-size:12px;
       font-weight:700;color:var(--mut);background:var(--card);border:1px solid var(--bd);
       border-radius:6px;padding:2px 10px;white-space:nowrap}
 .tabs{display:flex;gap:8px}
 .tab{background:transparent;color:var(--mut);border:1px solid var(--bd);border-radius:8px;
      padding:7px 14px;font-weight:700;cursor:pointer;font-size:13px;letter-spacing:.3px}
 .tab.on{background:var(--fg);color:var(--bg);border-color:var(--fg)}
 .tab:hover{filter:brightness(1.2)}
 .addr{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
 .seedrow{cursor:pointer;border-radius:6px;padding:3px 6px;margin:2px -6px}
 .seedrow:hover{background:var(--dim)} .seedrow.sel{outline:1px solid var(--fg)}
 .cosbar{height:8px;background:var(--dim);border-radius:3px}
 .cosfill{height:8px;background:#2bd6c6;border-radius:3px}
 .ring{background:rgba(239,83,80,.12);border:1px solid var(--dn);border-radius:8px;
       padding:8px 12px;margin:10px 0;font-weight:600;font-size:12px}
 .whychip{display:inline-block;font-size:10px;color:var(--mut);border:1px solid var(--bd);
          border-radius:4px;padding:1px 5px;margin-right:4px}
 .pos{color:var(--up)}.neg{color:var(--dn)}
 input.waddr{background:var(--bg);border:1px solid var(--bd);border-radius:8px;color:var(--fg);
             padding:7px 10px;width:100%;font-family:ui-monospace,Menlo,monospace;font-size:12px}
</style></head><body>
<header>
  <div class=brand>
    <img src="__SCYLLA__" height=28>
    <img src="__HL__" height=22 style=opacity:.85>
    <span class=title>ScyllaDB + Hyperliquid Feature Store <b></span>
  </div>
  <div class=ctl>
    <div class=tabs>
      <button id=tabLive class="tab on" onclick=showTab('live')>LIVE</button>
      <button id=tabSim class=tab onclick=showTab('sim')>SIMILAR WALLETS</button>
      <button id=tabGraph class=tab onclick=showTab('graph')>GRAPH</button>
    </div>
    <button id=burst onclick=doBurst()>⚡ BURST</button>
    <span class=pill id=burststate>steady</span>
  </div>
</header>
<div class=grid id=tab-live>
  <div class=card>
    <div class=sub>Hyperliquid Validator</div>
    <div class=big id=fps>0<span class=unit> fills/s</span></div>
    <canvas id=spark width=540 height=52></canvas>
    <div class=row><span class=sub>fills total</span><b id=ftot>0</b></div>
    <div class=row><span class=sub>active coins / wallets</span><b id=card>0</b></div>
    <div class=row><span class=sub>data clock</span><span class=pill id=clock>—</span></div>
  </div>
  <div class=card>
    <div class=sub>ScyllaDB Feature Store</div>
    <div class=big><span id=p99>0.000</span><span class=unit> ms</span> <span class=legend><b style=color:#2bd6c6>● read p99 (ms)</b> &nbsp;<b style=color:#e4e4e7>● writes/s</b></span></div>
    <canvas id=chart width=540 height=52></canvas>
    <div class=row><span class=sub>feature freshness (Δt = read - write)</span><b id=fresh>—</b></div>
    <div class=row><span class=sub>writes/s</span><b id=wps>0</b></div>
  </div>
  <div class=card style=grid-column:1/3;position:relative;padding:8px>
    <canvas id=amp width=1130 height=64></canvas>
    <span class="sub ampend" style=left:14px>fills</span>
    <span class="sub ampend" style=right:14px>writes</span>
    <span class=ampx id=ampx>× —</span>
  </div>
  <div class=card>
    <div class=sub>Write load by coin</div>
    <div id=hot></div>
  </div>
  <div class=card>
    <div class=sub>Cross-ticker signal board</div>
    <div id=board></div>
  </div>
  <div class=card style=grid-column:1/3>
    <div class=sub>Wallet archetype mix</div>
    <div id=archbar style=margin:8px 0></div>
    <div id=archlegend></div>
  </div>
</div>
<div class=grid id=tab-sim style=display:none>
  <div class=card>
    <div class=sub>Pick a wallet — behaviour neighbours via ANN, same engine</div>
    <div style=margin:10px 0>
      <input class=waddr id=waddr placeholder="paste a wallet address…"
             onkeydown="if(event.key=='Enter')pick(this.value.trim())">
    </div>
    <div class=sub style=margin-top:12px>🐋 whales — largest gross volume</div>
    <div id=whales></div>
    <div class=sub style=margin-top:12px>🧠 smart money — profitable + directional</div>
    <div id=smart></div>
  </div>
  <div class=card>
    <div class=sub>Nearest wallets <span class=legend>ORDER BY embedding ANN OF &lt;seed&gt; · refreshed every 3s while the firehose runs</span></div>
    <div id=simseed style=margin:10px 0></div>
    <div id=simring></div>
    <div id=simout><span class=sub>select a wallet on the left</span></div>
    <div class=row style=margin-top:10px><span class=sub id=simlat></span></div>
  </div>
</div>
<div class=grid id=tab-graph style=display:none>
  <div class=card style=grid-column:1/3>
    <div class=sub>Behaviour-similarity network
      <span class=legend>wallets that trade alike (ANN neighbours, cosine ≥ 0.985) — behavioural links, not counterparty flows · new wallets join as the index picks them up</span></div>
    <canvas id=gcanvas width=1130 height=560 style=cursor:pointer></canvas>
    <div class=row>
      <span class=sub id=glat></span>
      <span class=legend><b style=color:#e4e4e7>●</b> whale &nbsp;<b style=color:#2bd6c6>●</b> smart money
        &nbsp;<b style=color:#9be8df>●</b> both &nbsp;<b style=color:#71717a>●</b> neighbour
        &nbsp;<b style=color:#ef5350>◦</b> ring-tight — click a node to inspect it</span>
    </div>
  </div>
</div>
<script>
const sH=[],pH=[],wH=[];
const sp=document.getElementById('spark'),sc=sp.getContext('2d');
const ch=document.getElementById('chart'),cc=ch.getContext('2d');
const FG='#e4e4e7',MUT='#71717a',TEAL='#2bd6c6';
function line(ctx,cv,arr,color,max,dash){if(arr.length<2)return;ctx.setLineDash(dash||[]);ctx.beginPath();ctx.strokeStyle=color;ctx.lineWidth=1.8;
 arr.forEach((v,i)=>{const x=i/(arr.length-1)*cv.width,y=cv.height-(v/(max||1))*cv.height*0.92-4;i?ctx.lineTo(x,y):ctx.moveTo(x,y)});ctx.stroke();ctx.setLineDash([]);}
function drawSpark(){sc.clearRect(0,0,sp.width,sp.height);line(sc,sp,sH,FG,Math.max(...sH,1));}
function drawChart(){cc.clearRect(0,0,ch.width,ch.height);
 line(cc,ch,wH,FG,Math.max(...wH,1));       // writes/s (white)
 line(cc,ch,pH,TEAL,Math.max(...pH,2));}     // read p99 (ScyllaDB teal, own scale)
function fmt(n){return n>=1000?(n/1000).toFixed(1)+'k':Math.round(n)}
function doBurst(){fetch('/burst',{method:'POST'});}
// --- write-amplification flow: fills (white) split into writes (teal) ---
const am=document.getElementById('amp'),ac=am.getContext('2d');
let liveFps=0,liveWps=0,parts=[],spawnAcc=0,lastT=performance.now();
function ampTick(t){
 const dt=Math.min((t-lastT)/1000,.05);lastT=t;
 const mid=am.width*.45;
 // one particle ≈ 100 fills/s; keep the pipe legible at any rate
 spawnAcc+=Math.min(30,Math.max(3,liveFps/100))*dt;
 while(spawnAcc>1&&parts.length<500){spawnAcc--;
  parts.push({x:0,y:am.height/2+(Math.random()*26-13),v:150+Math.random()*70,vy:0,teal:0});}
 const k=Math.min(6,Math.max(2,Math.round(liveWps/Math.max(liveFps,1)/15)));
 const next=[];
 for(const p of parts){
  p.x+=p.v*dt;p.y+=p.vy*dt;
  if(!p.teal&&p.x>=mid){ // amplification point: 1 fill -> k writes
   for(let i=0;i<k;i++)next.push({x:mid,y:p.y,v:p.v*1.15,vy:(i-(k-1)/2)*16,teal:1});
   continue;}
  if(p.x<am.width&&p.y>2&&p.y<am.height-2)next.push(p);}
 parts=next;
 ac.clearRect(0,0,am.width,am.height);
 for(const p of parts){ac.fillStyle=p.teal?'rgba(43,214,198,.8)':'rgba(228,228,231,.85)';
  ac.beginPath();ac.arc(p.x,p.y,p.teal?1.5:2,0,7);ac.fill();}
 requestAnimationFrame(ampTick);}
requestAnimationFrame(ampTick);
const COL={'directional':'#e4e4e7','mixed':'#71717a','market-maker':'#3f3f46'};
// --- Similar-wallets tab -------------------------------------------------
let curTab='live',simActive=false,selAddr=null;
function showTab(t){curTab=t;simActive=(t=='sim');
 for(const[id,btn]of[['tab-live',tabLive],['tab-sim',tabSim],['tab-graph',tabGraph]]){
  const on=id=='tab-'+t;
  document.getElementById(id).style.display=on?'grid':'none';
  btn.className='tab'+(on?' on':'');}
 if(t=='sim'&&selAddr)fetchSim();
 if(t=='graph')fetchGraph();}
function shorten(a){return a.length>14?a.slice(0,8)+'…'+a.slice(-4):a}
function pnlFmt(p){const c=p>=0?'pos':'neg',s=p>=0?'+':'−';return `<b class=${c}>${s}$${fmt(Math.abs(p))}</b>`}
function seedRows(el,list){el.innerHTML=(list||[]).map(w=>
 `<div class="seedrow row${w.addr==selAddr?' sel':''}" onclick="pick('${w.addr}')" title="${w.addr}">`+
 `<span class=addr>${shorten(w.addr)}</span><span class=sub>${w.arch}</span>`+
 `<span style=width:120px;text-align:right>$${fmt(w.gross)} · ${pnlFmt(w.pnl)}</span></div>`).join('')
 ||'<span class=sub>warming up…</span>';}
function pick(a){if(!a)return;selAddr=a;waddr.value=a;fetchSim();}
async function fetchSim(){if(!selAddr)return;
 try{const r=await fetch('/similar/'+selAddr+'?k=8');
  if(!r.ok){simout.innerHTML=`<span class=sub>${(await r.json()).detail||r.status}</span>`;simseed.innerHTML='';simring.innerHTML='';return;}
  renderSim(await r.json());}catch(e){}}
function renderSim(d){const s=d.seed;
 simseed.innerHTML=`<span class=addr title="${s.addr}"><b>${shorten(s.addr)}</b></span> `+
  `<span class=pill>${s.archetype}</span> <span class=sub>gross</span> $${fmt(s.gross_volume)} `+
  `<span class=sub>pnl</span> ${pnlFmt(s.cum_realized_pnl)} <span class=sub>fills</span> ${s.total_fills.toLocaleString()}`;
 simring.innerHTML=d.ring_suspect?`<div class=ring>⚠ ${d.ring_size} near-identical neighbours (cosine ≥ 0.995) — possible coordinated ring / wash-trading cluster</div>`:'';
 simout.innerHTML=d.neighbours.map(n=>{const cw=Math.max(0,(n.cosine-0.9)/0.1)*100;
  const why=(n.why&&n.why.most_alike||[]).map(w=>`<span class=whychip>${w.dim}</span>`).join('');
  return `<div class=row title="${n.addr}">`+
   `<span class=addr style=width:110px>${shorten(n.addr)}</span>`+
   `<div style=flex:1;margin:0 10px><div class=cosbar><div class=cosfill style=width:${cw}%></div></div></div>`+
   `<span class=score style=width:52px>${n.cosine.toFixed(4)}</span>`+
   `<span class=sub style=width:90px;text-align:right>${n.archetype}</span>`+
   `<span style=width:90px;text-align:right>${pnlFmt(n.cum_realized_pnl)}</span>`+
   `</div><div style=margin:-2px 0 6px 0>${why}</div>`}).join('')
  ||'<span class=sub>no neighbours yet — the index is warming up</span>';
 simlat.textContent=`ANN ${d.ann_ms} ms · ${d.neighbours.length} feature point-reads ${d.point_reads_ms} ms — one CQL session`;}
setInterval(()=>{if(simActive&&selAddr)fetchSim()},3000);
// --- GRAPH tab: behaviour-similarity network ------------------------------
const G={nodes:new Map(),edges:new Map(),ms:0};
const gc=document.getElementById('gcanvas'),gx=gc.getContext('2d');
const KCOL={whale:'#e4e4e7',smart:'#2bd6c6',both:'#9be8df',neighbour:'#71717a'};
let hoverN=null;
async function fetchGraph(){try{
 const r=await fetch('/graph?k=5');if(!r.ok)return;
 const d=await r.json();G.ms=d.ms;
 const seen=new Set();
 for(const n of d.nodes){seen.add(n.id);
  let o=G.nodes.get(n.id);
  if(!o){ // spawn near a linked node if we already know one, else near centre
   let px=gc.width/2,py=gc.height/2;
   const e=d.edges.find(e=>e.a==n.id||e.b==n.id);
   if(e){const p=G.nodes.get(e.a==n.id?e.b:e.a);if(p){px=p.x;py=p.y;}}
   o={x:px+(Math.random()-.5)*60,y:py+(Math.random()-.5)*60,vx:0,vy:0,a:0};
   G.nodes.set(n.id,o);}
  Object.assign(o,{id:n.id,kind:n.kind,gross:n.gross,pnl:n.pnl,arch:n.arch,ring:n.ring,gone:0});}
 for(const[id,o]of G.nodes)if(!seen.has(id)&&++o.gone>2)G.nodes.delete(id);
 const ek=new Set();
 for(const e of d.edges){const k=e.a<e.b?e.a+'|'+e.b:e.b+'|'+e.a;ek.add(k);
  const ex=G.edges.get(k);
  if(ex)ex.cos=e.cos;else G.edges.set(k,{a:e.a,b:e.b,cos:e.cos,age:0});}
 for(const k of G.edges.keys())if(!ek.has(k))G.edges.delete(k);
}catch(err){}}
setInterval(()=>{if(curTab=='graph')fetchGraph()},5000);
function nodeR(n){return 3+Math.min(9,Math.log10(Math.max(n.gross||10,10)))}
function gTick(){
 if(curTab=='graph'){
  const ns=[...G.nodes.values()];
  for(let i=0;i<ns.length;i++){const p=ns[i];         // pairwise repulsion
   for(let j=i+1;j<ns.length;j++){const q=ns[j];
    let dx=p.x-q.x,dy=p.y-q.y;const d2=dx*dx+dy*dy+1,d=Math.sqrt(d2),f=1600/d2;
    dx/=d;dy/=d;p.vx+=dx*f;p.vy+=dy*f;q.vx-=dx*f;q.vy-=dy*f;}}
  for(const e of G.edges.values()){                    // springs: closer = more alike
   const p=G.nodes.get(e.a),q=G.nodes.get(e.b);if(!p||!q)continue;
   e.age++;
   const rest=34+Math.max(0,(1-e.cos))*2600;
   let dx=q.x-p.x,dy=q.y-p.y;const d=Math.sqrt(dx*dx+dy*dy)+.01,f=(d-rest)*0.02/d;
   p.vx+=dx*f;p.vy+=dy*f;q.vx-=dx*f;q.vy-=dy*f;}
  for(const p of ns){                                  // gravity + damping
   p.vx+=(gc.width/2-p.x)*0.0015;p.vy+=(gc.height/2-p.y)*0.0015;
   p.vx*=0.85;p.vy*=0.85;p.x+=p.vx;p.y+=p.vy;
   p.a=Math.min(1,(p.a||0)+0.04);
   p.x=Math.max(10,Math.min(gc.width-10,p.x));p.y=Math.max(10,Math.min(gc.height-10,p.y));}
  drawGraph(ns);}
 requestAnimationFrame(gTick);}
requestAnimationFrame(gTick);
function drawGraph(ns){gx.clearRect(0,0,gc.width,gc.height);
 for(const e of G.edges.values()){
  const p=G.nodes.get(e.a),q=G.nodes.get(e.b);if(!p||!q)continue;
  const t=Math.min(1,Math.max(0,(e.cos-0.985)/0.015));
  const al=Math.min(1,e.age/25)*(0.15+t*0.55);
  gx.strokeStyle=(p.ring&&q.ring)?`rgba(239,83,80,${al})`:`rgba(43,214,198,${al})`;
  gx.lineWidth=0.8+t*1.6;
  gx.beginPath();gx.moveTo(p.x,p.y);gx.lineTo(q.x,q.y);gx.stroke();}
 for(const n of ns){const r=nodeR(n);
  gx.globalAlpha=n.a;
  gx.fillStyle=KCOL[n.kind]||'#71717a';
  gx.beginPath();gx.arc(n.x,n.y,r,0,7);gx.fill();
  if(n.ring){gx.strokeStyle='#ef5350';gx.lineWidth=1.6;
   gx.beginPath();gx.arc(n.x,n.y,r+2.5,0,7);gx.stroke();}
  if(n.kind!='neighbour'){gx.fillStyle='#71717a';gx.font='10px ui-monospace,Menlo,monospace';
   gx.fillText(n.id.slice(0,6)+'…'+n.id.slice(-4),n.x+r+4,n.y+3);}
  gx.globalAlpha=1;}
 if(hoverN){const n=hoverN,txt=`${n.id}  ·  ${n.arch}  ·  gross $${fmt(n.gross)}  ·  pnl ${n.pnl>=0?'+':'−'}$${fmt(Math.abs(n.pnl))}`;
  gx.font='11px ui-monospace,Menlo,monospace';
  const tw=gx.measureText(txt).width;
  const tx=Math.min(n.x+12,gc.width-tw-16),ty=Math.max(n.y-14,16);
  gx.fillStyle='rgba(19,19,22,.95)';gx.strokeStyle='#26262b';
  gx.beginPath();gx.roundRect(tx-6,ty-12,tw+12,18,5);gx.fill();gx.stroke();
  gx.fillStyle='#e4e4e7';gx.fillText(txt,tx,ty+1);}
 glat.textContent=`${G.nodes.size} wallets · ${G.edges.size} links · graph rebuilt in ${G.ms} ms (ANN fan-out over the seeds)`;}
function gHit(ev){const b=gc.getBoundingClientRect();
 const mx=(ev.clientX-b.left)*gc.width/b.width,my=(ev.clientY-b.top)*gc.height/b.height;
 let best=null,bd=144;
 for(const n of G.nodes.values()){const dx=n.x-mx,dy=n.y-my,d=dx*dx+dy*dy;
  if(d<bd){bd=d;best=n;}}
 return best;}
gc.addEventListener('mousemove',ev=>{hoverN=gHit(ev);gc.style.cursor=hoverN?'pointer':'default'});
gc.addEventListener('mouseleave',()=>hoverN=null);
gc.addEventListener('click',ev=>{const n=gHit(ev);if(n){pick(n.id);showTab('sim');}});
const ws=new WebSocket('ws://'+location.host+'/ws');
ws.onmessage=e=>{const s=JSON.parse(e.data);
 if(s.seeds&&simActive){seedRows(document.getElementById('whales'),s.seeds.whales);
  seedRows(document.getElementById('smart'),s.seeds.smart);}
 if(!s.vector_on){tabSim.style.display='none';tabGraph.style.display='none';}
 fps.innerHTML=fmt(s.fills_per_s)+'<span class=unit> fills/s</span>';
 wps.textContent=fmt(s.writes_per_s); ftot.textContent=s.fills_total.toLocaleString();
 card.textContent=s.active_coins+' / '+s.wallets.toLocaleString();
 clock.textContent=(s.data_time||'—').replace('T',' ').slice(0,19);
 p99.textContent=s.read_p99_ms.toFixed(3);
 fresh.textContent=s.fresh_us<1000?s.fresh_us.toFixed(0)+' µs':(s.fresh_us/1000).toFixed(2)+' ms';
 liveFps=s.fills_per_s;liveWps=s.writes_per_s;
 ampx.textContent='× '+Math.round(s.writes_per_s/Math.max(s.fills_per_s,1))+' write amplification';
 burststate.textContent=s.bursting?'BURSTING':'steady';
 document.getElementById('burst').className=s.bursting?'burston':'';
 sH.push(s.fills_per_s); wH.push(s.writes_per_s); pH.push(s.read_p99_ms);
 [sH,wH,pH].forEach(a=>{if(a.length>120)a.shift()}); drawSpark(); drawChart();
 // write-load skew bars (monochrome)
 const hmx=Math.max(...s.hot.map(h=>h.vol),1);
 hot.innerHTML=s.hot.map(h=>`<div class=row><span class=coin>${h.coin}</span>`+
  `<div style=flex:1;margin:0 10px><div class=gbar style="width:${h.vol/hmx*100}%"></div></div>`+
  `<span class=sub style=width:62px;text-align:right>${fmt(h.vol)}</span></div>`).join('');
 // cross-ticker signal board: smart-money flow (diverging) + score + action
 board.innerHTML=s.scoreboard.map(b=>{const sm=b.smart||0,L=sm<0?(-sm*100):0,R=sm>0?(sm*100):0;
  const chip=b.action=='LONG'?'long':b.action=='SHORT'?'short':'flat';
  return `<div class=row><span class=coin>${b.coin}</span>`+
   `<div class=flow><div class=fhalf><div class="fbar sell" style=width:${L}%></div></div>`+
   `<div class=fhalf><div class="fbar buy" style=width:${R}%></div></div></div>`+
   `<span class=score>${b.score.toFixed(2)}</span>`+
   `<span class="chip ${chip}">${b.action}</span></div>`}).join('');
 // archetype mix (grayscale shades)
 const a=s.arch||{},tot=(a['market-maker']||0)+(a['directional']||0)+(a['mixed']||0)||1;
 archbar.innerHTML=['directional','mixed','market-maker'].map(k=>
  `<span class=seg style="width:${(a[k]||0)/tot*100}%;background:${COL[k]}"></span>`).join('');
 archlegend.innerHTML=['directional','mixed','market-maker'].map(k=>
  `<span class=achip><span style="color:${COL[k]}">■</span> ${k}: ${(a[k]||0).toLocaleString()}</span>`).join('');
};
</script></body></html>
"""


@app.websocket("/ws")
async def ws(socket: WebSocket):
    await socket.accept()
    try:
        while True:
            with _lock:
                payload = dict(STATS)
            await socket.send_json(payload)
            await asyncio.sleep(0.25)
    except Exception:
        return
