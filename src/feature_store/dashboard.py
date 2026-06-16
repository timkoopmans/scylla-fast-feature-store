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
import multiprocessing as mp
import os
import threading
import time
from collections import deque
from datetime import timezone

from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse

# Cap polars threads — the ingest and the write-blaster procs read parquet; set
# before polars is imported (via .features/.replay below).
os.environ.setdefault("POLARS_MAX_THREADS", "2")

from .config import KEYSPACE, make_cluster
from .features import FeatureEngine
from .replay import iter_fills
from .scorer import unusual_accumulation
from .statements import prepare_all
from .writer import Pipeline

UTC = timezone.utc
SPEED = float(os.environ.get("FS_SPEED", "30"))
DAYS = int(os.environ.get("FS_DAYS", "3"))
PROFILE = os.environ.get("FS_PROFILE", "local")
BURST_SECS = float(os.environ.get("FS_BURST_SECS", "8"))
# Background write-load fleet: N processes re-upserting fills at max speed so the
# dashboard shows ScyllaDB absorbing real write load (the single paced ingest is
# GIL-bound at ~5k/s). 0 = off.
# Baseline blasters run flat-out (steady writes/s). BURST blasters sit idle until
# ⚡ BURST is pressed, then write full-speed for the burst window — so BURST *adds*
# write load on top of the baseline (no throttling of anything).
BLASTERS = int(os.environ.get("FS_BLASTERS", "0"))
BURST_BLASTERS = int(os.environ.get("FS_BURST_BLASTERS", "0"))
BLAST_INFLIGHT = int(os.environ.get("FS_BLAST_INFLIGHT", "2048"))

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
    pipe = Pipeline(session, max_inflight=2048)
    now0 = time.monotonic()
    last_t = last_flush = last_speedchk = now0
    last_fills = last_writes = 0
    n = 0
    next_emit = now0
    prev_ts = None
    eff_speed = base_speed

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
    time.sleep(1.5)
    threading.Thread(target=_reader_thread, args=(shared,), daemon=True).start()


@app.on_event("shutdown")
def _shutdown():
    _proc["on"] = False
    for b in _proc.get("blasters", []):
        b.terminate()
    if _proc.get("p"):
        _proc["p"].terminate()


@app.post("/burst")
def burst(secs: float = BURST_SECS):
    """Spike replay to max speed for `secs` — drives a write burst on demand."""
    _proc["shared"]["burst_until"] = time.time() + secs
    return {"bursting_for_s": secs}


@app.get("/stats")
def stats():
    with _lock:
        return dict(STATS)


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
 h1{font-size:16px;font-weight:600;letter-spacing:.2px;margin:0 0 16px;display:flex;align-items:center;gap:10px}
 .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;max-width:1180px}
 .card{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:16px}
 .big{font-size:34px;font-weight:700;color:var(--fg)}.unit{font-size:12px;color:--mut;color:var(--mut)}
 .row{display:flex;justify-content:space-between;align-items:center;margin:5px 0}
 .coin{width:96px;font-weight:600}
 .score{width:40px;text-align:right;color:var(--mut)}
 canvas{width:100%;display:block}
 .lat{font-size:26px;font-weight:700;color:var(--fg)}
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
</style></head><body>
<h1><img src="__SCYLLA__" height=30 style=vertical-align:middle>
  Feature Store — live replay of the Hyperliquid
  <img src="__HL__" height=24 style=vertical-align:middle> fills firehose
  <button id=burst onclick=doBurst()>⚡ BURST</button>
  <span class=pill id=burststate>steady</span></h1>
<div class=grid>
  <div class=card>
    <div class=sub>Firehose</div>
    <div class=big id=fps>0<span class=unit> fills/s</span></div>
    <canvas id=spark width=540 height=52></canvas>
    <div class=row><span class=sub>writes/s</span><b id=wps>0</b></div>
    <div class=row><span class=sub>fills total</span><b id=ftot>0</b></div>
    <div class=row><span class=sub>active coins / wallets</span><b id=card>0</b></div>
    <div class=row><span class=sub>data clock</span><span class=pill id=clock>—</span></div>
  </div>
  <div class=card>
    <div class=sub>Read tail vs write load &nbsp;<span class=legend><b style=color:#a78bfa>● read p99 (ms)</b> &nbsp;<b style=color:#6ea8fe>● writes/s</b></span></div>
    <canvas id=chart width=540 height=120></canvas>
    <div class=lat id=p99>0.000<span class=unit> ms &nbsp;p99 point read</span></div>
    <div class=row><span class=sub>feature freshness (write→read)</span><b id=fresh>—</b></div>
    <div class=sub style=text-transform:none;letter-spacing:0>point-reads stay fast while writes spike — hit BURST</div>
  </div>
  <div class=card>
    <div class=sub>Write load by coin — the skew that drives partition-key design</div>
    <div id=hot></div>
  </div>
  <div class=card>
    <div class=sub>Cross-ticker signal board</div>
    <div class=legend style=margin:2px 0 8px>smart-money flow (sell ◀ ▶ buy) · accumulation score · action</div>
    <div id=board></div>
  </div>
  <div class=card style=grid-column:1/3>
    <div class=sub>Wallet archetype mix</div>
    <div id=archbar style=margin:8px 0></div>
    <div id=archlegend></div>
  </div>
</div>
<script>
const sH=[],pH=[],wH=[];
const sp=document.getElementById('spark'),sc=sp.getContext('2d');
const ch=document.getElementById('chart'),cc=ch.getContext('2d');
const FG='#e4e4e7',MUT='#71717a',BLUE='#6ea8fe',PUR='#a78bfa';
function line(ctx,cv,arr,color,max,dash){if(arr.length<2)return;ctx.setLineDash(dash||[]);ctx.beginPath();ctx.strokeStyle=color;ctx.lineWidth=1.8;
 arr.forEach((v,i)=>{const x=i/(arr.length-1)*cv.width,y=cv.height-(v/(max||1))*cv.height*0.92-4;i?ctx.lineTo(x,y):ctx.moveTo(x,y)});ctx.stroke();ctx.setLineDash([]);}
function drawSpark(){sc.clearRect(0,0,sp.width,sp.height);line(sc,sp,sH,BLUE,Math.max(...sH,1));}
function drawChart(){cc.clearRect(0,0,ch.width,ch.height);
 line(cc,ch,wH,BLUE,Math.max(...wH,1));    // writes/s (azure)
 line(cc,ch,pH,PUR,Math.max(...pH,2));}     // read p99 (violet, own scale)
function fmt(n){return n>=1000?(n/1000).toFixed(1)+'k':Math.round(n)}
function doBurst(){fetch('/burst',{method:'POST'});}
const COL={'directional':'#e4e4e7','mixed':'#71717a','market-maker':'#3f3f46'};
const ws=new WebSocket('ws://'+location.host+'/ws');
ws.onmessage=e=>{const s=JSON.parse(e.data);
 fps.innerHTML=fmt(s.fills_per_s)+'<span class=unit> fills/s</span>';
 wps.textContent=fmt(s.writes_per_s); ftot.textContent=s.fills_total.toLocaleString();
 card.textContent=s.active_coins+' / '+s.wallets.toLocaleString();
 clock.textContent=(s.data_time||'—').replace('T',' ').slice(0,19);
 p99.innerHTML=s.read_p99_ms.toFixed(3)+'<span class=unit> ms &nbsp;p99 point read</span>';
 fresh.textContent=s.fresh_us<1000?s.fresh_us.toFixed(0)+' µs':(s.fresh_us/1000).toFixed(2)+' ms';
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
