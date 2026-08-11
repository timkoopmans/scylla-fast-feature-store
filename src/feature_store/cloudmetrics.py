"""Server-side metrics from the ScyllaDB Cloud Prometheus proxy.

Why: every latency the dashboard measures itself is recorded INSIDE a Python
process that is competing with the load generator for CPU, so at high write
rates it reports client queuing as if it were database latency. These numbers
are recorded by the cluster and the Vector Store themselves and don't care what
the loader is doing.

They answer a different question — "what did the database do", not "what did the
application see" — so the dashboard shows both, labelled.

Setup: Cloud console -> Actions -> Enable Cluster Metrics -> Setup, which gives
a scrape config carrying the cluster id and bearer token. Feed those in as
FS_METRICS_URL / FS_METRICS_TOKEN.

Percentiles come from *deltas* between two scrapes: the exported histograms are
cumulative since node start, so quantiles over the raw buckets would describe
the cluster's whole lifetime rather than what is happening on stage now.
"""
from __future__ import annotations

import gzip
import os
import time
import urllib.parse
import urllib.request

URL = os.environ.get("FS_METRICS_URL", "")
TOKEN = os.environ.get("FS_METRICS_TOKEN", "")
# scheduling group carrying user workload (vs gossip/compaction/etc)
USER_SG = "sl:default"


def _parse_labels(s: str) -> dict:
    out = {}
    for part in s.split(","):
        if "=" not in part:
            continue
        k, _, v = part.partition("=")
        out[k.strip()] = v.strip().strip('"')
    return out


def scrape() -> dict:
    """Fetch the federate endpoint -> {metric_name: [(labels, value), ...]}."""
    if not URL or not TOKEN:
        return {}
    q = urllib.parse.urlencode({"match[]": '{job=~".+"}'})
    req = urllib.request.Request(
        f"{URL}?{q}",
        headers={"Authorization": f"Bearer {TOKEN}", "Accept-Encoding": "gzip"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip" or raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
    out: dict[str, list] = {}
    for line in raw.decode("utf-8", "replace").splitlines():
        if not line or line[0] == "#":
            continue
        name, _, rest = line.partition("{")
        if not rest:
            continue
        labels_s, _, tail = rest.partition("}")
        parts = tail.split()
        if not parts:
            continue
        try:
            val = float(parts[0])
        except ValueError:
            continue
        out.setdefault(name, []).append((_parse_labels(labels_s), val))
    return out


def _sum(samples, **match) -> float:
    return sum(v for lb, v in samples
               if all(lb.get(k) == w for k, w in match.items()))


def _p99_from_buckets(before, after, **match) -> float:
    """p99 over the delta between two cumulative histogram scrapes.

    Buckets are summed across nodes first (a cluster-wide histogram), then the
    99th percentile bucket boundary is returned. Bucket granularity is coarse,
    so this is the bucket's upper bound — an upper bound on the real p99, which
    is the honest direction to err.
    """
    def by_le(samples):
        acc: dict[float, float] = {}
        for lb, v in samples:
            if not all(lb.get(k) == w for k, w in match.items()):
                continue
            le = lb.get("le")
            if le is None:
                continue
            acc[float("inf") if le == "+Inf" else float(le)] = acc.get(
                float("inf") if le == "+Inf" else float(le), 0.0) + v
        return acc

    b, a = by_le(before), by_le(after)
    delta = {le: a.get(le, 0.0) - b.get(le, 0.0) for le in a}
    total = delta.get(float("inf"), 0.0)
    if total <= 0:
        return 0.0
    target = total * 0.99
    for le in sorted(delta):
        if delta[le] >= target:
            return le
    return float("inf")


def collect(prev: dict | None = None) -> tuple[dict, dict]:
    """Return (metrics_for_display, raw_scrape_for_next_call)."""
    now = scrape()
    if not now:
        return {}, {}
    if not prev or "_t" not in prev:
        now["_t"] = time.time()
        return {}, now
    dt = time.time() - prev["_t"]
    if dt <= 0:
        return {}, prev

    out: dict = {}

    # --- database: coordinator ops/s and latency (microsecond buckets) -------
    for op in ("read", "write"):
        cnt = f"scylla_storage_proxy_coordinator_{op}_latency_count"
        bkt = f"scylla_storage_proxy_coordinator_{op}_latency_bucket"
        if cnt in now and cnt in prev:
            d = _sum(now[cnt], scheduling_group_name=USER_SG) - \
                _sum(prev[cnt], scheduling_group_name=USER_SG)
            out[f"srv_{op}_ops"] = max(0.0, d / dt)
        if bkt in now and bkt in prev:
            us = _p99_from_buckets(prev[bkt], now[bkt], scheduling_group_name=USER_SG)
            out[f"srv_{op}_p99_ms"] = round(us / 1000.0, 3) if us != float("inf") else None

    # --- vector store: per-index size, query rate, latency (second buckets) --
    idx: dict[str, dict] = {}
    for lb, v in now.get("index_size", []):
        name = lb.get("index_name")
        if name:
            # same index is reported by every VS node — take max, not sum
            idx.setdefault(name, {})["vectors"] = max(idx.get(name, {}).get("vectors", 0), int(v))
    if "request_latency_seconds_count" in now and "request_latency_seconds_count" in prev:
        names = {lb.get("index_name") for lb, _ in now["request_latency_seconds_count"]}
        for name in filter(None, names):
            d = _sum(now["request_latency_seconds_count"], index_name=name) - \
                _sum(prev["request_latency_seconds_count"], index_name=name)
            idx.setdefault(name, {})["qps"] = max(0.0, d / dt)
            sec = _p99_from_buckets(prev.get("request_latency_seconds_bucket", []),
                                    now.get("request_latency_seconds_bucket", []),
                                    index_name=name)
            idx[name]["p99_ms"] = round(sec * 1000.0, 3) if sec != float("inf") else None
    if idx:
        out["srv_indexes"] = idx

    now["_t"] = time.time()
    return out, now


if __name__ == "__main__":   # quick check: python -m feature_store.cloudmetrics
    import json

    _, snap = collect(None)
    time.sleep(15)
    m, _ = collect(snap)
    print(json.dumps(m, indent=2, default=str))
