# Webinars 1+2 — fast feature store + vector search on ScyllaDB.
# Run these on the demo host (Docker + dataset + venv live there).
# `local-*` recipes run on a laptop against docker/docker-compose.local.yml.
#   just            # list recipes
#   just setup cluster-up schema demo
#
# Recipe args are POSITIONAL, in the order shown by `just --list`, e.g.:
#   just consume 3 0          # days=3, speed=0
#   just bench 600000 16 6    # n, procs, threads
#   just loadgen 12 1 default # procs, days, tuning

py := ".venv/bin/python"
host := env_var_or_default("FS_HOST", "localhost")
# remote demo host for the cloud dashboard/tunnel recipes, e.g. FS_REMOTE=ubuntu@1.2.3.4
remote := env_var_or_default("FS_REMOTE", "ubuntu@44.203.26.115")
rdir := env_var_or_default("FS_REMOTE_DIR", "scylla-fast-feature-store")

export PYTHONPATH := "src"

# show available recipes
default:
    @just --list

# --- one-time setup -------------------------------------------------------
# create the venv and install deps (needs uv)
setup:
    uv venv --python 3.13 .venv
    uv pip install --python {{ py }} scylla-driver polars pyarrow fastapi uvicorn websockets

# raise the kernel AIO limit ScyllaDB requires (needs sudo, once per host)
aio:
    sudo sysctl -w fs.aio-max-nr=1048576

# --- cluster --------------------------------------------------------------

# start the 3-node ScyllaDB cluster
cluster-up:
    scripts/cluster.sh up
cluster-status:
    scripts/cluster.sh status
cluster-down:
    scripts/cluster.sh down
cluster-nuke:
    scripts/cluster.sh nuke

# apply cql/schema.cql
schema:
    {{ py }} -m feature_store.apply_schema --schema cql/schema.cql

# apply the webinar-2 vector additions (embedding columns + ANN indexes)
schema-vector:
    {{ py }} -m feature_store.apply_schema --schema cql/schema_vector.cql

# --- local laptop cluster (single node + vector store, LIGHT LOAD ONLY) ----
# the real webinar runs on ScyllaDB Cloud; this is for iterating on a Mac
local-up:
    docker compose -f docker/docker-compose.local.yml -p fslocal up -d --wait
local-down:
    docker compose -f docker/docker-compose.local.yml -p fslocal down
local-nuke:
    docker compose -f docker/docker-compose.local.yml -p fslocal down -v

# live dashboard against the local single node (blasters off — light load).
# Keep speed modest: past ~2k fills/s the 2-vCPU node's ANN path starts
# returning empty results under write pressure (fine on Cloud, not on a laptop).
local-dashboard speed="10" days="1":
    FS_CONTACT_POINTS=127.0.0.1 FS_SPEED={{ speed }} FS_DAYS={{ days }} \
        FS_BLASTERS=0 FS_BURST_BLASTERS=0 \
        {{ py }} -m uvicorn --app-dir src feature_store.dashboard:app --host 127.0.0.1 --port 8090

# schema + light ingest + ANN smoke against the local single node
local-demo fills="200000":
    FS_CONTACT_POINTS=127.0.0.1 just schema
    FS_CONTACT_POINTS=127.0.0.1 just schema-vector
    FS_CONTACT_POINTS=127.0.0.1 {{ py }} -m feature_store.consumer \
        --speed 0 --days 1 --max-fills {{ fills }} --max-inflight 512
    FS_CONTACT_POINTS=127.0.0.1 {{ py }} -m feature_store.similarity smoke

# --- pipeline -------------------------------------------------------------
# stream fills -> features -> ScyllaDB. speed=0 is max; writes sample_keys.csv
consume days="1" speed="0" raw_sink="off":
    {{ py }} -m feature_store.consumer --speed {{ speed }} --days {{ days }} \
        --raw-sink {{ raw_sink }} --sample-out sample_keys.csv

# --- webinar 2: similarity ---------------------------------------------------
# k nearest wallets by behaviour (ANN) + their live features (point-reads)
similar mode id k="10":
    {{ py }} -m feature_store.similarity {{ mode }} {{ id }} --k {{ k }}

# end-to-end ANN sanity check (needs consumer to have run with embeddings on)
similar-smoke:
    {{ py }} -m feature_store.similarity smoke

# --- benchmarks -----------------------------------------------------------
# inference point-read latency
bench n="600000" procs="12" threads="6" tuning="tuned":
    {{ py }} -m feature_store.bench read --keys sample_keys.csv \
        --n {{ n }} --procs {{ procs }} --threads {{ threads }} --tuning {{ tuning }}

# before/after: tuned vs default driver profile
bench-compare:
    @just bench tuning=tuned
    @just bench tuning=default

# ANN query load + latency against the vector index (webinar 2). Same client
# shape as `bench`, so the ANN p99 and the point-read p99 are comparable.
bench-ann n="200000" procs="12" threads="8" k="10" seeds="5000":
    {{ py }} -m feature_store.bench ann --n {{ n }} --procs {{ procs }} \
        --threads {{ threads }} --k {{ k }} --seeds {{ seeds }}

# write-throughput ceiling (multi-process loadgen)
loadgen procs="12" days="1" tuning="tuned":
    {{ py }} -m feature_store.loadgen --procs {{ procs }} --days {{ days }} --tuning {{ tuning }}

# --- services -------------------------------------------------------------
# inference endpoint -> http://<host>:8080/score/coin/BTC
api:
    {{ py }} -m uvicorn --app-dir src feature_store.api:app --host 0.0.0.0 --port 8080

# live dashboard -> http://<host>:8090
dashboard speed="30" days="1":
    FS_SPEED={{ speed }} FS_DAYS={{ days }} \
        {{ py }} -m uvicorn --app-dir src feature_store.dashboard:app --host 0.0.0.0 --port 8090

# stop a detached dashboard (kills whatever listens on :8090)
dashboard-stop:
    -fuser -k 8090/tcp

# ScyllaDB Monitoring (Grafana on :3000)
monitoring-up:
    scripts/monitoring.sh up
monitoring-down:
    scripts/monitoring.sh down

# --- end-to-end -----------------------------------------------------------
# quick full run: assumes cluster is up. schema -> ingest 1 day -> read bench
demo:
    @just schema
    @just consume days=1 speed=0
    @just bench

# --- dev convenience (run from the Mac) -----------------------------------
# rsync this repo to $FS_HOST (set FS_HOST first)
sync:
    FS_HOST={{ host }} scripts/sync.sh

# --- remote demo control (run from your laptop; set FS_REMOTE=user@host) ---
# apply cql/schema.cql on the remote host against ScyllaDB Cloud
cloud-schema:
    ssh {{ remote }} "cd {{ rdir }} && source ~/.fs-cloud.env && \
        PYTHONPATH=src .venv/bin/python -m feature_store.apply_schema --schema cql/schema.cql"

# launch the live dashboard on the remote host against ScyllaDB Cloud (detached).
# assumes the repo + .venv + ~/.fs-cloud.env are already set up on the remote.
# blasters: background write-load procs (24 is the sweet spot on a 48-vCPU box).
# embed: how many baseline blasters ALSO write behaviour vectors — plain
# blasters never touch the vector index, so leave this >0 for webinar 2.
cloud-dashboard blasters="24" burst_blasters="16" speed="10" days="1" embed="6":
    #!/usr/bin/env bash
    set -euo pipefail
    [ -n "{{ remote }}" ] || { echo "set FS_REMOTE=user@host first"; exit 1; }
    if ssh {{ remote }} 'curl -sf -m3 http://127.0.0.1:8090/stats >/dev/null 2>&1'; then
      echo "dashboard already running on {{ remote }}:8090 — leaving it as-is"
      echo "(use 'just cloud-dashboard-restart' to change blasters/speed)"
    else
      ssh {{ remote }} "cd {{ rdir }} && source ~/.fs-cloud.env && \
        FS_SPEED={{ speed }} FS_DAYS={{ days }} FS_BLASTERS={{ blasters }} \
        FS_BURST_BLASTERS={{ burst_blasters }} FS_BLAST_EMBED={{ embed }} PYTHONPATH=src \
        nohup .venv/bin/python -m uvicorn --app-dir src feature_store.dashboard:app \
        --host 0.0.0.0 --port 8090 >/tmp/fs-dashboard.log 2>&1 & sleep 1; echo launched"
      echo "starting on {{ remote }}:8090 (baseline {{ blasters }} + {{ burst_blasters }} burst, {{ embed }} embedding)"
    fi

# ANN query load against Cloud, driven from the remote demo host
cloud-bench-ann n="200000" procs="12" threads="8" k="10":
    ssh {{ remote }} "cd {{ rdir }} && source ~/.fs-cloud.env && \
        PYTHONPATH=src .venv/bin/python -m feature_store.bench ann \
        --n {{ n }} --procs {{ procs }} --threads {{ threads }} --k {{ k }}"

# force a restart (kill + relaunch) — use to change blasters/speed/days
cloud-dashboard-restart blasters="24" burst_blasters="16" speed="0" days="1":
    @just cloud-dashboard-stop
    @sleep 2
    @just cloud-dashboard {{ blasters }} {{ burst_blasters }} {{ speed }} {{ days }}

# stop the remote dashboard AND its spawned blaster procs (fuser alone leaves
# the blasters orphaned). The [v] class stops the pattern matching this command.
cloud-dashboard-stop:
    ssh {{ remote }} 'pgrep -f "[v]env/bin/python" | xargs -r kill -9 2>/dev/null; fuser -k 8090/tcp 2>/dev/null; sleep 1; echo "stopped (python left: $(pgrep -cf "[v]env/bin/python"))"'

# tail the remote dashboard log
cloud-dashboard-log:
    ssh {{ remote }} "tail -n 40 /tmp/fs-dashboard.log"

# open an SSH tunnel from this laptop to the remote dashboard (Ctrl-C to close)
tunnel port="8090":
    @echo "tunnel localhost:{{ port }} -> {{ remote }}:8090  |  open http://localhost:{{ port }}"
    ssh -N -L {{ port }}:localhost:8090 {{ remote }}

# ONE COMMAND for the demo: launch the cloud dashboard + open the tunnel.
# Open http://localhost:8090 once it says tunnelling; Ctrl-C closes the tunnel
# (use 'just cloud-dashboard-stop' to stop the remote dashboard afterwards).
cloud-demo blasters="24" burst_blasters="16": (cloud-dashboard blasters burst_blasters)
    @echo "waiting for dashboard + blasters to ramp ..." && sleep 6
    @just tunnel
