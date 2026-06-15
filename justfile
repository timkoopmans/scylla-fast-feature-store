# Webinar 1 — fast feature store on ScyllaDB.
# Run these on the demo host (Docker + dataset + venv live there).
#   just            # list recipes
#   just setup cluster-up schema demo
#
# Recipe args are POSITIONAL, in the order shown by `just --list`, e.g.:
#   just consume 3 0          # days=3, speed=0
#   just bench 600000 16 6    # n, procs, threads
#   just loadgen 12 1 default # procs, days, tuning

py := ".venv/bin/python"
host := env_var_or_default("FS_HOST", "localhost")

export PYTHONPATH := "src"

# show available recipes
default:
    @just --list

# --- one-time setup -------------------------------------------------------
# create the venv and install deps (needs uv)
setup:
    uv venv --python 3.13 .venv
    uv pip install --python {{py}} scylla-driver polars pyarrow fastapi uvicorn websockets

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
    {{py}} -m feature_store.apply_schema --schema cql/schema.cql

# --- pipeline -------------------------------------------------------------
# stream fills -> features -> ScyllaDB. speed=0 is max; writes sample_keys.csv
consume days="1" speed="0" raw_sink="off":
    {{py}} -m feature_store.consumer --speed {{speed}} --days {{days}} \
        --raw-sink {{raw_sink}} --sample-out sample_keys.csv

# --- benchmarks -----------------------------------------------------------
# inference point-read latency
bench n="600000" procs="12" threads="6" tuning="tuned":
    {{py}} -m feature_store.bench read --keys sample_keys.csv \
        --n {{n}} --procs {{procs}} --threads {{threads}} --tuning {{tuning}}

# before/after: tuned vs default driver profile
bench-compare:
    @just bench tuning=tuned
    @just bench tuning=default

# write-throughput ceiling (multi-process loadgen)
loadgen procs="12" days="1" tuning="tuned":
    {{py}} -m feature_store.loadgen --procs {{procs}} --days {{days}} --tuning {{tuning}}

# --- services -------------------------------------------------------------
# inference endpoint -> http://<host>:8080/score/coin/BTC
api:
    {{py}} -m uvicorn --app-dir src feature_store.api:app --host 0.0.0.0 --port 8080

# live dashboard -> http://<host>:8090
dashboard speed="30" days="1":
    FS_SPEED={{speed}} FS_DAYS={{days}} \
        {{py}} -m uvicorn --app-dir src feature_store.dashboard:app --host 0.0.0.0 --port 8090

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
    FS_HOST={{host}} scripts/sync.sh
