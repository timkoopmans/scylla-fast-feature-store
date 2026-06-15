#!/usr/bin/env bash
# Convenience launchers for the demo components. Run from the repo root on the
# demo host (inside the venv environment; uses .venv/bin/python).
#
#   scripts/run.sh schema                 # apply cql/schema.cql
#   scripts/run.sh consume [args...]      # stream fills -> features -> ScyllaDB
#   scripts/run.sh bench [args...]        # read latency benchmark
#   scripts/run.sh loadgen [args...]      # write-throughput load test
#   scripts/run.sh api                    # inference endpoint (port 8080)
#   scripts/run.sh dashboard              # live web dashboard (port 8090)
#   scripts/run.sh dashboard-bg           # ...detached; logs to /tmp/fs-dashboard.log
#   scripts/run.sh stop-dashboard
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
export PYTHONPATH=src

case "${1:-}" in
  schema)   shift; exec $PY -m feature_store.apply_schema --schema cql/schema.cql "$@" ;;
  consume)  shift; exec $PY -m feature_store.consumer "$@" ;;
  bench)    shift; exec $PY -m feature_store.bench read "$@" ;;
  loadgen)  shift; exec $PY -m feature_store.loadgen "$@" ;;
  api)      shift; exec $PY -m uvicorn --app-dir src feature_store.api:app --host 0.0.0.0 --port 8080 "$@" ;;
  dashboard) shift; exec $PY -m uvicorn --app-dir src feature_store.dashboard:app --host 0.0.0.0 --port 8090 "$@" ;;
  dashboard-bg)
    setsid bash -c "cd '$PWD'; PYTHONPATH=src FS_SPEED=${FS_SPEED:-30} FS_DAYS=${FS_DAYS:-1} \
      $PY -m uvicorn --app-dir src feature_store.dashboard:app --host 0.0.0.0 --port 8090" \
      </dev/null >/tmp/fs-dashboard.log 2>&1 &
    echo "dashboard starting on :8090 (log: /tmp/fs-dashboard.log)" ;;
  stop-dashboard) fuser -k 8090/tcp 2>/dev/null && echo stopped || echo "not running" ;;
  *) echo "usage: $0 {schema|consume|bench|loadgen|api|dashboard|dashboard-bg|stop-dashboard} [args]"; exit 1 ;;
esac
