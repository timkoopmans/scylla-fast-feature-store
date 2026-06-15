#!/usr/bin/env bash
# Stand up the ScyllaDB Monitoring stack (Prometheus + Grafana) pointed at the
# 3-node demo cluster. Run on the demo host.
#
#   scripts/monitoring.sh up     # clone (first run) + start
#   scripts/monitoring.sh down
#
# Grafana lands on http://<demo-host>:3000  (admin/admin), dashboards prefixed
# "Scylla". CQL nodes expose Prometheus metrics on 9180 (mapped on node1; the
# stack scrapes all three by bridge IP).
set -euo pipefail
DIR="${FS_MON_DIR:-$HOME/scylla-monitoring}"
VER="${FS_MON_VER:-4.10.0}"

up() {
  if [ ! -d "$DIR" ]; then
    git clone --depth 1 -b "scylla-monitoring-$VER" \
      https://github.com/scylladb/scylla-monitoring.git "$DIR" || \
      git clone --depth 1 https://github.com/scylladb/scylla-monitoring.git "$DIR"
  fi
  cat > "$DIR/prometheus/scylla_servers.yml" <<'YML'
- targets:
    - 172.31.0.11:9180
    - 172.31.0.12:9180
    - 172.31.0.13:9180
  labels:
    cluster: fsdemo
    dc: datacenter1
YML
  cd "$DIR"
  # join the same docker network so Prometheus can reach bridge IPs
  ./start-all.sh -d "$DIR/data" --no-loki --no-alertmanager || ./start-all.sh -d "$DIR/data"
  docker network connect fsdemo-net aprom 2>/dev/null || true
  echo "Grafana: http://<demo-host>:3000  (admin/admin)"
}
down() { cd "$DIR" && ./kill-all.sh; }

case "${1:-up}" in
  up) up ;;
  down) down ;;
  *) echo "usage: $0 {up|down}"; exit 1 ;;
esac
