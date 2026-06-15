#!/usr/bin/env bash
# Bring up / tear down the 3-node ScyllaDB cluster with plain `docker run`
# (this host has no `docker compose` plugin). Mirrors docker/docker-compose.yml.
#
#   scripts/cluster.sh up      # create network + start 3 nodes
#   scripts/cluster.sh status  # nodetool status from node1
#   scripts/cluster.sh down    # remove containers (keeps volumes)
#   scripts/cluster.sh nuke    # remove containers AND volumes
set -euo pipefail
NET=fsdemo-net
IMG=scylladb/scylla:2026.1.5
COMMON="--seeds=172.31.0.11 --smp 6 --memory 10G --overprovisioned 1 --developer-mode 1 --api-address 0.0.0.0"

up() {
  docker network inspect $NET >/dev/null 2>&1 || docker network create --subnet 172.31.0.0/24 $NET
  docker run -d --name fsdemo-node1 --network $NET --ip 172.31.0.11 \
    -p 9042:9042 -p 9180:9180 -v fsdemo-data1:/var/lib/scylla $IMG $COMMON
  docker run -d --name fsdemo-node2 --network $NET --ip 172.31.0.12 \
    -v fsdemo-data2:/var/lib/scylla $IMG $COMMON
  docker run -d --name fsdemo-node3 --network $NET --ip 172.31.0.13 \
    -v fsdemo-data3:/var/lib/scylla $IMG $COMMON
}
status() { docker exec fsdemo-node1 nodetool status; }
down() { docker rm -f fsdemo-node1 fsdemo-node2 fsdemo-node3 2>/dev/null || true; }
nuke() { down; docker volume rm fsdemo-data1 fsdemo-data2 fsdemo-data3 2>/dev/null || true; }

case "${1:-up}" in
  up) up ;;
  status) status ;;
  down) down ;;
  nuke) nuke ;;
  *) echo "usage: $0 {up|status|down|nuke}"; exit 1 ;;
esac
