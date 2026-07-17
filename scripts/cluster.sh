#!/usr/bin/env bash
# Bring up / tear down the 3-node ScyllaDB cluster with plain `docker run`
# (this host has no `docker compose` plugin). Mirrors docker/docker-compose.yml.
#
# Webinar 2 adds the Vector Store sidecar (ANN indexing service): the Scylla
# nodes are started with --vector-store-primary-uri so `vector_index` custom
# indexes are served by the fsdemo-vector container (USearch/HNSW, fed via CDC).
#
#   scripts/cluster.sh up      # create network + start 3 nodes + vector store
#   scripts/cluster.sh status  # nodetool status from node1
#   scripts/cluster.sh down    # remove containers (keeps volumes)
#   scripts/cluster.sh nuke    # remove containers AND volumes
set -euo pipefail
NET=fsdemo-net
IMG=scylladb/scylla:2026.1.5
VS_IMG=scylladb/vector-store:1.7.0
VS_IP=172.31.0.20
COMMON="--seeds=172.31.0.11 --smp 6 --memory 10G --overprovisioned 1 --developer-mode 1 --api-address 0.0.0.0 --vector-store-primary-uri http://$VS_IP:6080"

up() {
  docker network inspect $NET >/dev/null 2>&1 || docker network create --subnet 172.31.0.0/24 $NET
  docker run -d --name fsdemo-node1 --network $NET --ip 172.31.0.11 \
    -p 9042:9042 -p 9180:9180 -v fsdemo-data1:/var/lib/scylla $IMG $COMMON
  docker run -d --name fsdemo-node2 --network $NET --ip 172.31.0.12 \
    -v fsdemo-data2:/var/lib/scylla $IMG $COMMON
  docker run -d --name fsdemo-node3 --network $NET --ip 172.31.0.13 \
    -v fsdemo-data3:/var/lib/scylla $IMG $COMMON
  vs_up
}

vs_up() {
  # Wait for node1 to answer CQL before starting the vector store (it connects
  # to Scylla on boot), mirroring the compose healthcheck+depends_on pattern.
  echo -n "waiting for node1 CQL"
  for _ in $(seq 1 60); do
    if docker exec fsdemo-node1 cqlsh -e 'DESCRIBE KEYSPACES' >/dev/null 2>&1; then
      echo " up"
      docker run -d --name fsdemo-vector --network $NET --ip $VS_IP \
        -p 6080:6080 \
        -e VECTOR_STORE_URI="0.0.0.0:6080" \
        -e VECTOR_STORE_SCYLLADB_URI="172.31.0.11:9042" \
        $VS_IMG
      return
    fi
    echo -n "."
    sleep 5
  done
  echo " timed out — start it manually with: scripts/cluster.sh vs-up" >&2
  return 1
}

status() {
  docker exec fsdemo-node1 nodetool status
  echo
  docker ps --filter name=fsdemo-vector --format 'vector store: {{.Status}}'
}
down() { docker rm -f fsdemo-node1 fsdemo-node2 fsdemo-node3 fsdemo-vector 2>/dev/null || true; }
nuke() { down; docker volume rm fsdemo-data1 fsdemo-data2 fsdemo-data3 2>/dev/null || true; }

case "${1:-up}" in
  up) up ;;
  vs-up) vs_up ;;
  status) status ;;
  down) down ;;
  nuke) nuke ;;
  *) echo "usage: $0 {up|vs-up|status|down|nuke}"; exit 1 ;;
esac
