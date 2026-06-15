"""Connection profiles and cluster construction.

Two axes the talk cares about:

  * profile: where ScyllaDB lives ('local' Docker, or 'cloud').
  * tuning : 'tuned' (shard/token-aware, prepared, LOCAL_ONE) vs 'default'
             (round-robin, LOCAL_QUORUM) for the before/after benchmark.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import (
    TokenAwarePolicy,
    DCAwareRoundRobinPolicy,
    RoundRobinPolicy,
    ConstantReconnectionPolicy,
)
from cassandra import ConsistencyLevel

KEYSPACE = "feature_store"

# Local 3-node cluster bridge IPs from docker/docker-compose.yml. On Linux the
# host can reach these directly, so the driver's shard-aware routing works.
LOCAL_CONTACT_POINTS = os.environ.get(
    "FS_CONTACT_POINTS", "172.31.0.11,172.31.0.12,172.31.0.13"
).split(",")


@dataclass
class Profile:
    contact_points: list[str]
    port: int = 9042
    local_dc: str = "datacenter1"
    username: str | None = None
    password: str | None = None
    secure_bundle: str | None = None   # ScyllaDB Cloud connect bundle path


def _local() -> Profile:
    return Profile(contact_points=LOCAL_CONTACT_POINTS)


def _cloud() -> Profile:
    # Populate from env / a ScyllaDB Cloud connect bundle when creds are supplied.
    return Profile(
        contact_points=os.environ.get("FS_CLOUD_HOSTS", "").split(","),
        username=os.environ.get("FS_CLOUD_USER"),
        password=os.environ.get("FS_CLOUD_PASS"),
        local_dc=os.environ.get("FS_CLOUD_DC", "AWS_US_EAST_1"),
        secure_bundle=os.environ.get("FS_CLOUD_BUNDLE"),
    )


PROFILES = {"local": _local, "cloud": _cloud}


def make_cluster(profile: str = "local", tuning: str = "tuned") -> Cluster:
    """Build a Cluster.

    tuning='tuned'   : TokenAware(DCAware) + shard awareness + LOCAL_ONE.
    tuning='default' : plain RoundRobin + LOCAL_QUORUM  (the "before" picture).
    """
    p = PROFILES[profile]()

    if tuning == "tuned":
        lb = TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=p.local_dc))
        read_cl = ConsistencyLevel.LOCAL_ONE
        write_cl = ConsistencyLevel.LOCAL_ONE
    else:  # 'default' — intentionally un-tuned baseline
        lb = RoundRobinPolicy()
        read_cl = ConsistencyLevel.LOCAL_QUORUM
        write_cl = ConsistencyLevel.LOCAL_QUORUM

    profiles = {
        EXEC_PROFILE_DEFAULT: ExecutionProfile(
            load_balancing_policy=lb,
            consistency_level=read_cl,
            request_timeout=15.0,
        ),
        "write": ExecutionProfile(
            load_balancing_policy=lb,
            consistency_level=write_cl,
            request_timeout=15.0,
        ),
    }

    auth = None
    if p.username:
        from cassandra.auth import PlainTextAuthProvider

        auth = PlainTextAuthProvider(username=p.username, password=p.password)

    kwargs = dict(
        contact_points=[h for h in p.contact_points if h],
        port=p.port,
        execution_profiles=profiles,
        auth_provider=auth,
        reconnection_policy=ConstantReconnectionPolicy(delay=2.0),
        protocol_version=4,
    )
    # The scylla-driver negotiates per-shard connections automatically when the
    # load-balancing policy is token-aware; the 'default' baseline above uses a
    # plain RoundRobin policy, which defeats that routing on purpose.

    if p.secure_bundle:  # ScyllaDB Cloud
        kwargs.pop("contact_points")
        kwargs.pop("port")
        kwargs["scylla_cloud"] = p.secure_bundle

    return Cluster(**kwargs)
