"""Apply cql/schema.cql to the target cluster.

    python -m feature_store.apply_schema --schema cql/schema.cql
"""
from __future__ import annotations

import argparse
import re

from .config import make_cluster


def split_statements(text: str):
    # strip line comments, then split on semicolons
    no_comments = re.sub(r"--[^\n]*", "", text)
    for stmt in no_comments.split(";"):
        s = stmt.strip()
        if s:
            yield s + ";"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="cql/schema.cql")
    ap.add_argument("--profile", default="local", choices=["local", "cloud"])
    args = ap.parse_args()

    cluster = make_cluster(args.profile, tuning="tuned")
    session = cluster.connect()

    # Create the keyspace with NetworkTopologyStrategy bound to the cluster's
    # actual local DC (datacenter1 locally, AWS_US_EAST_1 on ScyllaDB Cloud) so
    # this works on any target. The file's CREATE KEYSPACE IF NOT EXISTS is then
    # a no-op.
    dc = session.execute("SELECT data_center FROM system.local").one().data_center
    rf = min(3, 1 + len(list(session.execute("SELECT peer FROM system.peers"))))
    print(f"-> CREATE KEYSPACE feature_store ({dc} RF={rf}) ...")
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS feature_store WITH replication = "
        f"{{'class': 'NetworkTopologyStrategy', '{dc}': {rf}}}"
    )

    with open(args.schema) as fh:
        text = fh.read()
    for stmt in split_statements(text):
        head = " ".join(stmt.split()[:4])
        print(f"-> {head} ...")
        session.execute(stmt)
    print("schema applied.")
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
