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
