"""Update-heavy workload generator.

Notion's block data is roughly 90% updates: people edit existing blocks far more
often than they create new ones. This generator reproduces that shape, because an
append-mostly benchmark tells you nothing about an upsert pipeline.
"""
from __future__ import annotations

import argparse
import os
import random
import time
import uuid

import psycopg2

TYPES = ("text", "heading", "bullet", "database_row", "toggle")


def connect():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "app"),
        user=os.getenv("PG_USER", "app"),
        password=os.getenv("PG_PASSWORD", "app"),
    )


def seed(cur, workspaces: int, blocks: int) -> list[str]:
    ids = []
    for _ in range(blocks):
        block_id = str(uuid.uuid4())
        cur.execute(
            "INSERT INTO blocks (id, workspace_id, parent_id, type, content)"
            " VALUES (%s, %s, NULL, %s, %s)",
            (block_id, f"ws-{random.randrange(workspaces):04d}", random.choice(TYPES), "seed"),
        )
        ids.append(block_id)
    return ids


def run(args) -> None:
    random.seed(args.seed)
    conn = connect()
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("SELECT id FROM blocks LIMIT %s", (args.pool,))
    ids = [row[0] for row in cur.fetchall()]
    if len(ids) < args.pool:
        ids += seed(cur, args.workspaces, args.pool - len(ids))
        conn.commit()
        print(f"[seed] pool is {len(ids)} blocks")

    counts = {"u": 0, "c": 0, "d": 0}
    started = time.time()
    for n in range(1, args.events + 1):
        roll = random.random()
        if roll < args.update_ratio and ids:
            block_id = random.choice(ids)
            cur.execute(
                "UPDATE blocks SET content = %s, version = version + 1, updated_at = now()"
                " WHERE id = %s",
                (f"edit-{n}-{random.randrange(10**6)}", block_id),
            )
            counts["u"] += 1
        elif roll < args.update_ratio + args.delete_ratio and ids:
            block_id = ids.pop(random.randrange(len(ids)))
            cur.execute("DELETE FROM blocks WHERE id = %s", (block_id,))
            counts["d"] += 1
        else:
            block_id = str(uuid.uuid4())
            cur.execute(
                "INSERT INTO blocks (id, workspace_id, parent_id, type, content)"
                " VALUES (%s, %s, %s, %s, %s)",
                (
                    block_id,
                    f"ws-{random.randrange(args.workspaces):04d}",
                    random.choice(ids) if ids else None,
                    random.choice(TYPES),
                    f"new-{n}",
                ),
            )
            ids.append(block_id)
            counts["c"] += 1

        if n % args.batch == 0:
            conn.commit()
            if args.rate:
                time.sleep(max(0.0, args.batch / args.rate - (time.time() - started) % 1))
        if args.drift_at and n == args.drift_at:
            apply_drift(conn)

    conn.commit()
    elapsed = time.time() - started
    total = sum(counts.values())
    print(
        f"[workload] {total} changes in {elapsed:.1f}s "
        f"({total / elapsed:,.0f}/s into Postgres) "
        f"updates={counts['u']} inserts={counts['c']} deletes={counts['d']} "
        f"update_ratio={counts['u'] / total:.2%}"
    )
    cur.close()
    conn.close()


def apply_drift(conn) -> None:
    """Add a column mid-stream: the schema-evolution path under load."""
    cur = conn.cursor()
    cur.execute("ALTER TABLE blocks ADD COLUMN IF NOT EXISTS archived boolean DEFAULT false")
    cur.execute("UPDATE blocks SET archived = true WHERE random() < 0.1")
    conn.commit()
    cur.close()
    print("[drift] added blocks.archived upstream and touched ~10% of rows", flush=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--events", type=int, default=200_000)
    p.add_argument("--pool", type=int, default=20_000, help="existing blocks available to edit")
    p.add_argument("--workspaces", type=int, default=200)
    p.add_argument("--update-ratio", type=float, default=0.85)
    p.add_argument("--delete-ratio", type=float, default=0.03)
    p.add_argument("--batch", type=int, default=500, help="rows per Postgres commit")
    p.add_argument("--rate", type=int, default=0, help="target changes/sec, 0 = as fast as possible")
    p.add_argument("--drift-at", type=int, default=0, help="event number at which to add a column")
    p.add_argument("--seed", type=int, default=7)
    args = p.parse_args()
    if args.update_ratio + args.delete_ratio > 1:
        raise SystemExit("update_ratio + delete_ratio must be <= 1")
    run(args)


if __name__ == "__main__":
    main()
