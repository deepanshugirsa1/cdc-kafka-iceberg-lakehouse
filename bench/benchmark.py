"""Turn the stream's per-batch metrics into one defensible throughput number."""
from __future__ import annotations

import argparse
import json
import pathlib
import statistics

DEFAULT = "/opt/bench/batch_metrics.jsonl"


def load(path: pathlib.Path) -> list[dict]:
    if not path.exists():
        raise SystemExit(f"no metrics at {path} - run `make stream` and `make load` first")
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def summarize(batches: list[dict]) -> dict:
    active = [b for b in batches if b["rows"] > 0]
    if not active:
        raise SystemExit("no non-empty batches recorded")
    rows = sum(b["rows"] for b in active)
    wall = max(b["ended"] for b in active) - min(b["started"] for b in active)
    latencies = sorted(b["ended"] - b["started"] for b in active)
    busy = sum(latencies)
    return {
        "batches": len(active),
        "merged_rows": rows,
        "wall_seconds": round(wall, 2),
        "events_per_sec_end_to_end": round(rows / wall, 1) if wall > 0 else None,
        "events_per_sec_in_merge": round(rows / busy, 1) if busy > 0 else None,
        "batch_latency_p50_s": round(statistics.median(latencies), 2),
        "batch_latency_p95_s": round(latencies[int(len(latencies) * 0.95) - 1], 2),
        "max_batch_rows": max(b["rows"] for b in active),
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--metrics", default=DEFAULT)
    p.add_argument("--out", default="bench/report.json")
    args = p.parse_args()

    report = summarize(load(pathlib.Path(args.metrics)))
    pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(args.out).write_text(json.dumps(report, indent=2) + "\n")
    width = max(len(k) for k in report)
    for key, value in report.items():
        print(f"{key.ljust(width)}  {value}")
    print(f"\nwritten to {args.out}")


if __name__ == "__main__":
    main()
