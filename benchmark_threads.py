#!/usr/bin/env python3
"""
Benchmark parallel worker counts for run_crawls.py.

Runs the crawler with 10 runs, 4 hops, 3s watch time for each worker
count from 1 to 10, records wall-clock time, and plots the results.

Usage:
    python benchmark_threads.py
    python benchmark_threads.py --api-key YOUR_KEY
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

import matplotlib.pyplot as plt


def resolve_api_key(cli_key: str | None) -> str | None:
    import os
    key = cli_key or os.environ.get("YOUTUBE_API_KEY")
    if not key:
        env_path = Path(__file__).parent / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("YOUTUBE_API_KEY="):
                    key = line.split("=", 1)[1].strip()
                    break
    return key


def run_benchmark(workers: int, runs: int, hops: int, watch_time: int,
                  api_key: str, out_base: Path) -> float:
    out_dir = out_base / f"workers_{workers}"
    out_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "run_crawls.py",
        "-r", str(runs),
        "-n", str(hops),
        "-x", str(watch_time),
        "-p", str(workers),
        "-o", str(out_dir),
        "--api-key", api_key,
    ]

    print(f"  [{workers:>2} workers] Starting ...", flush=True)
    t0 = time.perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.perf_counter() - t0

    if result.returncode != 0:
        print(f"  [{workers:>2} workers] FAILED\n{result.stderr[-500:]}", flush=True)
        return float("nan")

    print(f"  [{workers:>2} workers] Done in {elapsed:.1f}s "
          f"({runs / elapsed:.2f} runs/s)", flush=True)
    return elapsed


def main():
    parser = argparse.ArgumentParser(description="Benchmark thread counts for run_crawls.py")
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--hops", type=int, default=4)
    parser.add_argument("--watch-time", type=int, default=3)
    parser.add_argument("--min-workers", type=int, default=1)
    parser.add_argument("--max-workers", type=int, default=10)
    parser.add_argument("--output-dir", default="data/benchmark")
    args = parser.parse_args()

    api_key = resolve_api_key(args.api_key)
    if not api_key:
        parser.error("API key required — use --api-key or YOUTUBE_API_KEY env var")

    out_base = Path(args.output_dir)
    worker_counts = list(range(args.min_workers, args.max_workers + 1))

    print(f"\nBenchmarking {args.min_workers}–{args.max_workers} workers "
          f"({args.runs} runs, {args.hops} hops, {args.watch_time}s watch time)\n")

    times = []
    for w in worker_counts:
        elapsed = run_benchmark(w, args.runs, args.hops, args.watch_time,
                                api_key, out_base)
        times.append(elapsed)

    throughputs = [
        args.runs / t if t == t else float("nan")  # nan check
        for t in times
    ]

    # ── Plot ──────────────────────────────────────────────────────────────────
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle(
        f"Thread benchmark  ({args.runs} runs · {args.hops} hops · {args.watch_time}s watch)",
        fontsize=13,
    )

    valid = [(w, t) for w, t in zip(worker_counts, times) if t == t]
    ws = [x[0] for x in valid]
    ts = [x[1] for x in valid]
    tps = [args.runs / t for t in ts]

    ax1.plot(ws, ts, marker="o", color="steelblue", linewidth=2)
    ax1.set_xlabel("Number of workers")
    ax1.set_ylabel("Total time (s)")
    ax1.set_title("Wall-clock time")
    ax1.set_xticks(ws)
    ax1.grid(True, alpha=0.3)
    for x, y in zip(ws, ts):
        ax1.annotate(f"{y:.0f}s", (x, y), textcoords="offset points",
                     xytext=(0, 8), ha="center", fontsize=8)

    ax2.plot(ws, tps, marker="o", color="darkorange", linewidth=2)
    ax2.set_xlabel("Number of workers")
    ax2.set_ylabel("Throughput (runs / second)")
    ax2.set_title("Throughput")
    ax2.set_xticks(ws)
    ax2.grid(True, alpha=0.3)
    for x, y in zip(ws, tps):
        ax2.annotate(f"{y:.3f}", (x, y), textcoords="offset points",
                     xytext=(0, 8), ha="center", fontsize=8)

    plt.tight_layout()
    plot_path = out_base / "benchmark_results.png"
    plot_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(plot_path, dpi=150)
    print(f"\nPlot saved to {plot_path}")

    # ── Summary table ─────────────────────────────────────────────────────────
    print("\n  Workers │  Time (s)  │ Throughput (runs/s)")
    print("  ────────┼────────────┼────────────────────")
    for w, t, tp in zip(ws, ts, tps):
        print(f"  {w:>7} │ {t:>10.1f} │ {tp:>18.3f}")

    best_tp_idx = tps.index(max(tps))
    print(f"\n  Best throughput: {ws[best_tp_idx]} workers ({tps[best_tp_idx]:.3f} runs/s)")


if __name__ == "__main__":
    main()
