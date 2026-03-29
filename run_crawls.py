#!/usr/bin/env python3
"""
Batch runner for the YouTube recommendation crawler.

Runs crawler_click.py multiple times in parallel, saving each crawl to a
separate JSON file. The seed pool is built once here and each worker receives
its own pre-assigned seed, avoiding redundant API calls.

Usage:
    python run_crawls.py --runs 100 --iterations 20 --watch-time 30 --output-dir data/batch_30s
    python run_crawls.py -r 100 -n 20 -x 5 -o data/batch_5s --parallel 10
"""

import argparse
import logging
import os
import random
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from crawler_click import CATEGORY_IDS, fetch_trending_videos

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def resolve_api_key(cli_key: str | None) -> str | None:
    """Resolve API key from CLI arg > env var > .env file."""
    key = cli_key or os.environ.get("YOUTUBE_API_KEY")
    if not key:
        env_path = Path(__file__).parent / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("YOUTUBE_API_KEY="):
                    key = line.split("=", 1)[1].strip()
                    break
    return key


def build_seed_pool(api_key: str) -> list[str]:
    """Build the candidate seed pool once across all CATEGORY_IDS."""
    log.info("Building seed pool ...")
    pool: set[str] = set()
    for cat_id in CATEGORY_IDS:
        pool.update(fetch_trending_videos(api_key, cat_id))
    log.info(f"Seed pool ready: {len(pool)} unique videos")
    return list(pool)


def pick_seeds(pool: list[str], n: int) -> list[str]:
    """
    Pick n seeds from the pool. Uses sampling without replacement when
    n <= pool size, otherwise falls back to sampling with replacement.
    """
    if n <= len(pool):
        return random.sample(pool, n)
    log.warning(
        f"Requested {n} seeds but pool only has {len(pool)} videos. "
        "Some seeds will repeat."
    )
    return [random.choice(pool) for _ in range(n)]


def run_single(
    cmd: list[str],
    run_idx: int,
    total: int,
    output_file: Path,
) -> tuple[int, int]:
    """Run one crawler subprocess. Returns (run_idx, returncode)."""
    print(f"[Run {run_idx:>3}/{total}] Starting -> {output_file}", flush=True)
    result = subprocess.run(cmd)
    status = "done" if result.returncode == 0 else f"FAILED (code {result.returncode})"
    print(f"[Run {run_idx:>3}/{total}] {status}", flush=True)
    return run_idx, result.returncode


def main():
    parser = argparse.ArgumentParser(
        description="Run the YouTube crawler multiple times in parallel"
    )
    parser.add_argument(
        "-r", "--runs",
        type=int,
        required=True,
        help="Number of times to run the crawler",
    )
    parser.add_argument(
        "-n", "--iterations",
        type=int,
        default=50,
        help="Number of recommendation hops per crawl (default: 50)",
    )
    parser.add_argument(
        "-x", "--watch-time",
        type=int,
        default=30,
        help="Seconds to watch each video (default: 30)",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=str,
        default="data",
        help="Directory to save crawl files (default: data/)",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run browser in headed mode",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="YouTube Data API key (default: reads from env/file)",
    )
    parser.add_argument(
        "-p", "--parallel",
        type=int,
        default=10,
        help="Number of crawler runs to execute in parallel (default: 10)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Pass --verbose to each crawler worker for full log output",
    )
    args = parser.parse_args()

    api_key = resolve_api_key(args.api_key)
    if not api_key:
        parser.error(
            "YouTube Data API key required. Provide via --api-key, "
            "YOUTUBE_API_KEY env var, or .env file."
        )

    # Build pool once and pre-assign one seed per run
    pool = build_seed_pool(api_key)
    if not pool:
        log.error("Seed pool is empty. Check your API key and network.")
        return
    seeds = pick_seeds(pool, args.runs)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    crawler_path = Path(__file__).parent / "crawler_click.py"

    # Build all jobs upfront — filenames are pre-assigned so no race condition
    jobs = []
    for i in range(1, args.runs + 1):
        output_file = out_dir / f"crawl_{i}.json"
        cmd = [
            sys.executable, str(crawler_path),
            "-n", str(args.iterations),
            "-x", str(args.watch_time),
            "-o", str(output_file),
            "--seed-url", f"https://www.youtube.com/watch?v={seeds[i - 1]}",
            "--run-id", str(i),
        ]
        if args.headed:
            cmd.append("--headed")
        if args.api_key:
            cmd.extend(["--api-key", args.api_key])
        if args.verbose:
            cmd.append("--verbose")
        jobs.append((cmd, output_file))

    print(f"\nStarting {args.runs} crawl(s) with {args.parallel} parallel worker(s) ...\n",
          flush=True)

    failed = []
    with ThreadPoolExecutor(max_workers=args.parallel) as executor:
        futures = {
            executor.submit(run_single, cmd, i, args.runs, output_file): i
            for i, (cmd, output_file) in enumerate(jobs, 1)
        }
        for future in as_completed(futures):
            run_idx, returncode = future.result()
            if returncode != 0:
                failed.append(run_idx)

    print(f"\nAll {args.runs} crawl(s) complete. Files saved to {out_dir}/", flush=True)
    if failed:
        print(f"[WARNING] {len(failed)} run(s) failed: {sorted(failed)}", flush=True)


if __name__ == "__main__":
    main()
