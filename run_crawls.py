#!/usr/bin/env python3
"""
Batch runner for the YouTube recommendation crawler.

Runs crawler_click.py multiple times, saving each crawl to a separate JSON file.

Usage:
    python run_crawls.py --runs 10 --iterations 50 --watch-time 30 --output-dir data/batch_30s
    python run_crawls.py -r 5 -n 100 -x 5 -o data/batch_5s
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="Run the YouTube crawler multiple times"
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
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    crawler_path = Path(__file__).parent / "crawler_click.py"

    for i in range(1, args.runs + 1):
        output_file = out_dir / f"crawl_{i}.json"
        print(f"\n{'='*60}")
        print(f"  Run {i}/{args.runs} -> {output_file}")
        print(f"{'='*60}\n")

        cmd = [
            sys.executable, str(crawler_path),
            "-n", str(args.iterations),
            "-x", str(args.watch_time),
            "-o", str(output_file),
        ]
        if args.headed:
            cmd.append("--headed")
        if args.api_key:
            cmd.extend(["--api-key", args.api_key])

        result = subprocess.run(cmd)

        if result.returncode != 0:
            print(f"\n[WARNING] Run {i} exited with code {result.returncode}")

    print(f"\nAll {args.runs} crawls complete. Files saved to {out_dir}/")


if __name__ == "__main__":
    main()
