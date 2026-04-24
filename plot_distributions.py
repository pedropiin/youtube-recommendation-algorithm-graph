#!/usr/bin/env python3
"""
Generate distribution plots for YouTube recommendation graphs:
  - In-degree and out-degree distributions (linear histogram + log-log scatter)
  - Strongly connected component size distribution
    (k = number of vertices, y = number of SCCs with exactly k vertices)

By default, runs over the five "final" graphs:
  output_graph/full/{31_03_26, 01_04_26, 02_04_26, 03_04_26, full}/graph.gexf
and writes plots to:
  plots/{31_03_26, 01_04_26, 02_04_26, 03_04_26, full}/

Usage:
    python plot_distributions.py
    python plot_distributions.py --input output_graph/full/full/graph.gexf --label full
"""

import argparse
from collections import Counter
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx


DEFAULT_TARGETS = [
    ("output_graph/full/31_03_26/graph.gexf", "31_03_26"),
    ("output_graph/full/01_04_26/graph.gexf", "01_04_26"),
    ("output_graph/full/02_04_26/graph.gexf", "02_04_26"),
    ("output_graph/full/03_04_26/graph.gexf", "03_04_26"),
    ("output_graph/full/full/graph.gexf", "full"),
]


def plot_degree_distribution(G: nx.DiGraph, output_path: Path, label: str) -> None:
    """Plot in and out degree distributions as linear histograms."""
    in_degrees = [d for _, d in G.in_degree()]
    out_degrees = [d for _, d in G.out_degree()]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle(f"Distribuição dos graus — {label}  "
                 f"(N={G.number_of_nodes()}, E={G.number_of_edges()})",
                 fontsize=14)

    for ax, degrees, title, color in [
        (axes[0], in_degrees, "Grau de entrada", "#4a90d9"),
        (axes[1], out_degrees, "Grau de saída", "#d94a4a"),
    ]:
        max_d = max(degrees) if degrees else 0
        ax.hist(degrees, bins=range(0, max_d + 2), color=color,
                edgecolor="white", alpha=0.85)
        ax.set_title(title)
        ax.set_xlabel("Grau k")
        ax.set_ylabel("Número de vértices")
        ax.grid(True, alpha=0.3)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(output_path, dpi=150)
    plt.close(fig)
    print(f"  degree distribution -> {output_path}")


def plot_scc_distribution(G: nx.DiGraph, output_path: Path, label: str) -> None:
    """Plot SCC size distribution: k (component size) vs number of SCCs of size k."""
    scc_sizes = [len(c) for c in nx.strongly_connected_components(G)]
    counter = Counter(scc_sizes)
    ks = sorted(counter.keys())
    counts = [counter[k] for k in ks]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle(f"Distribuição dos tamanhos das componentes fortemente conexas — {label}  "
                 f"(#CFCs={len(scc_sizes)})",
                 fontsize=14)

    # Left: vlines + scatter on linear x (bars become invisible when a giant SCC
    # stretches the x-range, since default bar width is in data units)
    axes[0].vlines(ks, 1, counts, color="#6aa84f", linewidth=1.5, alpha=0.9)
    axes[0].scatter(ks, counts, color="#6aa84f", s=25, zorder=3, edgecolor="white")
    axes[0].set_title("Escala linear")
    axes[0].set_xlabel("Tamanho da componente k")
    axes[0].set_ylabel("Número de CFCs com k vértices")
    axes[0].set_yscale("log")  # singletons dominate, log y keeps everything visible
    axes[0].set_ylim(bottom=0.8)
    axes[0].grid(True, which="both", alpha=0.3)

    # Right: log-log scatter
    axes[1].scatter(ks, counts, color="#6aa84f", s=40, alpha=0.9, edgecolor="white")
    axes[1].set_xscale("log")
    axes[1].set_yscale("log")
    axes[1].set_title("Escala log-log")
    axes[1].set_xlabel("Tamanho da componente k")
    axes[1].set_ylabel("Número de CFCs com k vértices")
    axes[1].grid(True, which="both", alpha=0.3)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(output_path, dpi=150)
    plt.close(fig)
    print(f"  SCC distribution    -> {output_path}")


def process(gexf_path: Path, label: str, plots_root: Path) -> None:
    print(f"\n[{label}] loading {gexf_path}")
    G = nx.read_gexf(str(gexf_path))
    if not G.is_directed():
        G = G.to_directed()
    print(f"  nodes={G.number_of_nodes()}, edges={G.number_of_edges()}")

    out_dir = plots_root / label
    out_dir.mkdir(parents=True, exist_ok=True)

    plot_degree_distribution(G, out_dir / "degree_distribution.png", label)
    plot_scc_distribution(G, out_dir / "scc_distribution.png", label)


def main():
    parser = argparse.ArgumentParser(
        description="Generate degree and SCC distribution plots for graph(s)"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Single .gexf file (requires --label). If omitted, runs over the "
             "five default final graphs.",
    )
    parser.add_argument(
        "--label", type=str, default=None,
        help="Subfolder name under the output dir (required with --input)",
    )
    parser.add_argument(
        "-o", "--output-dir", type=str, default="plots",
        help="Root output directory (default: plots)",
    )
    args = parser.parse_args()

    plots_root = Path(args.output_dir)
    plots_root.mkdir(parents=True, exist_ok=True)

    if args.input:
        if not args.label:
            parser.error("--label is required when --input is given")
        process(Path(args.input), args.label, plots_root)
        return

    for rel_path, label in DEFAULT_TARGETS:
        p = Path(rel_path)
        if not p.exists():
            print(f"[SKIP] missing {p}")
            continue
        process(p, label, plots_root)


if __name__ == "__main__":
    main()
