#!/usr/bin/env python3
"""
Build and visualize the YouTube recommendation graph from crawl data.

Usage:
    python build_graph.py data/crawl.json
    python build_graph.py data/crawl_5s.json data/crawl_30s.json data/crawl_60s.json

Outputs:
    - graph.html          — interactive pyvis visualization
    - graph_stats.json    — graph metrics (nodes, edges, degree distribution, etc.)
    - graph.gexf          — GEXF export for Gephi (optional further analysis)
"""

import argparse
import json
import sys
from pathlib import Path
from collections import Counter

import networkx as nx
from pyvis.network import Network
import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt


def load_crawl_data(paths: list[str]) -> tuple[list[dict], list[dict]]:
    """Load and merge crawl data from one or more JSON files."""
    all_videos = []
    all_edges = []

    for path in paths:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        all_videos.extend(data.get("videos", []))
        all_edges.extend(data.get("edges", []))

    return all_videos, all_edges


def build_graph(videos: list[dict], edges: list[dict]) -> nx.DiGraph:
    """Build a NetworkX directed graph from crawl data."""
    G = nx.DiGraph()

    # Add nodes with attributes
    video_map = {}
    for v in videos:
        vid = v["video_id"]
        video_map[vid] = v
        G.add_node(
            vid,
            title=v.get("title", "Unknown"),
            url=v.get("url", ""),
            has_transcript=v.get("transcript") is not None,
            iteration=v.get("iteration", -1),
            watch_time=v.get("watch_time", -1),
        )

    # Count edge occurrences for weights
    edge_counts: Counter = Counter()
    for e in edges:
        edge_counts[(e["source"], e["target"])] += 1

    # Add edges with weights
    for (src, tgt), weight in edge_counts.items():
        G.add_edge(src, tgt, weight=weight)

    return G


def compute_stats(G: nx.DiGraph) -> dict:
    """Compute basic graph statistics."""
    stats = {
        "num_nodes": G.number_of_nodes(),
        "num_edges": G.number_of_edges(),
        "density": nx.density(G),
        "is_weakly_connected": nx.is_weakly_connected(G),
        "num_weakly_connected_components": nx.number_weakly_connected_components(G),
        "num_strongly_connected_components": nx.number_strongly_connected_components(G),
    }

    if G.number_of_nodes() > 0:
        in_degrees = [d for _, d in G.in_degree()]
        out_degrees = [d for _, d in G.out_degree()]
        stats["avg_in_degree"] = sum(in_degrees) / len(in_degrees)
        stats["avg_out_degree"] = sum(out_degrees) / len(out_degrees)
        stats["max_in_degree"] = max(in_degrees)
        stats["max_out_degree"] = max(out_degrees)

        # Nodes with highest in-degree (most recommended to)
        top_in = sorted(G.in_degree(), key=lambda x: x[1], reverse=True)[:10]
        stats["top_in_degree"] = [
            {"video_id": vid, "in_degree": deg, "title": G.nodes[vid].get("title", "")}
            for vid, deg in top_in
        ]

        # Nodes with highest out-degree (recommend many others)
        top_out = sorted(G.out_degree(), key=lambda x: x[1], reverse=True)[:10]
        stats["top_out_degree"] = [
            {"video_id": vid, "out_degree": deg, "title": G.nodes[vid].get("title", "")}
            for vid, deg in top_out
        ]

        # Strongly connected components (potential "sink" clusters)
        sccs = list(nx.strongly_connected_components(G))
        scc_sizes = sorted([len(c) for c in sccs], reverse=True)
        stats["scc_sizes"] = scc_sizes[:20]  # Top 20

    return stats


def visualize_pyvis(G: nx.DiGraph, output_path: str = "graph.html"):
    """Create an interactive pyvis visualization."""
    net = Network(
        height="900px",
        width="100%",
        directed=True,
        notebook=False,
        bgcolor="#1a1a2e",
        font_color="white",
    )

    # Configure physics for better layout
    net.set_options("""
    {
        "physics": {
            "forceAtlas2Based": {
                "gravitationalConstant": -50,
                "centralGravity": 0.01,
                "springLength": 100,
                "springConstant": 0.08
            },
            "solver": "forceAtlas2Based",
            "stabilization": {
                "iterations": 200
            }
        },
        "edges": {
            "arrows": {"to": {"enabled": true, "scaleFactor": 0.5}},
            "color": {"color": "#4a90d9", "opacity": 0.6},
            "smooth": {"type": "curvedCW", "roundness": 0.2}
        },
        "nodes": {
            "font": {"size": 12}
        },
        "interaction": {
            "hover": true,
            "tooltipDelay": 100
        }
    }
    """)

    # Add nodes
    for node_id in G.nodes():
        attrs = G.nodes[node_id]
        title_text = attrs.get("title", node_id)
        iteration = attrs.get("iteration", -1)
        url = attrs.get("url", "")
        has_transcript = attrs.get("has_transcript", False)

        # Tooltip with info
        tooltip = (
            f"<b>{title_text}</b><br>"
            f"ID: {node_id}<br>"
            f"Iteration: {iteration}<br>"
            f"Transcript: {'Yes' if has_transcript else 'No'}<br>"
            f"In-degree: {G.in_degree(node_id)}<br>"
            f"Out-degree: {G.out_degree(node_id)}<br>"
            f"<a href='{url}' target='_blank'>Watch</a>"
        )

        # Size based on in-degree
        size = 10 + G.in_degree(node_id) * 5

        # Color based on iteration (earlier = blue, later = red)
        if iteration > 0:
            ratio = iteration / max(1, max(G.nodes[n].get("iteration", 1) for n in G.nodes()))
            r = int(255 * ratio)
            b = int(255 * (1 - ratio))
            color = f"rgb({r}, 80, {b})"
        else:
            color = "#888888"

        label = title_text[:30] + "..." if len(title_text) > 30 else title_text
        net.add_node(node_id, label=label, title=tooltip, size=size, color=color)

    # Add edges with width proportional to weight
    for src, tgt, data in G.edges(data=True):
        weight = data.get("weight", 1)
        net.add_edge(
            src, tgt,
            value=weight,
            title=f"weight: {weight}",
            width=1 + (weight - 1) * 2,
        )

    net.save_graph(output_path)
    print(f"Interactive graph saved to {output_path}")


def plot_degree_distribution(G: nx.DiGraph, output_path: str = "degree_distribution.png"):
    """Plot in-degree and out-degree distributions."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    in_degrees = [d for _, d in G.in_degree()]
    out_degrees = [d for _, d in G.out_degree()]

    axes[0].hist(in_degrees, bins=range(0, max(in_degrees) + 2), color="#4a90d9",
                 edgecolor="white", alpha=0.8)
    axes[0].set_title("In-Degree Distribution")
    axes[0].set_xlabel("In-Degree")
    axes[0].set_ylabel("Count")

    axes[1].hist(out_degrees, bins=range(0, max(out_degrees) + 2), color="#d94a4a",
                 edgecolor="white", alpha=0.8)
    axes[1].set_title("Out-Degree Distribution")
    axes[1].set_xlabel("Out-Degree")
    axes[1].set_ylabel("Count")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"Degree distribution plot saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Build and visualize YouTube recommendation graph"
    )
    parser.add_argument(
        "crawl_files",
        nargs="+",
        help="One or more crawl JSON files to merge into a single graph",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=str,
        default="output",
        help="Directory for output files (default: output/)",
    )
    args = parser.parse_args()

    # Load data
    print(f"Loading data from {len(args.crawl_files)} file(s) ...")
    videos, edges = load_crawl_data(args.crawl_files)
    print(f"  {len(videos)} videos, {len(edges)} edges loaded.")

    # Build graph
    G = build_graph(videos, edges)
    print(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    # Output directory
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Compute and save stats
    stats = compute_stats(G)
    stats_path = out_dir / "graph_stats.json"
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"\nGraph statistics:")
    print(f"  Nodes: {stats['num_nodes']}")
    print(f"  Edges: {stats['num_edges']}")
    print(f"  Density: {stats['density']:.4f}")
    print(f"  Weakly connected: {stats['is_weakly_connected']}")
    print(f"  Weakly connected components: {stats['num_weakly_connected_components']}")
    print(f"  Strongly connected components: {stats['num_strongly_connected_components']}")
    if "avg_in_degree" in stats:
        print(f"  Avg in-degree: {stats['avg_in_degree']:.2f}")
        print(f"  Avg out-degree: {stats['avg_out_degree']:.2f}")
        print(f"  Max in-degree: {stats['max_in_degree']}")
        print(f"  Max out-degree: {stats['max_out_degree']}")
    print(f"  Stats saved to {stats_path}")

    # Export GEXF (for Gephi)
    gexf_path = out_dir / "graph.gexf"
    nx.write_gexf(G, str(gexf_path))
    print(f"  GEXF exported to {gexf_path}")

    # Interactive visualization
    html_path = str(out_dir / "graph.html")
    visualize_pyvis(G, html_path)

    # Degree distribution plot
    if G.number_of_nodes() > 1:
        plot_path = str(out_dir / "degree_distribution.png")
        plot_degree_distribution(G, plot_path)


if __name__ == "__main__":
    main()
