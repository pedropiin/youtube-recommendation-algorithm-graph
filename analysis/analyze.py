#!/usr/bin/env python3
"""
analyze.py — Steps 1, 2, 5, 6 of the MC859 YouTube recommendation graph project.

Step 1: Annotate graph with OpenAI labels → export annotated GEXF
Step 2: Shortest-path analysis from crawl seeds to extremist nodes
Step 5: Label transition matrix heatmap  P(label_v | label_u)
Step 6: Hub analysis — top videos by in-degree, coloured by label

Outputs are written to analysis/output/.

Usage:
    python analyze.py
    python analyze.py --conditions 05_seconds 30_seconds 60_seconds
    python analyze.py --step 1   # run only one step
"""

import argparse
import glob
import json
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import networkx as nx
import numpy as np

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).parent.parent
CRAWL_DIR = _ROOT / "crawled_data"
CLASSIFICATIONS_PATH = _ROOT / "classifications_openai.jsonl"
OUT_DIR = Path(__file__).parent / "output"

# ── Label definitions ──────────────────────────────────────────────────────────
EXTREMIST_LABELS = {
    "teoria da conspiração ou desinformação (promove ativamente)",
    "conteúdo politicamente polarizador",
    "extremismo ou radicalização (promove ativamente)",
    "discurso de ódio ou racismo (promove ativamente)",
}

# Short display names for plotting
LABEL_SHORT = {
    "entretenimento ou games":                                   "entretenimento",
    "notícias ou jornalismo":                                    "notícias",
    "música ou dança":                                           "música",
    "lifestyle, culinária ou beleza":                            "lifestyle",
    "esportes":                                                  "esportes",
    "educativo (história, ciência, tecnologia, etc.)":           "educativo",
    "humor ou comédia":                                          "humor",
    "teoria da conspiração ou desinformação (promove ativamente)": "conspiração",
    "conteúdo politicamente polarizador":                        "polarizador",
    "extremismo ou radicalização (promove ativamente)":          "extremismo",
    "discurso de ódio ou racismo (promove ativamente)":          "discurso de ódio",
    "automóveis ou games":                                       "automóveis",
}

# Colour per label for consistent charts
LABEL_COLOUR = {
    "entretenimento":    "#4e79a7",
    "notícias":          "#f28e2b",
    "música":            "#e15759",
    "lifestyle":         "#76b7b2",
    "esportes":          "#59a14f",
    "educativo":         "#edc948",
    "humor":             "#b07aa1",
    "conspiração":       "#ff0000",
    "polarizador":       "#d62728",
    "extremismo":        "#8c0000",
    "discurso de ódio":  "#a30000",
    "automóveis":        "#9c755f",
    "desconhecido":      "#aaaaaa",
}


# ── Data loading ───────────────────────────────────────────────────────────────

def load_classifications() -> dict[str, str]:
    labels: dict[str, str] = {}
    with open(CLASSIFICATIONS_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            if d.get("openai_label"):
                labels[d["video_id"]] = d["openai_label"]
    return labels


def load_graph(conditions: list[str]) -> nx.DiGraph:
    """Build a DiGraph from crawl JSON files for the given watch-time conditions."""
    G = nx.DiGraph()
    edge_counts: Counter = Counter()
    seed_ids: set[str] = set()

    for cond in conditions:
        pattern = str(CRAWL_DIR / cond / "**" / "*.json")
        for path in glob.glob(pattern, recursive=True):
            with open(path, encoding="utf-8") as f:
                data = json.load(f)

            meta = data.get("metadata", {})
            seed_id = meta.get("seed_video_id")
            if seed_id:
                seed_ids.add(seed_id)

            for v in data.get("videos", []):
                vid = v.get("video_id")
                if not vid:
                    continue
                if vid not in G:
                    G.add_node(
                        vid,
                        title=v.get("title", ""),
                        iteration=v.get("iteration", -1),
                        watch_time=v.get("watch_time", -1),
                        label="desconhecido",
                        is_seed=False,
                    )
                # Keep lowest iteration seen (seed = iteration 1)
                cur_iter = G.nodes[vid].get("iteration", -1)
                new_iter = v.get("iteration", -1)
                if new_iter > 0 and (cur_iter < 0 or new_iter < cur_iter):
                    G.nodes[vid]["iteration"] = new_iter

            for e in data.get("edges", []):
                src, tgt = e.get("source"), e.get("target")
                if src and tgt:
                    edge_counts[(src, tgt)] += 1

    # Mark seeds
    for sid in seed_ids:
        if sid in G:
            G.nodes[sid]["is_seed"] = True

    # Add edges
    for (src, tgt), w in edge_counts.items():
        G.add_edge(src, tgt, weight=w)

    return G


def annotate_graph(G: nx.DiGraph, labels: dict[str, str]) -> None:
    """Add label attribute to every node in-place."""
    for vid in G.nodes():
        lbl = labels.get(vid, "desconhecido")
        G.nodes[vid]["label"] = lbl


# ── Step 1: export annotated graph ────────────────────────────────────────────

def step1_export(G: nx.DiGraph, out_dir: Path) -> None:
    print("\n=== Step 1: Annotated graph export ===")

    total = G.number_of_nodes()
    labelled = sum(1 for _, d in G.nodes(data=True) if d["label"] != "desconhecido")
    unlabelled = total - labelled

    print(f"  Nodes: {total}")
    print(f"  Labelled: {labelled} ({labelled/total*100:.1f}%)")
    print(f"  Unlabelled (not in classification file): {unlabelled}")

    label_counts = Counter(d["label"] for _, d in G.nodes(data=True))
    print("\n  Label distribution:")
    for lbl, cnt in sorted(label_counts.items(), key=lambda x: -x[1]):
        short = LABEL_SHORT.get(lbl, lbl)
        print(f"    {cnt:5d}  {short}")

    # Export GEXF (node attrs work fine with networkx ≥ 2.x)
    gexf_path = out_dir / "annotated_graph.gexf"
    nx.write_gexf(G, str(gexf_path))
    print(f"\n  GEXF saved → {gexf_path}")

    # Save coverage JSON
    coverage = {
        "total_nodes": total,
        "labelled_nodes": labelled,
        "unlabelled_nodes": unlabelled,
        "coverage_pct": round(labelled / total * 100, 2),
        "label_distribution": dict(label_counts),
    }
    cov_path = out_dir / "step1_coverage.json"
    with open(cov_path, "w", encoding="utf-8") as f:
        json.dump(coverage, f, ensure_ascii=False, indent=2)
    print(f"  Coverage stats → {cov_path}")


# ── Step 2: path analysis ─────────────────────────────────────────────────────

def step2_path_analysis(G: nx.DiGraph, out_dir: Path) -> None:
    print("\n=== Step 2: Shortest-path from seeds to extremist nodes ===")

    seeds = [n for n, d in G.nodes(data=True) if d.get("is_seed")]
    extremists = {n for n, d in G.nodes(data=True) if d["label"] in EXTREMIST_LABELS}

    print(f"  Seeds: {len(seeds)}")
    print(f"  Extremist nodes: {len(extremists)}")

    if not seeds or not extremists:
        print("  [skip] No seeds or extremist nodes found.")
        return

    # For each seed run BFS and record shortest distance to any extremist node.
    # NetworkX single-source shortest path is O(V+E); we run it for every seed.
    # We stop as soon as we hit an extremist node (cutoff avoids exploring the
    # whole graph when the nearest extremist is close).
    distances: list[int] = []
    unreachable = 0

    for seed in seeds:
        best = None
        try:
            # Use BFS length; cutoff=None explores everything — fine for our graph size
            lengths = nx.single_source_shortest_path_length(G, seed)
            for ext in extremists:
                if ext in lengths:
                    d = lengths[ext]
                    if best is None or d < best:
                        best = d
        except nx.NetworkXError:
            pass

        if best is None:
            unreachable += 1
        else:
            distances.append(best)

    reachable = len(distances)
    total = len(seeds)
    print(f"  Seeds that can reach an extremist node: {reachable}/{total} "
          f"({reachable/total*100:.1f}%)")
    if distances:
        print(f"  Min distance: {min(distances)}")
        print(f"  Max distance: {max(distances)}")
        print(f"  Mean distance: {np.mean(distances):.2f}")
        print(f"  Median distance: {np.median(distances):.1f}")

    # Plot distribution — bins centred on integers so bar for k sits above k
    fig, ax = plt.subplots(figsize=(9, 5))
    max_d = max(distances) if distances else 1
    bins = [x - 0.5 for x in range(0, max_d + 2)]
    ax.hist(distances, bins=bins, color="#d62728", edgecolor="white", alpha=0.85)
    ax.set_xlabel("Comprimento do menor caminho (saltos)", fontsize=12)
    ax.set_ylabel("Número de seeds", fontsize=12)
    ax.set_title(
        f"Menor caminho dos seeds até o vídeo nocivo mais próximo\n"
        f"({reachable}/{total} seeds alcançam conteúdo nocivo, {unreachable} inalcançáveis)",
        fontsize=12,
    )
    ax.set_xticks(range(0, max_d + 1))
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plot_path = out_dir / "step2_path_distribution.png"
    plt.savefig(plot_path, dpi=150)
    plt.close()
    print(f"  Plot → {plot_path}")

    # Save stats JSON
    dist_counter = Counter(distances)
    stats = {
        "total_seeds": total,
        "seeds_reachable": reachable,
        "seeds_unreachable": unreachable,
        "reachable_pct": round(reachable / total * 100, 2) if total else 0,
        "min_distance": int(min(distances)) if distances else None,
        "max_distance": int(max(distances)) if distances else None,
        "mean_distance": round(float(np.mean(distances)), 3) if distances else None,
        "median_distance": float(np.median(distances)) if distances else None,
        "distance_distribution": {str(k): v for k, v in sorted(dist_counter.items())},
    }
    stats_path = out_dir / "step2_path_stats.json"
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"  Stats → {stats_path}")


# ── Step 5: label transition matrix ───────────────────────────────────────────

def step5_transition_matrix(G: nx.DiGraph, out_dir: Path) -> None:
    print("\n=== Step 5: Label transition matrix ===")

    EXCLUDE_LABELS = {"automóveis ou games"}

    # Collect all labels that actually appear in the graph
    all_labels_present = sorted(
        {d["label"] for _, d in G.nodes(data=True)
         if d["label"] != "desconhecido" and d["label"] not in EXCLUDE_LABELS},
        key=lambda x: LABEL_SHORT.get(x, x),
    )
    if not all_labels_present:
        print("  [skip] No labelled nodes.")
        return

    shorts = [LABEL_SHORT.get(l, l) for l in all_labels_present]
    n = len(all_labels_present)
    label_idx = {lbl: i for i, lbl in enumerate(all_labels_present)}

    # Count transitions
    counts = np.zeros((n, n), dtype=float)
    skipped = 0
    for src, tgt in G.edges():
        src_lbl = G.nodes[src]["label"]
        tgt_lbl = G.nodes[tgt]["label"]
        if src_lbl == "desconhecido" or tgt_lbl == "desconhecido":
            skipped += 1
            continue
        if src_lbl in EXCLUDE_LABELS or tgt_lbl in EXCLUDE_LABELS:
            skipped += 1
            continue
        counts[label_idx[src_lbl], label_idx[tgt_lbl]] += 1

    print(f"  Edges with both endpoints labelled: {int(counts.sum())}")
    print(f"  Edges skipped (≥1 unlabelled endpoint): {skipped}")

    # Row-normalise → conditional probability P(label_v | label_u)
    row_sums = counts.sum(axis=1, keepdims=True)
    probs = np.where(row_sums > 0, counts / row_sums, 0.0)

    # Column-normalise → fraction of v's in-edges that come from u
    col_sums = counts.sum(axis=0, keepdims=True)
    in_share = np.where(col_sums > 0, counts / col_sums, 0.0)

    nocivo_short = {LABEL_SHORT.get(l, l) for l in EXTREMIST_LABELS}
    nocivo_idxs  = [i for i, s in enumerate(shorts) if s in nocivo_short]
    NOCIVO_COLOR = "#b22222"

    def _highlight_nocivo(ax, n, nocivo_idxs, shorts):
        """Color tick labels red and draw border rectangles for nocivo rows/cols."""
        from matplotlib.patches import Rectangle
        for lbl in ax.get_xticklabels():
            if lbl.get_text() in nocivo_short:
                lbl.set_color(NOCIVO_COLOR)
                lbl.set_fontweight("bold")
        for lbl in ax.get_yticklabels():
            if lbl.get_text() in nocivo_short:
                lbl.set_color(NOCIVO_COLOR)
                lbl.set_fontweight("bold")
        for idx in nocivo_idxs:
            # highlight full row (origin = nocivo)
            ax.add_patch(Rectangle((-0.5, idx - 0.5), n, 1,
                                   fill=False, edgecolor=NOCIVO_COLOR,
                                   linewidth=1.5, zorder=3))
            # highlight full column (destination = nocivo)
            ax.add_patch(Rectangle((idx - 0.5, -0.5), 1, n,
                                   fill=False, edgecolor=NOCIVO_COLOR,
                                   linewidth=1.5, zorder=3))

    # ── Matrix 1: conditional probability P(label_v | label_u) ───────────────
    fig, ax = plt.subplots(figsize=(11, 9))
    im = ax.imshow(probs, cmap="Blues", vmin=0, vmax=1)
    plt.colorbar(im, ax=ax, label="P(label_v | label_u)")

    ax.set_xticks(range(n))
    ax.set_yticks(range(n))
    ax.set_xticklabels(shorts, rotation=45, ha="right", fontsize=9)
    ax.set_yticklabels(shorts, fontsize=9)
    ax.set_xlabel("Label destino (v)", fontsize=11)
    ax.set_ylabel("Label origem (u)", fontsize=11)
    ax.set_title(
        "Matriz de transição de labels  P(label_v | label_u)\n"
        r"$\bf{Vermelho}$ = categorias nocivas",
        fontsize=13,
    )

    for i in range(n):
        for j in range(n):
            val = probs[i, j]
            if val > 0.01:
                text_colour = "white" if val > 0.55 else "black"
                ax.text(j, i, f"{val:.2f}", ha="center", va="center",
                        fontsize=7, color=text_colour)

    _highlight_nocivo(ax, n, nocivo_idxs, shorts)
    plt.tight_layout()
    plot_path = out_dir / "step5_transition_matrix.png"
    plt.savefig(plot_path, dpi=150)
    plt.close()
    print(f"  Matrix 1 (conditional probs) → {plot_path}")

    # ── Matrix 2: raw counts + % out-edges of u + % in-edges of v ────────────
    # Each cell shows three lines:
    #   count
    #   out%  (count / total out-edges of row label)
    #   in%   (count / total in-edges of col label)
    fig, ax = plt.subplots(figsize=(13, 11))
    # Colour by raw count (log scale for visibility)
    log_counts = np.log1p(counts)
    im2 = ax.imshow(log_counts, cmap="Oranges")
    plt.colorbar(im2, ax=ax, label="log(1 + contagem de arestas)")

    ax.set_xticks(range(n))
    ax.set_yticks(range(n))
    ax.set_xticklabels(shorts, rotation=45, ha="right", fontsize=9)
    ax.set_yticklabels(shorts, fontsize=9)
    ax.set_xlabel("Label destino (v)", fontsize=11)
    ax.set_ylabel("Label origem (u)", fontsize=11)
    ax.set_title(
        "Matriz de fluxo de arestas\n"
        "contagem  |  % das saídas de u (↓)  |  % das entradas de v (→)\n"
        r"$\bf{Vermelho}$ = categorias nocivas",
        fontsize=12,
    )

    for i in range(n):
        for j in range(n):
            c = int(counts[i, j])
            if c == 0:
                continue
            out_pct = probs[i, j] * 100      # % of u's out-edges
            in_pct  = in_share[i, j] * 100   # % of v's in-edges
            bg = log_counts[i, j] / (log_counts.max() + 1e-9)
            text_colour = "white" if bg > 0.55 else "black"
            cell_text = f"{c}\n{out_pct:.1f}%↓\n{in_pct:.1f}%→"
            ax.text(j, i, cell_text, ha="center", va="center",
                    fontsize=6.5, color=text_colour, linespacing=1.4)

    _highlight_nocivo(ax, n, nocivo_idxs, shorts)
    plt.tight_layout()
    plot2_path = out_dir / "step5_edge_flow_matrix.png"
    plt.savefig(plot2_path, dpi=150)
    plt.close()
    print(f"  Matrix 2 (edge flow)         → {plot2_path}")

    # Save raw counts + probs as JSON
    matrix_data = {
        "labels": shorts,
        "full_labels": all_labels_present,
        "raw_counts": counts.tolist(),
        "conditional_probs": probs.tolist(),
    }
    json_path = out_dir / "step5_transition_matrix.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(matrix_data, f, ensure_ascii=False, indent=2)
    print(f"  Matrix data → {json_path}")

    # Print the most notable cross-category transitions to extremist labels
    print("\n  Top transitions INTO extremist labels:")
    extremist_shorts = {LABEL_SHORT.get(l, l) for l in EXTREMIST_LABELS}
    for j, tgt_short in enumerate(shorts):
        if tgt_short not in extremist_shorts:
            continue
        col = [(probs[i, j], shorts[i]) for i in range(n) if i != j]
        col.sort(reverse=True)
        print(f"    → {tgt_short}:")
        for prob, src_short in col[:5]:
            if prob > 0:
                print(f"        from {src_short}: {prob:.3f}")


# ── Step 6: hub analysis ───────────────────────────────────────────────────────

def step6_hub_analysis(G: nx.DiGraph, out_dir: Path, top_n: int = 30) -> None:
    print(f"\n=== Step 6: Hub analysis (top {top_n} by in-degree) ===")

    top = sorted(G.in_degree(), key=lambda x: x[1], reverse=True)[:top_n]

    video_ids = [vid for vid, _ in top]
    in_degrees = [deg for _, deg in top]
    labels = [G.nodes[vid]["label"] for vid in video_ids]
    titles = [G.nodes[vid].get("title", vid) for vid in video_ids]
    # Truncate titles for display
    display_titles = [t[:45] + "…" if len(t) > 45 else t for t in titles]
    colours = [LABEL_COLOUR.get(LABEL_SHORT.get(lbl, lbl), "#aaaaaa") for lbl in labels]

    # Horizontal bar chart
    fig, ax = plt.subplots(figsize=(12, top_n * 0.42 + 2))
    y_pos = range(top_n - 1, -1, -1)  # top video at the top
    bars = ax.barh(list(y_pos), in_degrees[::-1], color=colours[::-1],
                   edgecolor="white", height=0.7)
    ax.set_yticks(list(y_pos))
    ax.set_yticklabels(display_titles[::-1], fontsize=8)
    ax.set_xlabel("Grau de entrada (vezes recomendado a partir de outros vídeos)", fontsize=11)
    ax.set_title(f"Top {top_n} vídeos mais recomendados por grau de entrada", fontsize=13)
    ax.grid(axis="x", alpha=0.3)

    # Legend
    seen_labels: set[str] = set()
    patches = []
    for lbl, col in zip(labels, colours):
        short = LABEL_SHORT.get(lbl, lbl)
        if short not in seen_labels:
            seen_labels.add(short)
            patches.append(mpatches.Patch(color=col, label=short))
    ax.legend(handles=patches, loc="lower right", fontsize=8, title="Label")

    plt.tight_layout()
    plot_path = out_dir / "step6_hubs.png"
    plt.savefig(plot_path, dpi=150)
    plt.close()
    print(f"  Plot → {plot_path}")

    # Save JSON
    hub_data = [
        {
            "rank": i + 1,
            "video_id": vid,
            "title": G.nodes[vid].get("title", ""),
            "in_degree": deg,
            "label": G.nodes[vid]["label"],
            "label_short": LABEL_SHORT.get(G.nodes[vid]["label"], G.nodes[vid]["label"]),
        }
        for i, (vid, deg) in enumerate(top)
    ]
    json_path = out_dir / "step6_hubs.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(hub_data, f, ensure_ascii=False, indent=2)
    print(f"  Hub data → {json_path}")

    # Brief textual summary
    print(f"\n  Top 10 hubs:")
    for entry in hub_data[:10]:
        print(f"    #{entry['rank']:>2}  [{entry['in_degree']:>4}]  "
              f"[{entry['label_short']:<14}]  {entry['title'][:60]}")


# ── Step 4: SCC analysis ──────────────────────────────────────────────────────

def step4_scc_analysis(G: nx.DiGraph, out_dir: Path) -> None:
    print("\n=== Step 4: Análise de CFCs ===")

    sccs = list(nx.strongly_connected_components(G))
    non_trivial = sorted([s for s in sccs if len(s) > 1], key=len, reverse=True)
    print(f"  Total de CFCs: {len(sccs)}")
    print(f"  Não-triviais (tamanho > 1): {len(non_trivial)}")

    # Condensation DAG — each node maps to its CFC members
    C = nx.condensation(G)

    cfc_records = []
    for cnode in C.nodes():
        members = C.nodes[cnode]["members"]
        if len(members) == 1:
            continue
        label_dist = Counter(G.nodes[m]["label"] for m in members)
        majority_label, majority_count = label_dist.most_common(1)[0]
        majority_pct = majority_count / len(members) * 100
        extremist_count = sum(c for l, c in label_dist.items() if l in EXTREMIST_LABELS)
        extremist_pct = extremist_count / len(members) * 100
        has_extremist = extremist_count > 0
        is_sink = C.out_degree(cnode) == 0
        cfc_records.append({
            "cnode": cnode,
            "size": len(members),
            "majority_label": majority_label,
            "majority_label_short": LABEL_SHORT.get(majority_label, majority_label),
            "majority_pct": round(majority_pct, 1),
            "extremist_count": extremist_count,
            "extremist_pct": round(extremist_pct, 1),
            "has_extremist": has_extremist,
            "is_sink": is_sink,
            "label_dist": dict(label_dist),
        })

    cfc_records.sort(key=lambda x: x["size"], reverse=True)

    # Summary
    sink_cfcs = [r for r in cfc_records if r["is_sink"]]
    extremist_sinks = [r for r in sink_cfcs if r["has_extremist"]]
    print(f"  CFCs sumidouro (sem arestas de saída na condensação): {len(sink_cfcs)}")
    print(f"  CFCs sumidouro com ≥1 nó extremista: {len(extremist_sinks)}")

    print(f"\n  Top 15 maiores CFCs não-triviais:")
    for i, r in enumerate(cfc_records[:15]):
        dist_str = ", ".join(
            f"{LABEL_SHORT.get(l,l)}:{c}"
            for l, c in sorted(r["label_dist"].items(), key=lambda x: -x[1])[:3]
        )
        sink_tag = " [SUM]" if r["is_sink"] else ""
        ext_tag  = " [EXT]"  if r["has_extremist"] else ""
        print(f"    CFC #{i+1:>2}  tamanho={r['size']:>4}  "
              f"{r['majority_label_short']:<14} ({r['majority_pct']:.0f}%)"
              f"{sink_tag}{ext_tag}  — {dist_str}")

    # CFCs predominantemente extremistas
    ext_cfcs = sorted(
        [r for r in cfc_records if r["has_extremist"]],
        key=lambda x: -x["extremist_pct"],
    )
    print(f"\n  CFCs com conteúdo extremista (ordenadas por % extremista):")
    for i, r in enumerate(ext_cfcs[:15]):
        dist_str = ", ".join(
            f"{LABEL_SHORT.get(l,l)}:{c}"
            for l, c in sorted(r["label_dist"].items(), key=lambda x: -x[1])[:4]
        )
        sink_tag = " [SUM]" if r["is_sink"] else ""
        print(f"    #{i+1:>2}  tamanho={r['size']:>4}  extremista={r['extremist_pct']:.0f}%"
              f"  maioria={r['majority_label_short']:<14} ({r['majority_pct']:.0f}%)"
              f"{sink_tag}  — {dist_str}")

    # Plot: top-20 CFCs by size, coloured by majority label
    top_plot = cfc_records[:20]
    sizes   = [r["size"] for r in top_plot]
    colours = [LABEL_COLOUR.get(r["majority_label_short"], "#aaaaaa") for r in top_plot]
    edge_colours = ["black" if r["is_sink"] else "none" for r in top_plot]
    y_labels = [
        f"CFC #{i+1}  (n={r['size']}, {r['majority_label_short']} {r['majority_pct']:.0f}%)"
        + (" ◆" if r["is_sink"] else "")
        for i, r in enumerate(top_plot)
    ]

    fig, ax = plt.subplots(figsize=(11, 8))
    y_pos = range(len(top_plot) - 1, -1, -1)
    ax.barh(
        list(y_pos), sizes[::-1],
        color=colours[::-1], edgecolor=edge_colours[::-1], linewidth=1.2, height=0.7,
    )
    ax.set_yticks(list(y_pos))
    ax.set_yticklabels(y_labels[::-1], fontsize=8)
    ax.set_xlabel("Tamanho da CFC (número de nós)", fontsize=11)
    ax.set_title(
        f"Top {len(top_plot)} maiores CFCs — coloridas pelo label majoritário\n"
        f"Percentual = fração do label majoritário dentro da CFC  ·  ◆ = CFC sumidouro",
        fontsize=11,
    )
    ax.grid(axis="x", alpha=0.3)

    seen: set[str] = set()
    patches = []
    for r in top_plot:
        s = r["majority_label_short"]
        if s not in seen:
            seen.add(s)
            patches.append(mpatches.Patch(color=LABEL_COLOUR.get(s, "#aaaaaa"), label=s))
    ax.legend(handles=patches, fontsize=8, title="Label majoritário")

    plt.tight_layout()
    plot_path = out_dir / "step4_cfcs.png"
    plt.savefig(plot_path, dpi=150)
    plt.close()
    print(f"\n  Plot → {plot_path}")

    # Save JSON
    json_path = out_dir / "step4_cfcs.json"
    save_records = [
        {k: v for k, v in r.items() if k != "cnode"}
        for r in cfc_records
    ]
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(save_records, f, ensure_ascii=False, indent=2)
    print(f"  Dados → {json_path}")


# ── Meta-graph (labels as nodes) ──────────────────────────────────────────────

def meta_graph_analysis(G: nx.DiGraph, out_dir: Path) -> None:
    print("\n=== Meta-graph (collapsed by label) ===")

    EXCLUDE_LABELS = {"automóveis ou games"}

    # Count edges and node populations
    label_counts: Counter = Counter(d["label"] for _, d in G.nodes(data=True)
                                    if d["label"] != "desconhecido"
                                    and d["label"] not in EXCLUDE_LABELS)
    all_labels = sorted(label_counts.keys(), key=lambda x: LABEL_SHORT.get(x, x))

    edge_counts: Counter = Counter()
    for src, tgt, data in G.edges(data=True):
        sl = G.nodes[src]["label"]
        tl = G.nodes[tgt]["label"]
        if sl == "desconhecido" or tl == "desconhecido":
            continue
        if sl in EXCLUDE_LABELS or tl in EXCLUDE_LABELS:
            continue
        edge_counts[(sl, tl)] += data.get("weight", 1)

    # Build meta-graph
    MG = nx.DiGraph()
    for lbl in all_labels:
        short = LABEL_SHORT.get(lbl, lbl)
        MG.add_node(short, count=label_counts[lbl], label=lbl)

    for (sl, tl), w in edge_counts.items():
        if sl == tl:
            continue  # skip self-loops for cross-label clarity
        ss, ts = LABEL_SHORT.get(sl, sl), LABEL_SHORT.get(tl, tl)
        if MG.has_edge(ss, ts):
            MG[ss][ts]["weight"] += w
        else:
            MG.add_edge(ss, ts, weight=w)

    # Self-loop percentages (for annotation)
    self_loop_pct = {}
    for lbl in all_labels:
        short = LABEL_SHORT.get(lbl, lbl)
        total_out = sum(w for (sl, _), w in edge_counts.items() if sl == lbl)
        self_w = edge_counts.get((lbl, lbl), 0)
        self_loop_pct[short] = self_w / total_out * 100 if total_out > 0 else 0

    print(f"  Meta-graph: {MG.number_of_nodes()} label nodes, "
          f"{MG.number_of_edges()} cross-label edges")
    print("  Self-loop % (edges staying within same label):")
    for s, pct in sorted(self_loop_pct.items(), key=lambda x: -x[1]):
        print(f"    {s:<16}  {pct:.1f}%")

    # Layout and draw
    pos = nx.spring_layout(MG, seed=42, k=2.5)

    node_sizes  = [MG.nodes[n]["count"] / 10 for n in MG.nodes()]
    node_colours = [LABEL_COLOUR.get(n, "#aaaaaa") for n in MG.nodes()]

    all_weights = [d["weight"] for _, _, d in MG.edges(data=True)]
    max_w = max(all_weights) if all_weights else 1
    edge_widths = [3 + 8 * (d["weight"] / max_w) for _, _, d in MG.edges(data=True)]
    edge_colours_list = []
    for u, v, d in MG.edges(data=True):
        tgt_label = MG.nodes[v]["label"]
        if tgt_label in EXTREMIST_LABELS:
            edge_colours_list.append("#cc0000")
        else:
            edge_colours_list.append("#888888")

    fig, ax = plt.subplots(figsize=(13, 10))
    nx.draw_networkx_nodes(MG, pos, ax=ax,
                           node_size=node_sizes,
                           node_color=node_colours,
                           alpha=0.9)
    nx.draw_networkx_labels(MG, pos, ax=ax, font_size=8, font_weight="bold")
    nx.draw_networkx_edges(MG, pos, ax=ax,
                           width=edge_widths,
                           edge_color=edge_colours_list,
                           alpha=0.6,
                           arrows=True,
                           arrowsize=15,
                           connectionstyle="arc3,rad=0.1")
    # Self-loop annotation below each node
    offset = 0.08
    for node, (x, y) in pos.items():
        pct = self_loop_pct.get(node, 0)
        ax.text(x, y - offset, f"↺{pct:.0f}%", ha="center", va="top",
                fontsize=7, color="#555555")

    ax.set_title(
        "Meta-grafo: nós por label, arestas de recomendação entre labels\n"
        "Tamanho do nó ∝ qtd. de vídeos · Espessura ∝ contagem · Arestas vermelhas → nocivo · ↺ = % auto-referência",
        fontsize=11,
    )
    ax.axis("off")
    plt.tight_layout()
    plot_path = out_dir / "meta_graph.png"
    plt.savefig(plot_path, dpi=150)
    plt.close()
    print(f"\n  Plot → {plot_path}")

    # Save edge table
    edge_table = sorted(
        [{"from": u, "to": v, "weight": d["weight"]}
         for u, v, d in MG.edges(data=True)],
        key=lambda x: -x["weight"],
    )
    json_path = out_dir / "meta_graph_edges.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(edge_table, f, ensure_ascii=False, indent=2)
    print(f"  Edge table → {json_path}")

    print("\n  Top 10 cross-label edges by count:")
    for e in edge_table[:10]:
        print(f"    {e['from']:<16} → {e['to']:<16}  {e['weight']}")


# ── Random-walk absorption probability ────────────────────────────────────────

def random_walk_absorption(G: nx.DiGraph, out_dir: Path) -> None:
    """
    For each node compute the probability of eventually reaching any extremist
    node under a random walk whose transition probabilities are proportional to
    edge weights.

    Method: absorbing Markov chain — extremist nodes are absorbing states.
    Solve (I - Q) p = r  where Q is the sub-stochastic matrix over transient
    (non-extremist) states and r[i] = Σ_{v extremist} P(i→v).
    """
    print("\n=== Weighted random walk: absorption probability into extremist nodes ===")

    from scipy.sparse import lil_matrix
    from scipy.sparse.linalg import spsolve

    nodes = list(G.nodes())
    idx   = {n: i for i, n in enumerate(nodes)}
    n     = len(nodes)

    extremist_set = {nd for nd in nodes if G.nodes[nd]["label"] in EXTREMIST_LABELS}
    transient_list = [nd for nd in nodes if nd not in extremist_set]
    t = len(transient_list)
    t_idx = {nd: i for i, nd in enumerate(transient_list)}

    print(f"  Absorbing states (extremist nodes): {len(extremist_set)}")
    print(f"  Transient states: {t}")

    # Restrict the linear system to transient nodes that can actually reach an
    # extremist node. Nodes with no such path have absorption probability = 0
    # and would create zero rows that make (I-Q) singular.
    can_reach_extremist: set = set()
    for ext in extremist_set:
        can_reach_extremist.update(nx.ancestors(G, ext))
    solvable = [nd for nd in transient_list if nd in can_reach_extremist]
    s = len(solvable)
    s_idx = {nd: i for i, nd in enumerate(solvable)}

    print(f"  Transient nodes reachable to an extremist: {s} / {t} "
          f"({s/t*100:.1f}%)")

    # Build Q (s×s) and r (s,)
    Q = lil_matrix((s, s), dtype=float)
    r = np.zeros(s, dtype=float)

    for nd in solvable:
        out_edges = list(G.out_edges(nd, data="weight"))
        if not out_edges:
            continue
        total_w = sum((w if w else 1) for _, _, w in out_edges)
        i = s_idx[nd]
        for _, nbr, w in out_edges:
            p = (w if w else 1) / total_w
            if nbr in extremist_set:
                r[i] += p
            elif nbr in s_idx:
                Q[i, s_idx[nbr]] += p

    print("  Solving linear system (I - Q) p = r ...")
    from scipy.sparse import eye as speye
    A = speye(s, format="csr") - Q.tocsr()
    p_solvable = spsolve(A, r)

    # Merge back: solvable nodes get computed value, others get 0
    absorption_array = np.zeros(t, dtype=float)
    for i, nd in enumerate(solvable):
        absorption_array[t_idx[nd]] = p_solvable[i]
    absorption = absorption_array

    # Build full absorption array (extremist nodes = 1.0)
    absorption_map = {nd: float(absorption[t_idx[nd]]) for nd in transient_list}
    for nd in extremist_set:
        absorption_map[nd] = 1.0

    # Report for seeds
    seeds = [nd for nd in G.nodes() if G.nodes[nd].get("is_seed")]
    seed_probs = [absorption_map.get(nd, 0.0) for nd in seeds]

    print(f"\n  Absorption probabilities over {len(seeds)} seeds:")
    print(f"    Mean:   {np.mean(seed_probs):.4f}")
    print(f"    Median: {np.median(seed_probs):.4f}")
    print(f"    Min:    {np.min(seed_probs):.4f}")
    print(f"    Max:    {np.max(seed_probs):.4f}")
    print(f"    Seeds with p > 0.01: {sum(1 for p in seed_probs if p > 0.01)} "
          f"({sum(1 for p in seed_probs if p > 0.01)/len(seeds)*100:.1f}%)")
    print(f"    Seeds with p > 0.10: {sum(1 for p in seed_probs if p > 0.10)} "
          f"({sum(1 for p in seed_probs if p > 0.10)/len(seeds)*100:.1f}%)")
    print(f"    Seeds with p > 0.50: {sum(1 for p in seed_probs if p > 0.50)} "
          f"({sum(1 for p in seed_probs if p > 0.50)/len(seeds)*100:.1f}%)")

    # Plot distribution over seeds
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(seed_probs, bins=40, color="#8c0000", edgecolor="white", alpha=0.85)
    ax.set_xlabel("Probabilidade de absorção em conteúdo nocivo", fontsize=12)
    ax.set_ylabel("Número de seeds", fontsize=12)
    ax.set_title(
        "Probabilidade de absorção via random walk: P(atingir conteúdo nocivo | partir de um seed)\n"
        "Probabilidades de transição proporcionais aos pesos das arestas",
        fontsize=11,
    )
    ax.axvline(np.mean(seed_probs), color="black", linestyle="--", linewidth=1.5,
               label=f"média = {np.mean(seed_probs):.3f}")
    ax.legend(fontsize=10)
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plot_path = out_dir / "random_walk_absorption.png"
    plt.savefig(plot_path, dpi=150)
    plt.close()
    print(f"\n  Plot → {plot_path}")

    # Save per-seed table
    seed_table = sorted(
        [{"video_id": nd,
          "title": G.nodes[nd].get("title", ""),
          "label": G.nodes[nd]["label"],
          "absorption_prob": round(absorption_map.get(nd, 0.0), 6)}
         for nd in seeds],
        key=lambda x: -x["absorption_prob"],
    )
    json_path = out_dir / "random_walk_seed_probs.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(seed_table, f, ensure_ascii=False, indent=2)
    print(f"  Per-seed probabilities → {json_path}")

    print("\n  Top 10 seeds by absorption probability:")
    for e in seed_table[:10]:
        print(f"    p={e['absorption_prob']:.4f}  [{LABEL_SHORT.get(e['label'],e['label']):<14}]"
              f"  {e['title'][:65]}")


# ── Random walk: P(nocivo at step t) per interval ────────────────────────────

def rw_click_intervals(G: nx.DiGraph, out_dir: Path,
                       n_steps: int = 30,
                       intervals: list[tuple[int,int]] = [(0,10),(10,20),(20,30)]) -> None:
    """
    For each seed, propagate the probability distribution over the graph using
    the weighted transition matrix (no absorbing states) and record the
    probability of being on a nocivo node at each click.

    Produces one plot per interval showing the distribution across seeds.
    """
    print("\n=== Random walk: P(nocivo no clique t) por intervalo ===")

    from scipy.sparse import lil_matrix

    nodes = list(G.nodes())
    n = len(nodes)
    idx = {nd: i for i, nd in enumerate(nodes)}

    nocivo_cols = np.array([i for nd, i in idx.items()
                            if G.nodes[nd]["label"] in EXTREMIST_LABELS])
    print(f"  Nós nocivos: {len(nocivo_cols)}  |  Total de nós: {n}")

    # ── Build row-stochastic transition matrix P ──────────────────────────────
    P = lil_matrix((n, n), dtype=np.float32)
    for nd in nodes:
        out_edges = list(G.out_edges(nd, data="weight"))
        i = idx[nd]
        if not out_edges:
            P[i, i] = 1.0          # self-loop for sink nodes
        else:
            total_w = sum((w if w else 1) for _, _, w in out_edges)
            for _, nbr, w in out_edges:
                P[i, idx[nbr]] += (w if w else 1) / total_w
    PT = P.tocsr().T.tocsr()       # store transpose for efficient V = V·P

    # ── Seed initial distributions ────────────────────────────────────────────
    seeds = [nd for nd in G.nodes() if G.nodes[nd].get("is_seed")]
    s = len(seeds)
    print(f"  Seeds: {s}")

    V = np.zeros((s, n), dtype=np.float32)
    for i, nd in enumerate(seeds):
        V[i, idx[nd]] = 1.0

    # ── Propagate n_steps clicks, record p_t for each seed ───────────────────
    # p_matrix[t, i] = P(seed i is on nocivo at click t+1)
    p_matrix = np.zeros((n_steps, s), dtype=np.float32)
    for t in range(n_steps):
        V = (PT @ V.T).T           # (n×n) @ (n×s) → (n×s), then transpose → (s×n)
        p_matrix[t] = V[:, nocivo_cols].sum(axis=1)
        if (t + 1) % 10 == 0:
            print(f"    clique {t+1:>2}: média p(nocivo) = {p_matrix[t].mean():.4f}")

    # ── One plot per interval ─────────────────────────────────────────────────
    color = "#8c0000"
    for (start, end) in intervals:
        steps = list(range(start, min(end, n_steps)))
        if not steps:
            continue
        clicks = [t + 1 for t in steps]          # 1-indexed
        data = p_matrix[steps, :]                 # (len_interval, s)

        median = np.median(data, axis=1)
        mean   = np.mean(data, axis=1)
        q25    = np.percentile(data, 25, axis=1)
        q75    = np.percentile(data, 75, axis=1)
        q10    = np.percentile(data, 10, axis=1)
        q90    = np.percentile(data, 90, axis=1)

        fig, ax = plt.subplots(figsize=(9, 5))
        ax.fill_between(clicks, q10, q90, alpha=0.15, color=color, label="P10–P90")
        ax.fill_between(clicks, q25, q75, alpha=0.30, color=color, label="IQR (P25–P75)")
        ax.plot(clicks, median, color=color, linewidth=2,   label="mediana")
        ax.plot(clicks, mean,   color=color, linewidth=1.5, linestyle="--", label="média")

        ax.set_xlabel("Número de cliques a partir do seed", fontsize=11)
        ax.set_ylabel("P(vídeo nocivo no clique t)", fontsize=11)
        ax.set_title(
            f"Probabilidade de assistir conteúdo nocivo no clique t\n"
            f"Intervalo {start+1}–{end} cliques  ·  {s} seeds",
            fontsize=12,
        )
        ax.set_xlim(clicks[0], clicks[-1])
        ax.set_ylim(0, min(1.0, ax.get_ylim()[1] * 1.15))
        tick_step = 5
        ax.set_xticks([c for c in clicks if c % tick_step == 0])
        ax.legend(fontsize=9)
        ax.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        fname = f"rw_interval_{start+1}_{end}.png"
        path = out_dir / fname
        plt.savefig(path, dpi=150)
        plt.close()
        print(f"  Plot intervalo {start+1}–{end} → {path}")

    # ── Save raw p_matrix as JSON ─────────────────────────────────────────────
    seed_ids = [nd for nd in G.nodes() if G.nodes[nd].get("is_seed")]
    result = {
        "seeds": [{"video_id": nd, "title": G.nodes[nd].get("title",""),
                   "label": LABEL_SHORT.get(G.nodes[nd]["label"], G.nodes[nd]["label"])}
                  for nd in seed_ids],
        "p_nocivo_per_step": p_matrix.tolist(),
    }
    json_path = out_dir / "rw_click_intervals.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"  Dados salvos → {json_path}")


# ── Step 7: Watch-time condition comparison ───────────────────────────────────

def _compute_condition_metrics(G: nx.DiGraph) -> dict:
    """Return a dict of key metrics for a single-condition graph."""
    from scipy.sparse import lil_matrix
    from scipy.sparse.linalg import spsolve
    from scipy.sparse import eye as speye

    EXCLUDE = {"automóveis ou games"}

    nodes = list(G.nodes())
    total = len(nodes)
    extremist_set = {n for n in nodes if G.nodes[n]["label"] in EXTREMIST_LABELS}
    seeds = [n for n in nodes if G.nodes[n].get("is_seed")]

    # ── label distribution (excluding automóveis) ────────────────────────────
    label_counts = Counter(
        LABEL_SHORT.get(d["label"], d["label"])
        for _, d in G.nodes(data=True)
        if d["label"] not in EXCLUDE
    )

    # ── extremist % ──────────────────────────────────────────────────────────
    extremist_pct = len(extremist_set) / total * 100 if total else 0

    # ── Step-2-style BFS hop distances ───────────────────────────────────────
    min_hops = []
    reachable_seeds = 0
    for seed in seeds:
        lengths = nx.single_source_shortest_path_length(G, seed)
        ext_dists = [d for n, d in lengths.items() if n in extremist_set and d > 0]
        if ext_dists:
            min_hops.append(min(ext_dists))
            reachable_seeds += 1

    seeds_reachable_pct = reachable_seeds / len(seeds) * 100 if seeds else 0
    mean_min_hops = float(np.mean(min_hops)) if min_hops else float("nan")

    # ── transition probability to extremist (row = source label) ─────────────
    all_labels = sorted(
        {d["label"] for _, d in G.nodes(data=True)
         if d["label"] != "desconhecido" and d["label"] not in EXCLUDE},
        key=lambda x: LABEL_SHORT.get(x, x),
    )
    label_idx = {l: i for i, l in enumerate(all_labels)}
    n_lbl = len(all_labels)
    edge_matrix = np.zeros((n_lbl, n_lbl), dtype=float)
    for src, tgt in G.edges():
        sl, tl = G.nodes[src]["label"], G.nodes[tgt]["label"]
        if sl == "desconhecido" or tl == "desconhecido":
            continue
        if sl in EXCLUDE or tl in EXCLUDE:
            continue
        if sl in label_idx and tl in label_idx:
            edge_matrix[label_idx[sl], label_idx[tl]] += 1

    row_sums = edge_matrix.sum(axis=1, keepdims=True)
    probs = np.where(row_sums > 0, edge_matrix / row_sums, 0.0)

    # probability of transitioning into any extremist label, per source
    extremist_col_idx = [label_idx[l] for l in all_labels if l in EXTREMIST_LABELS]
    p_to_extremist = {
        LABEL_SHORT.get(all_labels[i], all_labels[i]): float(probs[i, extremist_col_idx].sum())
        for i in range(n_lbl)
        if all_labels[i] not in EXTREMIST_LABELS
    }

    # ── random walk absorption for seeds ─────────────────────────────────────
    transient_list = [n for n in nodes if n not in extremist_set]
    t_idx = {n: i for i, n in enumerate(transient_list)}

    can_reach: set = set()
    for ext in extremist_set:
        can_reach.update(nx.ancestors(G, ext))
    solvable = [n for n in transient_list if n in can_reach]
    s = len(solvable)
    s_idx = {n: i for i, n in enumerate(solvable)}

    absorption_map: dict = {n: 1.0 for n in extremist_set}
    if s > 0:
        Q = lil_matrix((s, s), dtype=float)
        r = np.zeros(s, dtype=float)
        for nd in solvable:
            out_edges = list(G.out_edges(nd, data="weight"))
            if not out_edges:
                continue
            total_w = sum((w if w else 1) for _, _, w in out_edges)
            ii = s_idx[nd]
            for _, nbr, w in out_edges:
                p = (w if w else 1) / total_w
                if nbr in extremist_set:
                    r[ii] += p
                elif nbr in s_idx:
                    Q[ii, s_idx[nbr]] += p
        A = speye(s, format="csr") - Q.tocsr()
        p_sol = spsolve(A, r)
        for i, nd in enumerate(solvable):
            absorption_map[nd] = float(p_sol[i])
    for nd in transient_list:
        absorption_map.setdefault(nd, 0.0)

    seed_probs = [absorption_map.get(nd, 0.0) for nd in seeds]
    mean_absorption = float(np.mean(seed_probs)) if seed_probs else float("nan")

    return {
        "n_nodes": total,
        "n_edges": G.number_of_edges(),
        "n_seeds": len(seeds),
        "extremist_pct": round(extremist_pct, 2),
        "seeds_reachable_pct": round(seeds_reachable_pct, 2),
        "mean_min_hops": round(mean_min_hops, 3),
        "mean_absorption": round(mean_absorption, 4),
        "label_counts": dict(label_counts),
        "p_to_extremist": p_to_extremist,
    }


def step7_watch_time_comparison(labels: dict[str, str], out_dir: Path) -> None:
    print("\n=== Step 7: Watch-time condition comparison ===")

    CONDITIONS = ["05_seconds", "30_seconds", "60_seconds"]
    COND_LABELS = {"05_seconds": "5s", "30_seconds": "30s", "60_seconds": "60s"}
    metrics: dict[str, dict] = {}

    for cond in CONDITIONS:
        print(f"\n  [{cond}] loading graph …")
        G = load_graph([cond])
        annotate_graph(G, labels)
        print(f"    {G.number_of_nodes()} nodes, {G.number_of_edges()} edges — computing metrics …")
        metrics[cond] = _compute_condition_metrics(G)

    short_conds = [COND_LABELS[c] for c in CONDITIONS]

    # ── Print summary table ───────────────────────────────────────────────────
    print("\n  Summary table:")
    header = f"  {'Métrica':<30}" + "".join(f"  {COND_LABELS[c]:>8}" for c in CONDITIONS)
    print(header)
    print("  " + "-" * (30 + 12 * len(CONDITIONS)))
    rows = [
        ("Nós",           "n_nodes"),
        ("Arestas",        "n_edges"),
        ("Seeds",          "n_seeds"),
        ("% conteúdo nocivo",   "extremist_pct"),
        ("% seeds → nocivo",    "seeds_reachable_pct"),
        ("Média min-hops",         "mean_min_hops"),
        ("Média absorção (RW)",    "mean_absorption"),
    ]
    for display, key in rows:
        vals = "".join(f"  {metrics[c][key]:>8}" for c in CONDITIONS)
        print(f"  {display:<30}{vals}")

    # ── Save JSON ─────────────────────────────────────────────────────────────
    json_path = out_dir / "step7_comparison.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({COND_LABELS[c]: metrics[c] for c in CONDITIONS},
                  f, ensure_ascii=False, indent=2)
    print(f"\n  Metrics saved → {json_path}")

    # ── Plot 1: stacked bar — label distribution per condition ────────────────
    all_short_labels = sorted(
        {lbl for c in CONDITIONS for lbl in metrics[c]["label_counts"]},
        key=lambda x: -sum(metrics[c]["label_counts"].get(x, 0) for c in CONDITIONS),
    )
    totals = [sum(metrics[c]["label_counts"].values()) for c in CONDITIONS]
    pcts = {
        lbl: [metrics[c]["label_counts"].get(lbl, 0) / totals[i] * 100
              for i, c in enumerate(CONDITIONS)]
        for lbl in all_short_labels
    }

    fig, ax = plt.subplots(figsize=(9, 6))
    bottoms = np.zeros(len(CONDITIONS))
    for lbl in all_short_labels:
        vals = np.array(pcts[lbl])
        color = LABEL_COLOUR.get(lbl, "#aaaaaa")
        bars = ax.bar(short_conds, vals, bottom=bottoms, color=color,
                      label=lbl, edgecolor="white", linewidth=0.4)
        for bar, val, bot in zip(bars, vals, bottoms):
            if val >= 3:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bot + val / 2, f"{val:.0f}%",
                        ha="center", va="center", fontsize=7, color="white", fontweight="bold")
        bottoms += vals

    ax.set_xlabel("Condição experimental (tempo de exibição)", fontsize=11)
    ax.set_ylabel("% dos vídeos no grafo", fontsize=11)
    ax.set_title("Distribuição de labels por condição experimental", fontsize=13)
    ax.legend(loc="upper right", fontsize=8, bbox_to_anchor=(1.22, 1))
    ax.set_ylim(0, 105)
    plt.tight_layout()
    p1 = out_dir / "step7_label_distribution.png"
    plt.savefig(p1, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Plot 1 (distribuição de labels) → {p1}")

    # ── Plot 2: 4-panel scalar metrics comparison ─────────────────────────────
    fig, axes = plt.subplots(1, 4, figsize=(16, 5))
    fig.suptitle("Comparação de métricas por condição experimental", fontsize=14)

    panels = [
        (axes[0], "% conteúdo nocivo",              "extremist_pct",       "#8c0000"),
        (axes[1], "% seeds que alcançam nocivo",     "seeds_reachable_pct", "#d62728"),
        (axes[2], "Média min-hops (seeds alcançáveis)", "mean_min_hops", "#4e79a7"),
        (axes[3], "Média prob. absorção (random walk)", "mean_absorption", "#f28e2b"),
    ]
    for ax, title, key, color in panels:
        vals = [metrics[c][key] for c in CONDITIONS]
        bars = ax.bar(short_conds, vals, color=color, edgecolor="white", alpha=0.85)
        for bar, val in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + max(vals) * 0.02,
                    f"{val:.2f}", ha="center", va="bottom", fontsize=9)
        ax.set_title(title, fontsize=10)
        ax.set_xlabel("Condição")
        ax.set_ylim(0, max(vals) * 1.2 if max(vals) > 0 else 1)
        ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    p2 = out_dir / "step7_scalar_metrics.png"
    plt.savefig(p2, dpi=150)
    plt.close()
    print(f"  Plot 2 (métricas escalares) → {p2}")

    # ── Plot 3: heatmap — P(→extremista | source label) per condition ─────────
    source_labels = sorted(
        {lbl for c in CONDITIONS for lbl in metrics[c]["p_to_extremist"]},
        key=lambda x: -sum(metrics[c]["p_to_extremist"].get(x, 0) for c in CONDITIONS),
    )
    heat = np.array([
        [metrics[c]["p_to_extremist"].get(lbl, 0.0) for c in CONDITIONS]
        for lbl in source_labels
    ])

    fig, ax = plt.subplots(figsize=(7, max(4, len(source_labels) * 0.6 + 1.5)))
    im = ax.imshow(heat, cmap="Reds", vmin=0, aspect="auto")
    plt.colorbar(im, ax=ax, label="P(→nocivo)")
    ax.set_xticks(range(len(CONDITIONS)))
    ax.set_xticklabels(short_conds, fontsize=10)
    ax.set_yticks(range(len(source_labels)))
    ax.set_yticklabels(source_labels, fontsize=9)
    ax.set_xlabel("Condição experimental", fontsize=11)
    ax.set_ylabel("Label de origem", fontsize=11)
    ax.set_title("P(próxima recomendação nociva | label origem)\npor condição experimental",
                 fontsize=12)
    for i, lbl in enumerate(source_labels):
        for j, c in enumerate(CONDITIONS):
            val = heat[i, j]
            color = "white" if val > heat.max() * 0.6 else "black"
            ax.text(j, i, f"{val:.3f}", ha="center", va="center", fontsize=9, color=color)
    plt.tight_layout()
    p3 = out_dir / "step7_transition_to_extremist.png"
    plt.savefig(p3, dpi=150)
    plt.close()
    print(f"  Plot 3 (transição → extremista) → {p3}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="MC859 analysis steps 1/2/4/5/6 + meta-graph + random walk")
    parser.add_argument(
        "--conditions",
        nargs="+",
        default=["05_seconds", "30_seconds", "60_seconds"],
        help="Watch-time conditions to include (subdirs of crawled_data/)",
    )
    parser.add_argument(
        "--step",
        type=str,
        choices=["1", "2", "4", "5", "6", "meta", "rw", "7", "rwi"],
        default=None,
        help="Run only one step: 1/2/4/5/6/meta/rw/7/rwi (default: all)",
    )
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Loading graph from conditions: {args.conditions} ...")
    G = load_graph(args.conditions)
    print(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    print("Loading classifications ...")
    labels = load_classifications()
    annotate_graph(G, labels)
    print(f"  {len(labels)} labels loaded.")

    run_all = args.step is None

    if run_all or args.step == "1":
        step1_export(G, OUT_DIR)
    if run_all or args.step == "2":
        step2_path_analysis(G, OUT_DIR)
    if run_all or args.step == "4":
        step4_scc_analysis(G, OUT_DIR)
    if run_all or args.step == "5":
        step5_transition_matrix(G, OUT_DIR)
    if run_all or args.step == "6":
        step6_hub_analysis(G, OUT_DIR)
    if run_all or args.step == "meta":
        meta_graph_analysis(G, OUT_DIR)
    if run_all or args.step == "rw":
        random_walk_absorption(G, OUT_DIR)
    if run_all or args.step == "7":
        step7_watch_time_comparison(labels, OUT_DIR)
    if run_all or args.step == "rwi":
        rw_click_intervals(G, OUT_DIR)

    print("\nDone. All outputs in:", OUT_DIR)


if __name__ == "__main__":
    main()
