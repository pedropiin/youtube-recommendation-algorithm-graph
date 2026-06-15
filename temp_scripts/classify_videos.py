#!/usr/bin/env python3
"""
classify_videos.py — Video extremism/harmfulness classification for MC859

Designed to run cell-by-cell in Google Colab (each # %% is a cell).
Uses two independent classifiers:
  1. mDeBERTa-v3-base-xnli-multilingual-nli-2mil7 — zero-shot, Portuguese-aware
  2. Google Perspective API — toxicity/hate/threat scores for PT

Output: classifications.jsonl (one JSON object per line, written incrementally).
Supports resuming interrupted runs — already-classified videos are skipped.

Each output line schema:
{
  "video_id": str,
  "title": str,
  "mdeberta_label": str,          # top-scoring label
  "mdeberta_scores": {            # full score distribution over all labels
      label: float, ...
  },
  "mdeberta_error": str | null,
  "perspective": {
      "toxicity": float,
      "severe_toxicity": float,
      "identity_attack": float,
      "threat": float,
      "insult": float
  },
  "perspective_error": str | null
}
"""

# %% ── CELL 1: Install dependencies ──────────────────────────────────────────
# Run once. If Colab prompts to restart the runtime, do so before continuing.
# !pip install -q transformers torch accelerate tqdm requests

# %% ── CELL 2: Mount Google Drive ─────────────────────────────────────────────
# Skip if you uploaded crawled_data/ directly to Colab (/content/).
# from google.colab import drive
# drive.mount('/content/drive')

# %% ── CELL 3: Configuration ──────────────────────────────────────────────────
import os

# ─── Perspective API key ──────────────────────────────────────────────────────
# Get one at: https://developers.perspectiveapi.com/s/docs-get-started
# Leave empty ("") to skip Perspective API and only run mDeBERTa.
PERSPECTIVE_API_KEY = ""

# ─── Paths ────────────────────────────────────────────────────────────────────
# Where your crawled_data/ folder lives in Colab.
#   Google Drive: "/content/drive/MyDrive/mc859/crawled_data"
#   Uploaded zip: "/content/crawled_data"
CRAWL_DATA_DIR = "/content/drive/MyDrive/mc859/crawled_data"

# Where to write results. Use a Drive path so results survive session resets.
OUTPUT_PATH = "/content/drive/MyDrive/mc859/classifications.jsonl"

# ─── Zero-shot classification labels (in Portuguese) ─────────────────────────
# mDeBERTa was fine-tuned with Portuguese NLI data, so Portuguese hypothesis
# templates produce better results than English ones.
LABELS = [
    "conteúdo extremista ou radical",
    "discurso de ódio ou racismo",
    "teoria da conspiração ou desinformação",
    "conteúdo politicamente polarizador",
    "notícias ou jornalismo",
    "entretenimento",
    "esportes",
    "música ou dança",
    "humor ou comédia",
    "culinária, beleza ou lifestyle",
]

# ─── Perspective API attributes (all have production PT support) ──────────────
PERSPECTIVE_ATTRS = [
    "TOXICITY",
    "SEVERE_TOXICITY",
    "IDENTITY_ATTACK",
    "THREAT",
    "INSULT",
]

# ─── Text limits ──────────────────────────────────────────────────────────────
# mDeBERTa supports 512 tokens (~2000 chars). We build input as:
#   title | description[:300] | transcript[:1500]
# Perspective API accepts up to 20,480 bytes — no special limit needed here.
DESCRIPTION_CHARS = 300
TRANSCRIPT_CHARS = 1500
MDEBERTA_MAX_CHARS = 2000

# Perspective API: free tier allows 1 query per second.
PERSPECTIVE_QPS_DELAY = 1.1  # seconds between calls


# %% ── CELL 4: Load and deduplicate all videos ────────────────────────────────
import json
import glob

def load_all_videos(data_dir: str) -> dict:
    """
    Load all crawl JSONs, return a deduplicated dict of video_id -> video.
    When a video_id appears multiple times, keep the entry with the most text
    (prefers entries with transcripts over title-only entries).
    """
    all_files = glob.glob(f"{data_dir}/**/*.json", recursive=True)
    print(f"Found {len(all_files)} crawl files")

    videos = {}
    for path in all_files:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for v in data.get("videos", []):
            vid = v.get("video_id")
            if not vid:
                continue
            existing = videos.get(vid)
            if existing is None:
                videos[vid] = v
            else:
                # Prefer the entry that has a transcript when the other doesn't
                if v.get("transcript") and not existing.get("transcript"):
                    videos[vid] = v

    print(f"Unique videos after deduplication: {len(videos)}")
    return videos


videos = load_all_videos(CRAWL_DATA_DIR)


# %% ── CELL 5: Compute remaining videos (resume support) ─────────────────────

def load_done_ids(output_path: str) -> set:
    done = set()
    if os.path.exists(output_path):
        with open(output_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    done.add(json.loads(line)["video_id"])
                except Exception:
                    pass
        print(f"Resuming: {len(done)} videos already classified, skipping them.")
    return done


done_ids = load_done_ids(OUTPUT_PATH)
remaining = {vid: v for vid, v in videos.items() if vid not in done_ids}
print(f"Videos to classify: {len(remaining)}")


# %% ── CELL 6: Load mDeBERTa classifier ──────────────────────────────────────
import torch
from transformers import pipeline

device = 0 if torch.cuda.is_available() else -1
device_label = f"GPU ({torch.cuda.get_device_name(0)})" if device == 0 else "CPU"
print(f"Running mDeBERTa on: {device_label}")
if device == -1:
    print("  Note: CPU inference is slower (~20-30 min for 8,400 videos).")
    print("  Enable GPU in Runtime → Change runtime type → T4 GPU.")

print("Loading MoritzLaurer/mDeBERTa-v3-base-xnli-multilingual-nli-2mil7 ...")
classifier = pipeline(
    "zero-shot-classification",
    model="MoritzLaurer/mDeBERTa-v3-base-xnli-multilingual-nli-2mil7",
    device=device,
)
print("Model loaded.")


# %% ── CELL 7: Helper functions ───────────────────────────────────────────────
import time
import requests


def prepare_text(video: dict) -> str:
    """
    Combine title + description + transcript into a single string for classification.
    Truncates each component to stay within mDeBERTa's token budget.
    """
    parts = []

    title = (video.get("title") or "").strip()
    if title:
        parts.append(title)

    description = (video.get("description") or "").strip()
    if description:
        parts.append(description[:DESCRIPTION_CHARS])

    transcript = (video.get("transcript") or "").strip()
    if transcript:
        transcript = transcript.replace("Pesquisar transcrição", "").strip()
        parts.append(transcript[:TRANSCRIPT_CHARS])

    text = " | ".join(parts)
    return text[:MDEBERTA_MAX_CHARS]


def call_perspective(text: str, api_key: str) -> dict:
    """
    Call Perspective API for the given text.
    Returns a dict of lowercased attribute names -> float scores.
    Retries once after 60s on rate-limit (HTTP 429).
    Raises on other errors.
    """
    if not text.strip():
        return {}

    url = "https://commentanalyzer.googleapis.com/v1alpha1/comments:analyze"
    body = {
        "comment": {"text": text},
        "languages": ["pt"],
        "requestedAttributes": {attr: {} for attr in PERSPECTIVE_ATTRS},
    }

    for attempt in range(2):
        resp = requests.post(
            url, params={"key": api_key}, json=body, timeout=15
        )
        if resp.status_code == 200:
            data = resp.json()
            return {
                attr.lower(): data["attributeScores"][attr]["summaryScore"]["value"]
                for attr in PERSPECTIVE_ATTRS
                if attr in data.get("attributeScores", {})
            }
        elif resp.status_code == 429 and attempt == 0:
            print("  Perspective API rate limit — waiting 60s before retry...")
            time.sleep(60)
        else:
            resp.raise_for_status()

    return {}


# %% ── CELL 8: Main classification loop ──────────────────────────────────────
# Writes one JSONL line per video immediately after classification.
# Safe to interrupt and resume — already-written lines are detected on restart.

from tqdm.auto import tqdm

use_perspective = bool(PERSPECTIVE_API_KEY)
if not use_perspective:
    print("PERSPECTIVE_API_KEY is empty — skipping Perspective API.")

with open(OUTPUT_PATH, "a", encoding="utf-8") as out_f:
    for video_id, video in tqdm(remaining.items(), total=len(remaining)):
        result = {
            "video_id": video_id,
            "title": video.get("title", ""),
        }

        text = prepare_text(video)

        # ── mDeBERTa zero-shot ────────────────────────────────────────────────
        try:
            pred = classifier(
                text,
                candidate_labels=LABELS,
                hypothesis_template="Este vídeo é {}.",
                multi_label=False,
            )
            result["mdeberta_label"] = pred["labels"][0]
            result["mdeberta_scores"] = dict(zip(pred["labels"], pred["scores"]))
            result["mdeberta_error"] = None
        except Exception as e:
            result["mdeberta_label"] = None
            result["mdeberta_scores"] = {}
            result["mdeberta_error"] = str(e)

        # ── Perspective API ───────────────────────────────────────────────────
        if use_perspective:
            try:
                result["perspective"] = call_perspective(text, PERSPECTIVE_API_KEY)
                result["perspective_error"] = None
            except Exception as e:
                result["perspective"] = {}
                result["perspective_error"] = str(e)
            time.sleep(PERSPECTIVE_QPS_DELAY)
        else:
            result["perspective"] = {}
            result["perspective_error"] = None

        # ── Flush to disk immediately ─────────────────────────────────────────
        out_f.write(json.dumps(result, ensure_ascii=False) + "\n")
        out_f.flush()

print(f"\nDone. Results written to: {OUTPUT_PATH}")


# %% ── CELL 9: Summary statistics ─────────────────────────────────────────────

import pandas as pd

records = []
with open(OUTPUT_PATH, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            try:
                records.append(json.loads(line))
            except Exception:
                pass

df = pd.DataFrame(records)
print(f"Total classified: {len(df)}\n")

if "mdeberta_label" in df.columns:
    print("── mDeBERTa label distribution ──────────────────────────────")
    counts = df["mdeberta_label"].value_counts()
    total = counts.sum()
    for label, count in counts.items():
        bar = "█" * int(count / total * 40)
        print(f"  {label:<45} {count:>5}  {bar}")

if "perspective" in df.columns:
    print("\n── Perspective API score means (videos with scores) ─────────")
    persp_df = pd.json_normalize(df["perspective"].dropna())
    if not persp_df.empty:
        for col in persp_df.columns:
            mean_val = persp_df[col].mean()
            print(f"  {col:<25} mean: {mean_val:.3f}")

    print("\n── Top 20 most toxic videos (by TOXICITY score) ─────────────")
    df_persp = df.copy()
    df_persp["toxicity"] = df_persp["perspective"].apply(
        lambda x: x.get("toxicity") if isinstance(x, dict) else None
    )
    top_toxic = df_persp.dropna(subset=["toxicity"]).nlargest(20, "toxicity")
    for _, row in top_toxic.iterrows():
        print(f"  [{row['toxicity']:.3f}] {row['title'][:80]}")
