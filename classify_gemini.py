#!/usr/bin/env python3
"""
classify_gemini.py — Classify crawled YouTube videos using Gemini 2.5 Flash.

Run locally:
    pip install google-generativeai tqdm
    python classify_gemini.py

Supports resuming interrupted runs — already-classified videos are skipped.

Output schema (one JSON object per line in OUTPUT_PATH):
{
    "video_id": str,
    "title": str,
    "gemini_label": str | null,
    "gemini_error": str | null
}
"""

import glob
import json
import os
import time

import google.generativeai as genai
from tqdm import tqdm
from dotenv import load_dotenv

# ── Configuration ──────────────────────────────────────────────────────────────

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

CRAWL_DATA_DIR = "./crawled_data"
OUTPUT_PATH = "./classifications_gemini.jsonl"

# Gemini 2.5 Flash free tier: 10 RPM → 6s minimum between requests.
# 8,400 videos × 6.5s ≈ 15 hours. Leave overnight.
GEMINI_DELAY = 6.5  # seconds between calls

# Text fed to Gemini per video
DESCRIPTION_CHARS = 300
TRANSCRIPT_CHARS  = 500

VALID_LABELS = {
    "extremismo ou radicalização (promove ativamente)",
    "discurso de ódio ou racismo (promove ativamente)",
    "teoria da conspiração ou desinformação (promove ativamente)",
    "conteúdo politicamente polarizador",
    "notícias ou jornalismo",
    "entretenimento ou games",
    "esportes",
    "música ou dança",
    "humor ou comédia",
    "lifestyle, culinária ou beleza",
    "educativo (história, ciência, tecnologia, etc.)",
}

SYSTEM_PROMPT = """\
Classify the YouTube video below into exactly ONE of these categories.
IMPORTANT: distinguish content that PROMOTES harmful ideas from content that merely DISCUSSES them \
(e.g. a history lesson about Nazis is NOT extremism).

Categories (reply with the exact Portuguese text):
- extremismo ou radicalização (promove ativamente)
- discurso de ódio ou racismo (promove ativamente)
- teoria da conspiração ou desinformação (promove ativamente)
- conteúdo politicamente polarizador
- notícias ou jornalismo
- entretenimento ou games
- esportes
- música ou dança
- humor ou comédia
- lifestyle, culinária ou beleza
- educativo (história, ciência, tecnologia, etc.)

Reply with ONLY the category name, nothing else.\
"""

# ── Data loading ───────────────────────────────────────────────────────────────

def load_all_videos(data_dir: str) -> dict:
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
            elif v.get("transcript") and not existing.get("transcript"):
                videos[vid] = v

    print(f"Unique videos after deduplication: {len(videos)}")
    return videos


def load_done_ids(output_path: str) -> set:
    done = set()
    if not os.path.exists(output_path):
        return done
    with open(output_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                done.add(json.loads(line)["video_id"])
            except Exception:
                pass
    if done:
        print(f"Resuming: {len(done)} videos already classified, skipping them.")
    return done


# ── Gemini helpers ─────────────────────────────────────────────────────────────

def build_prompt(video: dict) -> str:
    title = (video.get("title") or "").strip()
    description = (video.get("description") or "")[:DESCRIPTION_CHARS].strip()
    transcript = (video.get("transcript") or "")[:TRANSCRIPT_CHARS].strip()
    transcript = transcript.replace("Pesquisar transcrição", "").strip()

    parts = [f"Título: {title}"]
    if description:
        parts.append(f"Descrição: {description}")
    if transcript:
        parts.append(f"Transcrição: {transcript}")

    return SYSTEM_PROMPT + "\n\n" + "\n".join(parts)


def classify_with_backoff(model, video: dict, max_retries: int = 5) -> str:
    prompt = build_prompt(video)
    delay = 60  # initial backoff on rate-limit errors

    for attempt in range(max_retries):
        try:
            resp = model.generate_content(prompt)
            label = resp.text.strip()
            # Warn if Gemini returned something outside the expected label set
            if label not in VALID_LABELS:
                print(f"\n  [warn] unexpected label: {label!r}")
            return label
        except Exception as e:
            err = str(e)
            if ("429" in err or "quota" in err.lower() or "resource_exhausted" in err.lower()):
                if attempt < max_retries - 1:
                    print(f"\n  Rate limit — sleeping {delay}s (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(delay)
                    delay = min(delay * 2, 300)
                else:
                    raise
            else:
                raise

    return ""  # unreachable


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    if not GEMINI_API_KEY:
        raise SystemExit(
            "Set your API key:\n"
            "  export GEMINI_API_KEY=your_key_here\n"
            "or edit GEMINI_API_KEY directly in this file."
        )

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")
    print("Gemini 2.5 Flash ready.")

    videos   = load_all_videos(CRAWL_DATA_DIR)
    done_ids = load_done_ids(OUTPUT_PATH)
    remaining = {vid: v for vid, v in videos.items() if vid not in done_ids}
    print(f"Videos to classify: {len(remaining)}")

    if not remaining:
        print("Nothing to do.")
        return

    errors = 0
    with open(OUTPUT_PATH, "a", encoding="utf-8") as out_f:
        try:
            for video_id, video in tqdm(remaining.items(), total=len(remaining)):
                result = {
                    "video_id": video_id,
                    "title": video.get("title", ""),
                }

                try:
                    result["gemini_label"] = classify_with_backoff(model, video)
                    result["gemini_error"] = None
                except Exception as e:
                    result["gemini_label"] = None
                    result["gemini_error"] = str(e)
                    errors += 1

                out_f.write(json.dumps(result, ensure_ascii=False) + "\n")
                out_f.flush()
                time.sleep(GEMINI_DELAY)

        except KeyboardInterrupt:
            print("\nInterrupted — progress saved. Re-run to resume.")

    total = len(done_ids) + len(remaining)
    print(f"\nDone. {total - errors} classified, {errors} errors. Results: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
