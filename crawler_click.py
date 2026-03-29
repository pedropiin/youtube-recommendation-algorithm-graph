#!/usr/bin/env python3
"""
YouTube Recommendation Graph Crawler

Simulates a new user's behavior on YouTube:
1. Finds a seed video (via homepage, trending, or search)
2. Watches the video for `watch_time` seconds
3. Grabs the video's URL, title, and transcription
4. Clicks the first recommended video in the sidebar
5. Records the recommendation edge (A -> B)
6. Repeats steps 2-5 for `n_iterations`

Usage:
    python crawler.py --iterations 50 --watch-time 30 --output data/crawl.json
    python crawler.py -n 100 -x 5 -o data/crawl_5s.json
"""

import argparse
import json
import os
import random
import re
import logging
from datetime import datetime
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright

log = logging.getLogger(__name__)


def _progress(msg: str, run_id: str | None = None) -> None:
    """Print a progress line that always shows regardless of log level."""
    prefix = f"[Run {run_id}] " if run_id else ""
    print(f"{prefix}{msg}", flush=True)

# Generic trending (None) + category-specific trending pools.
# None = no category filter (overall mostPopular for BR)
# 24=Entertainment, 17=Sports, 25=News & Politics, 22=People & Blogs
# Note: category 27 (Education) returns 404 for BR mostPopular — excluded.
CATEGORY_IDS = [None, 24, 17, 25, 22]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_video_id(url: str) -> str | None:
    """Extract the 11-char video ID from a YouTube URL."""
    match = re.search(r"[?&]v=([a-zA-Z0-9_-]{11})", url)
    if match:
        return match.group(1)
    # Handle youtu.be short links
    match = re.search(r"youtu\.be/([a-zA-Z0-9_-]{11})", url)
    return match.group(1) if match else None


def clean_watch_url(url: str) -> str:
    """Strip playlist and other params, keep only v= parameter."""
    vid = extract_video_id(url)
    if vid:
        return f"https://www.youtube.com/watch?v={vid}"
    return url


def fetch_transcript(page, video_id: str) -> str | None:
    """
    Fetch transcript by opening the transcript panel in the YouTube UI.

    Expands the description, clicks "Show transcript" / "Mostrar transcrição",
    then scrapes text from the engagement panel. This uses the browser directly,
    avoiding API-level or IP-level rate limiting.
    Returns the full transcript as a single string, or None on failure.
    """
    try:
        # Expand description to reveal the "Show transcript" button
        desc = page.locator("#description-inline-expander").first
        if not desc.is_visible(timeout=3000):
            log.info(f"  Description expander not visible for {video_id}")
            return None
        desc.click()
        page.wait_for_timeout(1000)

        # Click "Show transcript" / "Mostrar transcrição"
        transcript_btn = page.locator(
            'button:has-text("Mostrar transcrição"), '
            'button:has-text("Show transcript")'
        ).first
        if not transcript_btn.is_visible(timeout=2000):
            log.info(f"  No 'Show transcript' button for {video_id}")
            return None
        transcript_btn.click()
        page.wait_for_timeout(3000)

        # Scrape text from the transcript engagement panel
        panel_content = page.locator(
            "ytd-engagement-panel-section-list-renderer #content"
        ).first
        if not panel_content.is_visible(timeout=2000):
            log.info(f"  Transcript panel did not open for {video_id}")
            return None

        raw_text = panel_content.inner_text(timeout=5000)
        # Lines alternate: timestamp, duration label, transcript text
        # Filter out timestamps (e.g. "0:18") and duration labels (e.g. "18 segundos")
        lines = [line.strip() for line in raw_text.split("\n") if line.strip()]
        texts = []
        for line in lines:
            # Skip timestamp lines like "0:18", "1:23:45"
            if re.match(r"^\d+:\d{2}(:\d{2})?$", line):
                continue
            # Skip duration labels like "18 segundos", "1 minuto e 23 segundos"
            if re.match(r"^\d+\s*(segundos?|minutos?|seconds?|minutes?|hours?|horas?)", line, re.IGNORECASE):
                continue
            texts.append(line)

        # Close the transcript panel
        try:
            close_btn = page.locator(
                "ytd-engagement-panel-title-header-renderer #visibility-button button"
            ).first
            if close_btn.is_visible(timeout=1000):
                close_btn.click()
        except Exception:
            pass

        return " ".join(texts) if texts else None

    except Exception as e:
        log.warning(f"Could not fetch transcript for {video_id}: {e}")
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        return None


def dismiss_popups(page) -> None:
    """Dismiss consent dialogs and recommendation prompts."""
    popup_selectors = [
        # Cookie/consent dialogs
        'button:has-text("Accept all")',
        'button:has-text("Aceitar tudo")',
        'button:has-text("Reject all")',
        'button:has-text("Rejeitar tudo")',
        # "Recommendations not relevant?" popup — close it
        'button[aria-label="Close"]',
        'button[aria-label="Fechar"]',
        '#dismiss-button button',
        'tp-yt-paper-dialog button.yt-spec-button-shape-next--text',
    ]
    for selector in popup_selectors:
        try:
            btn = page.locator(selector).first
            if btn.is_visible(timeout=1000):
                btn.click()
                log.info(f"Dismissed popup via: {selector}")
                page.wait_for_timeout(1000)
        except Exception:
            pass


def wait_and_scroll(page, seconds: int) -> None:
    """Simulate watching a video for `seconds` by waiting and scrolling."""
    log.info(f"  Watching for {seconds}s ...")
    half = max(1, seconds // 2)
    page.wait_for_timeout(half * 1000)
    # Scroll down to load recommendations
    page.evaluate("window.scrollBy(0, 400)")
    page.wait_for_timeout((seconds - half) * 1000)
    page.evaluate("window.scrollTo(0, 0)")


def get_video_title(page) -> str:
    """Extract the video title from the watch page."""
    selectors = [
        "yt-formatted-string.style-scope.ytd-watch-metadata",
        "h1.ytd-watch-metadata yt-formatted-string",
        "#title h1 yt-formatted-string",
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            title = el.inner_text(timeout=5000)
            if title and title.strip():
                return title.strip()
        except Exception:
            continue

    # Fallback: page title
    try:
        raw = page.title()
        return raw.replace(" - YouTube", "").strip()
    except Exception:
        return "Unknown"


def fetch_trending_videos(api_key: str, category_id: int | None = None) -> list[str]:
    """
    Fetch up to 50 trending video IDs in Brazil for a given category.
    If category_id is None, fetches from the generic (no category) trending list.
    Returns a list of video IDs.
    """
    params = {
        "part": "id",
        "chart": "mostPopular",
        "regionCode": "BR",
        "maxResults": 50,
        "key": api_key,
    }
    if category_id is not None:
        params["videoCategoryId"] = category_id

    label = f"category {category_id}" if category_id is not None else "generic trending"
    try:
        resp = requests.get(
            "https://www.googleapis.com/youtube/v3/videos",
            params=params,
            timeout=10,
        )
        resp.raise_for_status()
        ids = [item["id"] for item in resp.json().get("items", [])]
        log.info(f"  Fetched {len(ids)} videos from {label}")
        return ids
    except Exception as e:
        log.warning(f"  Failed to fetch trending videos ({label}): {e}")
        return []


def pick_seed(api_key: str) -> str | None:
    """
    Build a candidate pool from all CATEGORY_IDS (including generic trending)
    and pick one at random.
    Returns a video URL, or None if the pool is empty.
    """
    log.info("Building seed pool from trending lists ...")
    pool: set[str] = set()
    for cat_id in CATEGORY_IDS:
        pool.update(fetch_trending_videos(api_key, cat_id))

    if not pool:
        log.error("Seed pool is empty. Check your API key and network.")
        return None

    chosen = random.choice(list(pool))
    log.info(f"Seed pool: {len(pool)} unique videos. Picked: {chosen}")
    return f"https://www.youtube.com/watch?v={chosen}"


def get_video_description(page) -> str | None:
    """
    Scrape the description box from the watch page.
    Tries to expand it first if collapsed, then extracts the full text.
    """
    # Expand the description if it has a "...more" button
    try:
        expand_btn = page.locator(
            "ytd-text-inline-expander tp-yt-paper-button#expand, "
            "#description-inline-expander #expand"
        ).first
        if expand_btn.is_visible(timeout=2000):
            expand_btn.click()
            page.wait_for_timeout(500)
    except Exception:
        pass

    selectors = [
        "#description-inline-expander yt-attributed-string",
        "ytd-text-inline-expander yt-attributed-string",
        "#description yt-attributed-string",
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            text = el.inner_text(timeout=3000)
            if text and text.strip():
                return text.strip()
        except Exception:
            continue
    return None


def click_first_recommendation(page, max_retries: int = 3) -> str | None:
    """
    Click the first recommendation on the watch page sidebar.
    Uses actual clicking (SPA navigation) instead of page.goto() so that
    YouTube's algorithm registers the click event properly.

    Returns the new video URL after navigation, or None on failure.
    """
    # Scroll to ensure recommendations are loaded
    page.evaluate("window.scrollBy(0, 300)")
    page.wait_for_timeout(2000)
    dismiss_popups(page)

    current_vid = extract_video_id(page.url)

    selectors = [
        "ytd-watch-next-secondary-results-renderer a[href*='/watch']",
        "#related a[href*='/watch']",
        "ytd-compact-video-renderer a[href*='/watch']",
    ]

    for attempt in range(max_retries):
        for sel in selectors:
            links = page.locator(sel)
            count = links.count()
            for j in range(min(count, 10)):
                try:
                    href = links.nth(j).get_attribute("href")
                    if not href or "/watch" not in href or "/shorts/" in href:
                        continue
                    # Skip if it's the same video
                    vid = extract_video_id(
                        href if "youtube.com" in href
                        else f"https://www.youtube.com{href}"
                    )
                    if vid == current_vid:
                        continue

                    log.info(f"  Clicking recommendation (attempt {attempt+1}): {href}")
                    links.nth(j).click()

                    # Wait for SPA navigation to complete
                    page.wait_for_timeout(3000)

                    # Verify we actually navigated to a new video
                    new_vid = extract_video_id(page.url)
                    if new_vid and new_vid != current_vid:
                        return clean_watch_url(page.url)

                    # If URL didn't change, the click may not have worked
                    log.warning("  Click didn't navigate. Retrying ...")
                    page.wait_for_timeout(1000)
                    break  # break inner loops to retry

                except Exception as e:
                    log.warning(f"  Click failed ({e}). Retrying ...")
                    page.wait_for_timeout(1000)
                    break
            else:
                continue
            break  # break selector loop if we broke from inner
        else:
            # No selector had valid links — give up
            break

    log.warning("  All click attempts failed.")
    return None


# ---------------------------------------------------------------------------
# Main crawl loop
# ---------------------------------------------------------------------------

def crawl(
    n_iterations: int,
    watch_time: int,
    output_path: str,
    api_key: str,
    headless: bool = True,
    verbose: bool = False,
    run_id: str | None = None,
    seed_url: str | None = None,
):
    """
    Run the YouTube recommendation crawl.

    Args:
        n_iterations: Number of recommendation hops to follow.
        watch_time: Seconds to "watch" each video before clicking next.
        output_path: Path to save the JSON output.
        api_key: YouTube Data API key for fetching trending seed.
        headless: Whether to run the browser in headless mode.
        verbose: If True, log full detail. If False, only print progress lines.
        run_id: Optional identifier shown in progress output (set by run_crawls.py).
        seed_url: If provided, skip pool building and use this URL as the seed.
    """
    # ---- Resolve seed video ----
    if seed_url:
        seed_url = clean_watch_url(seed_url)
        seed_vid = extract_video_id(seed_url)
        log.info(f"Using provided seed: {seed_url}")
    else:
        seed_url = pick_seed(api_key)
        if not seed_url:
            log.error("Could not pick a seed video. Aborting.")
            return
        seed_url = clean_watch_url(seed_url)
        seed_vid = extract_video_id(seed_url)
    _progress(f"Seed: {seed_url}", run_id)

    videos = []
    edges = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
        )

        # Set consent cookies to bypass GDPR/consent dialogs
        context.add_cookies([
            {
                "name": "CONSENT",
                "value": "PENDING+987",
                "domain": ".youtube.com",
                "path": "/",
            },
            {
                "name": "SOCS",
                "value": "CAESEwgDEgk2MTcwNTcyNjUaAmVuIAEaBgiA_L2uBg",
                "domain": ".youtube.com",
                "path": "/",
            },
        ])

        page = context.new_page()

        # Navigate to the seed video
        log.info(f"Opening seed video: {seed_url}")
        page.goto(seed_url, wait_until="domcontentloaded")
        page.wait_for_timeout(5000)
        dismiss_popups(page)

        # ---- Main loop ----
        for i in range(n_iterations):
            current_url = clean_watch_url(page.url)
            current_video_id = extract_video_id(current_url)

            if not current_video_id:
                log.error(
                    f"Iteration {i+1}: cannot extract video ID from {page.url}. Stopping."
                )
                break

            # Get title before progress line so it can be included
            title = get_video_title(page)
            _progress(f"Iteration {i+1}/{n_iterations} | {title} | {current_url}", run_id)
            log.info(f"--- Iteration {i+1}/{n_iterations} ---")
            log.info(f"  Video: {current_url}")
            log.info(f"  Title: {title}")

            # Fetch description
            log.info("  Fetching description ...")
            description = get_video_description(page)
            if description:
                log.info(f"  Description: {len(description)} chars")
            else:
                log.info("  Description: not available")

            # Watch the video
            wait_and_scroll(page, watch_time)

            # Fetch transcript
            log.info("  Fetching transcript ...")
            transcript = fetch_transcript(page, current_video_id)
            if transcript:
                log.info(f"  Transcript: {len(transcript)} chars")
            else:
                log.info("  Transcript: not available")

            # Record video data
            video_data = {
                "video_id": current_video_id,
                "url": current_url,
                "title": title,
                "description": description,
                "transcript": transcript,
                "iteration": i + 1,
                "watch_time": watch_time,
                "timestamp": datetime.now().isoformat(),
            }
            videos.append(video_data)

            # Click first recommendation (unless last iteration)
            if i < n_iterations - 1:
                next_url = click_first_recommendation(page)
                if not next_url:
                    log.error("  No recommendation found. Stopping early.")
                    break

                next_video_id = extract_video_id(next_url)
                if next_video_id:
                    edges.append({
                        "source": current_video_id,
                        "target": next_video_id,
                        "iteration": i + 1,
                    })
                    log.info(f"  Edge: {current_video_id} -> {next_video_id}")
                    page.wait_for_timeout(2000)
                    dismiss_popups(page)
                else:
                    log.warning(f"  Could not extract video ID from {next_url}")
                    break

        browser.close()

    # ---- Save results ----
    result = {
        "metadata": {
            "n_iterations": n_iterations,
            "watch_time_seconds": watch_time,
            "seed_video_id": seed_vid,
            "seed_source": "youtube_data_api_trending_br_multicategory",
            "navigation_method": "click",
            "crawl_date": datetime.now().isoformat(),
            "total_videos": len(videos),
            "total_edges": len(edges),
        },
        "videos": videos,
        "edges": edges,
    }

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    _progress(f"Done — {len(videos)} videos, {len(edges)} edges → {output_path}", run_id)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="YouTube Recommendation Graph Crawler"
    )
    parser.add_argument(
        "-n", "--iterations",
        type=int,
        default=50,
        help="Number of recommendation hops (default: 50)",
    )
    parser.add_argument(
        "-x", "--watch-time",
        type=int,
        default=30,
        help="Seconds to watch each video (default: 30)",
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default="data/crawl.json",
        help="Output JSON file path (default: data/crawl.json)",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run browser in headed mode (visible window)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="YouTube Data API key (default: reads from YOUTUBE_API_KEY env var or .env file)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Log full detail. By default only iteration progress is printed.",
    )
    # Internal args set by run_crawls.py — hidden from user help
    parser.add_argument("--run-id", type=str, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--seed-url", type=str, default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()

    # Configure logging: verbose → INFO (full detail), default → WARNING (suppressed)
    run_prefix = f"[Run {args.run_id}] " if args.run_id else ""
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format=f"%(asctime)s {run_prefix}[%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    # Resolve API key: CLI arg > env var > .env file
    api_key = args.api_key or os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        env_path = Path(__file__).parent / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("YOUTUBE_API_KEY="):
                    api_key = line.split("=", 1)[1].strip()
                    break
    if not api_key:
        parser.error(
            "YouTube Data API key required. Provide via --api-key, "
            "YOUTUBE_API_KEY env var, or .env file."
        )

    crawl(
        n_iterations=args.iterations,
        watch_time=args.watch_time,
        output_path=args.output,
        api_key=api_key,
        headless=not args.headed,
        verbose=args.verbose,
        run_id=args.run_id,
        seed_url=args.seed_url,
    )


if __name__ == "__main__":
    main()
