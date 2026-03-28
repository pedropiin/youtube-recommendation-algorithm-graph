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
import re
import logging
from datetime import datetime
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright
from youtube_transcript_api import YouTubeTranscriptApi

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


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


_transcript_api = YouTubeTranscriptApi()


def fetch_transcript(video_id: str) -> str | None:
    """
    Fetch the transcript for a YouTube video.
    Tries Portuguese first, then English, then any available language.
    Returns the full transcript as a single string, or None on failure.
    """
    # Try preferred languages first
    for langs in [["pt", "pt-BR"], ["en"], None]:
        try:
            if langs:
                result = _transcript_api.fetch(video_id, languages=langs)
            else:
                # No language preference — get whatever is available
                transcript_list = _transcript_api.list(video_id)
                first = next(iter(transcript_list))
                result = _transcript_api.fetch(video_id, languages=[first.language_code])
            return " ".join(snippet.text for snippet in result)
        except Exception:
            continue

    log.warning(f"Could not fetch transcript for {video_id}")
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


def fetch_trending_seed(api_key: str) -> str | None:
    """
    Fetch the first trending (most popular) video in Brazil via YouTube Data API.
    Returns the video URL or None on failure.
    """
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "id",
        "chart": "mostPopular",
        "regionCode": "BR",
        "maxResults": 1,
        "key": api_key,
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        if items:
            video_id = items[0]["id"]
            log.info(f"Trending seed from API: {video_id}")
            return f"https://www.youtube.com/watch?v={video_id}"
    except Exception as e:
        log.warning(f"YouTube Data API call failed: {e}")

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
):
    """
    Run the YouTube recommendation crawl.

    Args:
        n_iterations: Number of recommendation hops to follow.
        watch_time: Seconds to "watch" each video before clicking next.
        output_path: Path to save the JSON output.
        api_key: YouTube Data API key for fetching trending seed.
        headless: Whether to run the browser in headless mode.
    """
    # ---- Get seed video via YouTube Data API ----
    log.info("Fetching trending seed video via YouTube Data API ...")
    seed_url = fetch_trending_seed(api_key)
    if not seed_url:
        log.error("Could not fetch trending seed. Check your API key. Aborting.")
        return

    seed_url = clean_watch_url(seed_url)
    seed_vid = extract_video_id(seed_url)
    log.info(f"Seed video: {seed_url} (id={seed_vid})")

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

            log.info(f"--- Iteration {i+1}/{n_iterations} ---")
            log.info(f"  Video: {current_url}")

            # Get title
            title = get_video_title(page)
            log.info(f"  Title: {title}")

            # Watch the video
            wait_and_scroll(page, watch_time)

            # Fetch transcript
            log.info("  Fetching transcript ...")
            transcript = fetch_transcript(current_video_id)
            if transcript:
                log.info(f"  Transcript: {len(transcript)} chars")
            else:
                log.info("  Transcript: not available")

            # Record video data
            video_data = {
                "video_id": current_video_id,
                "url": current_url,
                "title": title,
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
            "seed_source": "youtube_data_api_trending_br",
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

    log.info(f"Crawl complete! {len(videos)} videos, {len(edges)} edges.")
    log.info(f"Data saved to {output_path}")


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
    args = parser.parse_args()

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
    )


if __name__ == "__main__":
    main()
