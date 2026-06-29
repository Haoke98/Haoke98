#!/usr/bin/env python3
"""
Generate a top-languages stats SVG for all GitHub repos (public + private).

Features:
  - Fetches ALL repos (public + private) via GitHub API
  - Smart retry: exponential backoff for transient failures (proxy jitter, 5xx, 429)
  - Local cache: per-repo language data cached for 30 min — restart after crash
    picks up where it left off without re-fetching
  - Generates SVG card matching github-readme-stats style

Usage:
    python3 generate-lang-stats.py --token ***
    python3 generate-lang-stats.py --token *** -t tokyonight -n 15
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from xml.sax.saxutils import escape as xml_escape

# ── Config ────────────────────────────────────────────────────────
DEFAULT_USERNAME = "Haoke98"
DEFAULT_OUTPUT = "lang-stats.svg"
DEFAULT_THEME = "merko"
CACHE_FILE = "lang-cache.json"          # per-repo language cache
CACHE_TTL_MINUTES = 30                   # how long cache is considered fresh
MAX_RETRIES = 3                          # retries per request
BASE_BACKOFF = 2                         # seconds, doubles each retry

# Theme palettes
THEMES = {
    "dark": {
        "bg": "#0d1117", "border": "#30363d", "title": "#58a6ff",
        "text": "#c9d1d9", "bar_bg": "#21262d",
        "bar_colors": [
            "#f78166", "#56d364", "#e3b341", "#6e7681", "#79c0ff",
            "#a5d6ff", "#ffa198", "#aff5b4", "#ff7b72", "#d2a8ff",
            "#ffdfb6", "#d2a8ff", "#f0f6fc", "#8b949e", "#7ee787",
            "#a5d6ff", "#f2cc60", "#e5534b", "#db61a2", "#1f6feb",
        ],
    },
    "merko": {
        "bg": "#0d1117", "border": "#30363d", "title": "#58a6ff",
        "text": "#c9d1d9", "bar_bg": "#21262d",
        "bar_colors": [
            "#006d32", "#0e4429", "#006d32", "#26a641", "#39d353",
            "#006d32", "#0e4429", "#006d32", "#26a641", "#39d353",
        ],
    },
    "tokyonight": {
        "bg": "#1a1b27", "border": "#30363d", "title": "#70a5fd",
        "text": "#38bdae", "bar_bg": "#1f2937",
        "bar_colors": [
            "#bf91f3", "#3fb950", "#ff966c", "#0077b6", "#00b4d8",
            "#90e0ef", "#caf0f8", "#f72585", "#7209b7", "#4361ee",
            "#4cc9f0", "#4895ef", "#560bad", "#f7a1c4", "#b5179e",
            "#3a0ca3", "#ffd166", "#ef476f", "#118ab2", "#06d6a0",
        ],
    },
    "onedark": {
        "bg": "#282c34", "border": "#3b4048", "title": "#61afef",
        "text": "#abb2bf", "bar_bg": "#3b4048",
        "bar_colors": [
            "#e5c07b", "#98c379", "#56b6c2", "#c678dd", "#e06c75",
            "#61afef", "#e5c07b", "#98c379", "#56b6c2", "#c678dd",
        ],
    },
    "nord": {
        "bg": "#2e3440", "border": "#4c566a", "title": "#81a1c1",
        "text": "#d8dee9", "bar_bg": "#4c566a",
        "bar_colors": [
            "#bf616a", "#a3be8c", "#ebcb8b", "#81a1c1", "#b48ead",
            "#88c0d0", "#5e81ac", "#d08770", "#8fbcbb", "#bf616a",
        ],
    },
}

# ── Cache helpers ─────────────────────────────────────────────────

def load_cache(path: str) -> dict:
    """Load per-repo language cache from JSON file."""
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            print("⚠️  Cache file corrupt, starting fresh.", file=sys.stderr)
    return {}


def save_cache(path: str, cache: dict):
    """Save per-repo language cache to JSON file."""
    try:
        with open(path, "w") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except OSError as e:
        print(f"⚠️  Failed to save cache: {e}", file=sys.stderr)


def cache_is_fresh(entry: dict, ttl_minutes: int = CACHE_TTL_MINUTES) -> bool:
    """Check if a cached entry is still within the TTL window."""
    ts = entry.get("_ts", "")
    try:
        cached_time = datetime.fromisoformat(ts)
        age = datetime.now(timezone.utc) - cached_time
        return age < timedelta(minutes=ttl_minutes)
    except (ValueError, TypeError):
        return False


def cache_get(cache: dict, repo_name: str, ttl_minutes: int = CACHE_TTL_MINUTES) -> dict | None:
    """Return cached language data if fresh, otherwise None."""
    entry = cache.get(repo_name)
    if entry and cache_is_fresh(entry, ttl_minutes):
        return {k: v for k, v in entry.items() if k != "_ts"}
    return None


def cache_put(cache: dict, repo_name: str, data: dict):
    """Store language data in cache with timestamp."""
    cache[repo_name] = {"_ts": datetime.now(timezone.utc).isoformat(), **data}


# ── API client ────────────────────────────────────────────────────

def api_get(url: str, token: str, log_prefix: str = "") -> dict:
    """
    Make an authenticated GitHub API request with exponential-backoff retry.
    Handles transient failures (proxy jitter, 5xx, 429, DNS failures, etc.)
    gracefully — retries up to MAX_RETRIES then returns empty on failure.
    """
    req = Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("User-Agent", "lang-stats-generator/2.0")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode())

        except HTTPError as e:
            status, msg = e.code, e.reason

            # Auth errors — don't retry
            if status == 401:
                print(f"  ❌ {log_prefix} 401 Unauthorized — token invalid or expired.", file=sys.stderr)
                return {}
            if status == 403 and "rate limit" in str(e.headers).lower():
                # Rate limited — wait longer
                wait = 60
                print(f"  ⏳ {log_prefix} 403 Rate limited, waiting {wait}s...", file=sys.stderr)
                time.sleep(wait)
                continue

            # Not found — don't retry (empty repo?)
            if status == 404:
                print(f"  ⚠️  {log_prefix} 404 Not Found — skipping.", file=sys.stderr)
                return {}

            # Transient: 429, 451, 5xx, or unknown — retry
            if status == 429:
                print(f"  ⏳ {log_prefix} 429 Too Many Requests.", file=sys.stderr)
            elif status == 451:
                print(f"  ⚠️  {log_prefix} 451 Unavailable (regional/proxy block).", file=sys.stderr)
            elif 500 <= status < 600:
                print(f"  ⚠️  {log_prefix} {status} Server Error.", file=sys.stderr)
            else:
                print(f"  ⚠️  {log_prefix} HTTP {status}: {msg}.", file=sys.stderr)

            if attempt == MAX_RETRIES:
                print(f"  ❌ {log_prefix} Failed after {MAX_RETRIES} attempts — skipping.", file=sys.stderr)
                return {}
            wait = BASE_BACKOFF ** attempt
            print(f"  🔄 {log_prefix} Retry {attempt}/{MAX_RETRIES} in {wait}s...", file=sys.stderr)
            time.sleep(wait)

        except URLError as e:
            reason = str(e.reason) if hasattr(e, 'reason') else str(e)
            print(f"  ⚠️  {log_prefix} Network error: {reason}.", file=sys.stderr)
            if attempt == MAX_RETRIES:
                print(f"  ❌ {log_prefix} Failed after {MAX_RETRIES} attempts — skipping.", file=sys.stderr)
                return {}
            wait = BASE_BACKOFF ** attempt
            print(f"  🔄 {log_prefix} Retry {attempt}/{MAX_RETRIES} in {wait}s...", file=sys.stderr)
            time.sleep(wait)

        except Exception as e:
            print(f"  ⚠️  {log_prefix} Unexpected error: {e}.", file=sys.stderr)
            if attempt == MAX_RETRIES:
                print(f"  ❌ {log_prefix} Failed after {MAX_RETRIES} attempts — skipping.", file=sys.stderr)
                return {}
            wait = BASE_BACKOFF ** attempt
            time.sleep(wait)

    return {}


# ── Business logic ────────────────────────────────────────────────

def fetch_all_repos(username: str, token: str) -> list:
    """Fetch all repos (public + private) with pagination and retry."""
    all_repos = []
    page = 1
    while True:
        url = f"https://api.github.com/user/repos?type=all&per_page=100&page={page}&sort=updated"
        print(f"📡 Fetching repo list — page {page}...", file=sys.stderr)
        repos = api_get(url, token, log_prefix=f"repos-p{page}")

        if not repos:
            break

        all_repos.extend(repos)
        if len(repos) < 100:
            break
        page += 1
        time.sleep(0.3)  # gentle pacing

    own = [r for r in all_repos if r.get("owner", {}).get("login") == username]
    print(f"✅ Found {len(own)} repos total (public + private)", file=sys.stderr)
    return own


def fetch_repo_languages(owner: str, repo: str, token: str) -> dict:
    """Fetch language breakdown for a single repo."""
    url = f"https://api.github.com/repos/{owner}/{repo}/languages"
    return api_get(url, token, log_prefix=f"{repo}")


def aggregate_languages(repos: list, token: str, cache: dict, cache_path: str,
                        ttl_minutes: int = CACHE_TTL_MINUTES) -> list:
    """
    Walk all repos, fetch language data (with cache), and aggregate.
    Returns sorted list of (language, total_bytes).
    """
    totals = defaultdict(int)
    total = len(repos)
    skipped = 0
    cached_hits = 0

    for i, repo in enumerate(repos):
        name = repo["name"]
        owner = repo["owner"]["login"]

        # Check cache first
        cached = cache_get(cache, name, ttl_minutes)
        if cached:
            print(f"📊 [{i + 1}/{total}] {name} ← cache hit (+{len(cached)} langs)", file=sys.stderr)
            for lang, b in cached.items():
                totals[lang] += b
            cached_hits += 1
            continue

        # Fetch from API
        print(f"📊 [{i + 1}/{total}] {name}...", file=sys.stderr)
        langs = fetch_repo_languages(owner, name, token)

        if not langs:
            # Could be an empty response or an error — don't cache
            print(f"  ⚠️  {name} returned no data — skipping (not cached)", file=sys.stderr)
            skipped += 1
        else:
            # Cache the result
            cache_put(cache, name, langs)
            save_cache(cache_path, cache)

            for lang, b in langs.items():
                totals[lang] += b

        # Pacing to avoid secondary rate limits
        if (i + 1) % 5 == 0:
            time.sleep(0.5)

    print(f"\n📊 Summary:", file=sys.stderr)
    print(f"   Processed: {total - skipped}/{total} repos", file=sys.stderr)
    print(f"   Cache hits: {cached_hits}", file=sys.stderr)
    print(f"   Skipped (errors): {skipped}", file=sys.stderr)

    sorted_langs = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    return sorted_langs


def format_bytes(b: int) -> str:
    """Human-readable byte count."""
    if b >= 1_000_000:
        return f"{b / 1_000_000:.1f} MB"
    elif b >= 1_000:
        return f"{b / 1_000:.0f} KB"
    else:
        return f"{b} B"


def generate_svg(languages: list, username: str, theme_name: str,
                 output_path: str, top_n: int = 20):
    """Generate the top-languages SVG card."""
    theme = THEMES.get(theme_name, THEMES["merko"])

    card_width = 360
    row_height = 24
    header_height = 60
    padding_x = 16
    padding_y = 12

    content_rows = min(len(languages), top_n)
    card_height = header_height + content_rows * row_height + padding_y * 2
    total_bytes = sum(b for _, b in languages[:top_n])

    bar_x = padding_x
    bar_width = card_width - padding_x * 2
    bar_height = 10
    max_bar_text_width = 80
    bar_usable_width = bar_width - max_bar_text_width - 10
    text_x = bar_x + max_bar_text_width + 10

    svg_parts = [f'''<?xml version="1.0" encoding="UTF-8"?>
<svg width="{card_width}" height="{card_height}" viewBox="0 0 {card_width} {card_height}"
     xmlns="http://www.w3.org/2000/svg">
  <style>
    text {{ font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace; }}
  </style>
  <rect x="0" y="0" width="{card_width}" height="{card_height}" rx="6" fill="{theme['bg']}"/>
  <rect x="0" y="0" width="{card_width}" height="{card_height}" rx="6"
        fill="none" stroke="{theme['border']}" stroke-width="1"/>

  <text x="{padding_x}" y="30" fill="{theme['title']}" font-size="14" font-weight="700">
    Top Languages
  </text>
  <text x="{padding_x}" y="50" fill="{theme['text']}" font-size="11" opacity="0.6">
    Public + Private repos
  </text>
''']

    for idx, (lang, bytes_count) in enumerate(languages[:top_n]):
        y = header_height + idx * row_height
        pct = (bytes_count / total_bytes * 100) if total_bytes > 0 else 0
        bar_color = theme["bar_colors"][idx % len(theme["bar_colors"])]
        bar_fill_width = max(0, int(bar_usable_width * (pct / 100)))

        svg_parts.append(f'''
  <text x="{bar_x}" y="{y + 16}" fill="{theme['text']}" font-size="12"
        text-anchor="start" dominant-baseline="middle">{xml_escape(lang)}</text>
  <rect x="{text_x}" y="{y + 6}" width="{bar_usable_width}" height="{bar_height}"
        rx="5" fill="{theme['bar_bg']}"/>
  <rect x="{text_x}" y="{y + 6}" width="{bar_fill_width}" height="{bar_height}"
        rx="5" fill="{bar_color}"/>
  <text x="{card_width - padding_x}" y="{y + 16}" fill="{theme['text']}" font-size="10"
        text-anchor="end" dominant-baseline="middle" opacity="0.8">
    {pct:.1f}%  {format_bytes(bytes_count)}
  </text>
''')

    svg_parts.append('\n</svg>')
    svg_content = "".join(svg_parts)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(svg_content)

    print(f"\n✅ SVG saved to: {output_path}", file=sys.stderr)
    print(f"   Languages: {content_rows} shown of {len(languages)} total", file=sys.stderr)
    print(f"   Total code: {format_bytes(total_bytes)}", file=sys.stderr)


# ── Main ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate GitHub top-languages stats SVG (public + private repos)"
    )
    parser.add_argument("-t", "--token", help="GitHub token (or GITHUB_TOKEN env var)")
    parser.add_argument("-u", "--username", default=DEFAULT_USERNAME)
    parser.add_argument("-o", "--output", default=DEFAULT_OUTPUT)
    parser.add_argument("-T", "--theme", default=DEFAULT_THEME, choices=list(THEMES.keys()))
    parser.add_argument("-n", "--top", type=int, default=20)
    parser.add_argument("--cache-ttl", type=int, default=CACHE_TTL_MINUTES,
                        help=f"Cache TTL in minutes (default: {CACHE_TTL_MINUTES})")
    parser.add_argument("--no-cache", action="store_true",
                        help="Ignore cache and re-fetch everything")
    parser.add_argument("--save-json", help="Save raw data as JSON")
    args = parser.parse_args()

    # Token
    token = args.token or os.environ.get("GITHUB_TOKEN")
    if not token:
        print("❌ GitHub token required.", file=sys.stderr)
        print("   Set GITHUB_TOKEN env var or use --token", file=sys.stderr)
        sys.exit(1)

    print(f"🔍 GitHub user:  {args.username}", file=sys.stderr)
    print(f"🎨 Theme:        {args.theme}", file=sys.stderr)
    print(f"📁 Output:       {args.output}", file=sys.stderr)
    print(f"💾 Cache TTL:    {args.cache_ttl} min", file=sys.stderr)
    print(f"🔄 Retries:      {MAX_RETRIES} per request", file=sys.stderr)
    print("", file=sys.stderr)

    # Load cache
    cache_path = os.path.join(os.path.dirname(__file__) or ".", CACHE_FILE)
    cache = {} if args.no_cache else load_cache(cache_path)
    if not args.no_cache and cache:
        print(f"📦 Loaded cache: {len(cache)} entries\n", file=sys.stderr)

    # 1. Fetch repos
    repos = fetch_all_repos(args.username, token)
    if not repos:
        print("❌ No repos found.", file=sys.stderr)
        sys.exit(1)

    public = sum(1 for r in repos if not r.get("private", False))
    private = sum(1 for r in repos if r.get("private", False))
    print(f"   → {public} public, {private} private\n", file=sys.stderr)

    # 2. Aggregate languages (with cache)
    languages = aggregate_languages(repos, token, cache, cache_path,
                                    ttl_minutes=args.cache_ttl)

    if not languages:
        print("⚠️  No language data collected.", file=sys.stderr)
        sys.exit(1)

    print(f"\n📊 Top languages:\n", file=sys.stderr)
    for lang, b in languages[:10]:
        print(f"   {lang:20s} {format_bytes(b)}", file=sys.stderr)

    # 3. Save JSON
    if args.save_json:
        with open(args.save_json, "w") as f:
            json.dump({lang: b for lang, b in languages}, f, indent=2)
        print(f"\n💾 Raw data saved to: {args.save_json}", file=sys.stderr)

    # 4. Generate SVG
    generate_svg(languages, args.username, args.theme, args.output, args.top)


if __name__ == "__main__":
    main()
