#!/usr/bin/env python3
"""
GitHub Language Stats — fetch, store, and visualize.

Default: fetch language data from GitHub API, store in local SQLite DB.
Use flags to control SVG generation.

Database schema (lang-stats.db):
  runs            — one row per run (timestamp, repo counts, totals)
  repo_languages  — per-repo language breakdown for each run
  repo_metadata   — per-repo stars/forks/issues snapshot for each run
  language_stats  — aggregated stats per run (JSON)

Old databases are upgraded in place: columns added by newer versions are
appended with ALTER TABLE (after a timestamped .bak copy), so earlier runs
are never dropped. Every run is a new row — history is append-only.

Usage:
    # 1. Fetch data + store to DB only (default)
    python3 generate-lang-stats.py --token ***

    # 2. Fetch + store + generate SVG
    python3 generate-lang-stats.py --token *** --svg

    # 3. Generate SVG from latest DB data (no API calls)
    python3 generate-lang-stats.py --svg-only

    # 4. --svg-only with theme
    python3 generate-lang-stats.py --svg-only -t tokyonight -n 15

    # 5. Keep repos whose language call failed by reusing the previous
    #    run's numbers instead of dropping them from the new totals
    python3 generate-lang-stats.py --token *** --reuse-missing
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from xml.sax.saxutils import escape as xml_escape

# ── Paths ─────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "lang-stats.db")
CACHE_PATH = os.path.join(SCRIPT_DIR, "lang-cache.json")
SVG_PATH = os.path.join(SCRIPT_DIR, "..", "lang-stats.svg")

# ── Config ────────────────────────────────────────────────────────
DEFAULT_USERNAME = "Haoke98"
DEFAULT_THEME = "merko"
SCRIPT_VERSION = "3.1"
CACHE_TTL_MINUTES = 30
MAX_RETRIES = 3
BASE_BACKOFF = 2

THEMES = {
    "dark": {
        "bg": "#0d1117", "border": "#30363d", "title": "#58a6ff",
        "text": "#c9d1d9", "bar_bg": "#21262d",
        "bar_colors": [
            "#f78166", "#56d364", "#e3b341", "#6e7681", "#79c0ff",
            "#a5d6ff", "#ffa198", "#aff5b4", "#ff7b72", "#d2a8ff",
        ],
    },
    "merko": {
        "bg": "#0d1117", "border": "#30363d", "title": "#58a6ff",
        "text": "#c9d1d9", "bar_bg": "#21262d",
        "bar_colors": [
            "#006d32", "#0e4429", "#006d32", "#26a641", "#39d353",
        ],
    },
    "tokyonight": {
        "bg": "#1a1b27", "border": "#30363d", "title": "#70a5fd",
        "text": "#38bdae", "bar_bg": "#1f2937",
        "bar_colors": [
            "#bf91f3", "#3fb950", "#ff966c", "#0077b6", "#00b4d8",
        ],
    },
    "onedark": {
        "bg": "#282c34", "border": "#3b4048", "title": "#61afef",
        "text": "#abb2bf", "bar_bg": "#3b4048",
        "bar_colors": [
            "#e5c07b", "#98c379", "#56b6c2", "#c678dd", "#e06c75",
        ],
    },
    "nord": {
        "bg": "#2e3440", "border": "#4c566a", "title": "#81a1c1",
        "text": "#d8dee9", "bar_bg": "#4c566a",
        "bar_colors": [
            "#bf616a", "#a3be8c", "#ebcb8b", "#81a1c1", "#b48ead",
        ],
    },
}

# ── Database ──────────────────────────────────────────────────────

SCHEMA_SQL = """
        CREATE TABLE IF NOT EXISTS runs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            username    TEXT NOT NULL,
            timestamp   TEXT NOT NULL,
            total_repos INTEGER,
            public_repos INTEGER,
            private_repos INTEGER,
            total_bytes INTEGER,
            language_count INTEGER,
            total_stars  INTEGER DEFAULT 0,
            total_forks  INTEGER DEFAULT 0,
            total_issues INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS repo_languages (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id    INTEGER NOT NULL REFERENCES runs(id),
            repo_name TEXT NOT NULL,
            language  TEXT NOT NULL,
            bytes     INTEGER NOT NULL,
            UNIQUE(run_id, repo_name, language)
        );

        CREATE TABLE IF NOT EXISTS repo_metadata (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id     INTEGER NOT NULL REFERENCES runs(id),
            repo_name  TEXT NOT NULL,
            stars      INTEGER DEFAULT 0,
            forks      INTEGER DEFAULT 0,
            open_issues INTEGER DEFAULT 0,
            watchers   INTEGER DEFAULT 0,
            size_kb    INTEGER DEFAULT 0,
            is_fork    INTEGER DEFAULT 0,
            is_private INTEGER DEFAULT 0,
            language   TEXT,
            default_branch TEXT,
            created_at TEXT,
            updated_at TEXT,
            pushed_at  TEXT,
            UNIQUE(run_id, repo_name)
        );

        CREATE TABLE IF NOT EXISTS language_stats (
            run_id     INTEGER PRIMARY KEY REFERENCES runs(id),
            stats_json TEXT NOT NULL
        );
"""

# Columns introduced after a table was first released. `CREATE TABLE IF NOT
# EXISTS` silently ignores existing tables, so an old DB would otherwise blow
# up on the first INSERT that mentions a newer column. Additive only, and the
# DEFAULT fills the rows already stored — no run is ever rewritten or dropped.
SCHEMA_UPGRADES = {
    "runs": {
        "total_stars": "INTEGER DEFAULT 0",
        "total_forks": "INTEGER DEFAULT 0",
        "total_issues": "INTEGER DEFAULT 0",
    },
}


def _table_columns(conn, table) -> set:
    return {row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')}


def _pending_upgrades(conn) -> list:
    """Return [(table, column, sql_type)] missing from an existing DB."""
    pending = []
    for table, columns in SCHEMA_UPGRADES.items():
        existing = _table_columns(conn, table)
        if not existing:
            continue  # table missing entirely — SCHEMA_SQL just created it
        for column, decl in columns.items():
            if column not in existing:
                pending.append((table, column, decl))
    return pending


def backup_db(conn, reason="migration") -> str:
    """WAL-safe snapshot next to the DB; returns the backup path."""
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    dst = f"{DB_PATH}.bak-{reason}-{stamp}"
    with sqlite3.connect(dst) as bck:
        conn.backup(bck)
    return dst


def backfill_run_totals(conn) -> int:
    """Recover star/fork/issue totals from repo_metadata where it exists.

    Only touches runs whose totals are still 0 and that actually have metadata
    rows, so upgraded databases show real numbers for their old runs.
    """
    rows = conn.execute(
        "SELECT r.id, SUM(m.stars), SUM(m.forks), SUM(m.open_issues) "
        "FROM runs r JOIN repo_metadata m ON m.run_id = r.id "
        "WHERE COALESCE(r.total_stars, 0) = 0 "
        "  AND COALESCE(r.total_forks, 0) = 0 "
        "GROUP BY r.id HAVING COUNT(m.id) > 0"
    ).fetchall()
    for run_id, stars, forks, issues in rows:
        conn.execute(
            "UPDATE runs SET total_stars=?, total_forks=?, total_issues=? WHERE id=?",
            (stars or 0, forks or 0, issues or 0, run_id),
        )
    if rows:
        conn.commit()
    return len(rows)


def migrate_db(conn, pre_existing: bool) -> list:
    """Add missing columns to an existing DB without losing a single row."""
    pending = _pending_upgrades(conn)
    if not pending:
        return []
    if pre_existing:
        print(f"🗄️  Backup before upgrade: {backup_db(conn)}", file=sys.stderr)
    for table, column, decl in pending:
        conn.execute(f'ALTER TABLE "{table}" ADD COLUMN {column} {decl}')
    conn.commit()
    print("🔧 Schema upgraded in place (history kept): "
          + ", ".join(f"{t}.{c}" for t, c, _ in pending), file=sys.stderr)
    recovered = backfill_run_totals(conn)
    if recovered:
        print(f"   ↺ Recovered star/fork/issue totals for {recovered} old run(s) "
              f"from repo_metadata", file=sys.stderr)
    return pending


def init_db() -> sqlite3.Connection:
    """Open the DB, creating or upgrading the schema as needed."""
    pre_existing = os.path.exists(DB_PATH) and os.path.getsize(DB_PATH) > 0
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    migrate_db(conn, pre_existing)
    return conn


def save_run(conn, username, total_repos, public_repos, private_repos,
             total_bytes, lang_count, total_stars=0, total_forks=0, total_issues=0):
    """Insert a new run and return its ID."""
    ts = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "INSERT INTO runs (username, timestamp, total_repos, public_repos, "
        "private_repos, total_bytes, language_count, total_stars, total_forks, total_issues) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (username, ts, total_repos, public_repos, private_repos,
         total_bytes, lang_count, total_stars, total_forks, total_issues),
    )
    conn.commit()
    return cur.lastrowid


def save_repo_languages(conn, run_id, repo_name, langs):
    """Insert language breakdown for one repo."""
    rows = [(run_id, repo_name, lang, b) for lang, b in langs.items()]
    conn.executemany(
        "INSERT OR IGNORE INTO repo_languages (run_id, repo_name, language, bytes) "
        "VALUES (?,?,?,?)", rows,
    )
    conn.commit()


def save_repo_metadata(conn, run_id, repo):
    """Insert metadata (stars, forks, issues, etc.) for one repo."""
    conn.execute(
        "INSERT OR IGNORE INTO repo_metadata "
        "(run_id, repo_name, stars, forks, open_issues, watchers, size_kb, "
        "is_fork, is_private, language, default_branch, created_at, updated_at, pushed_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            run_id,
            repo.get("name", ""),
            repo.get("stargazers_count", 0) or 0,
            repo.get("forks_count", 0) or 0,
            repo.get("open_issues_count", 0) or 0,
            repo.get("watchers_count", 0) or 0,
            repo.get("size", 0) or 0,
            1 if repo.get("fork") else 0,
            1 if repo.get("private") else 0,
            repo.get("language", ""),
            repo.get("default_branch", ""),
            repo.get("created_at", ""),
            repo.get("updated_at", ""),
            repo.get("pushed_at", ""),
        ),
    )
    conn.commit()


def save_language_stats(conn, run_id, languages):
    """Store aggregated language stats as JSON."""
    stats = {lang: b for lang, b in languages}
    conn.execute(
        "INSERT OR REPLACE INTO language_stats (run_id, stats_json) VALUES (?,?)",
        (run_id, json.dumps(stats, ensure_ascii=False)),
    )
    conn.commit()


def get_latest_stats(conn, username):
    """Return the most recent aggregated stats for a user."""
    row = conn.execute(
        "SELECT ls.stats_json, r.timestamp FROM language_stats ls "
        "JOIN runs r ON r.id = ls.run_id "
        "WHERE r.username = ? ORDER BY r.id DESC LIMIT 1",
        (username,),
    ).fetchone()
    if row:
        stats = json.loads(row[0])
        sorted_langs = sorted(stats.items(), key=lambda x: x[1], reverse=True)
        return sorted_langs, row[1]
    return None, None


def get_recent_runs(conn, username, limit=5):
    """Return recent runs summary."""
    return conn.execute(
        "SELECT id, timestamp, total_repos, public_repos, private_repos, "
        "total_bytes, language_count, total_stars, total_forks, total_issues "
        "FROM runs WHERE username = ? ORDER BY id DESC LIMIT ?",
        (username, limit),
    ).fetchall()


# ── Cache helpers ─────────────────────────────────────────────────

def load_cache():
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            print("⚠️  Cache corrupt, starting fresh.", file=sys.stderr)
    return {}


def save_cache(cache):
    try:
        with open(CACHE_PATH, "w") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except OSError as e:
        print(f"⚠️  Failed to save cache: {e}", file=sys.stderr)


def cache_is_fresh(entry, ttl_minutes=CACHE_TTL_MINUTES):
    ts = entry.get("_ts", "")
    try:
        age = datetime.now(timezone.utc) - datetime.fromisoformat(ts)
        return age < timedelta(minutes=ttl_minutes)
    except (ValueError, TypeError):
        return False


def cache_get(cache, repo_name, ttl_minutes=CACHE_TTL_MINUTES):
    entry = cache.get(repo_name)
    if entry and cache_is_fresh(entry, ttl_minutes):
        return {k: v for k, v in entry.items() if k != "_ts"}
    return None


def cache_put(cache, repo_name, data):
    cache[repo_name] = {"_ts": datetime.now(timezone.utc).isoformat(), **data}


# ── API client ────────────────────────────────────────────────────

def api_get(url, token, log_prefix=""):
    req = Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("User-Agent", f"lang-stats-generator/{SCRIPT_VERSION}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode())
        except HTTPError as e:
            if e.code == 401:
                print(f"  ❌ {log_prefix} 401 Unauthorized.", file=sys.stderr)
                return {}
            if e.code == 404:
                return {}
            if e.code == 403 and "rate limit" in str(e.headers).lower():
                time.sleep(60); continue
            if attempt == MAX_RETRIES:
                print(f"  ❌ {log_prefix} HTTP {e.code} — skipped.", file=sys.stderr)
                return {}
        except URLError as e:
            if attempt == MAX_RETRIES:
                print(f"  ❌ {log_prefix} Network error — skipped.", file=sys.stderr)
                return {}
        except Exception:
            if attempt == MAX_RETRIES:
                print(f"  ❌ {log_prefix} Error — skipped.", file=sys.stderr)
                return {}
        wait = BASE_BACKOFF ** attempt
        time.sleep(wait)
    return {}


# ── Business logic ────────────────────────────────────────────────

def fetch_all_repos(username, token):
    all_repos = []
    page = 1
    while True:
        url = f"https://api.github.com/user/repos?type=all&per_page=100&page={page}&sort=updated"
        print(f"📡 Fetching repo list — page {page}...", file=sys.stderr)
        repos = api_get(url, token, f"repos-p{page}")
        if not repos:
            break
        all_repos.extend(repos)
        if len(repos) < 100:
            break
        page += 1
        time.sleep(0.3)
    own = [r for r in all_repos if r.get("owner", {}).get("login") == username]
    print(f"✅ Found {len(own)} repos", file=sys.stderr)
    return own


def fetch_repo_languages(owner, repo, token):
    return api_get(f"https://api.github.com/repos/{owner}/{repo}/languages",
                   token, repo)


def latest_repo_languages(conn, repo_name, before_run_id):
    """Most recent stored language breakdown for a repo, older than run_id.

    Used with --reuse-missing so a transient API failure does not silently
    shrink this run's totals for repos that were measured before.
    """
    row = conn.execute(
        "SELECT run_id FROM repo_languages WHERE repo_name = ? AND run_id < ? "
        "ORDER BY run_id DESC LIMIT 1",
        (repo_name, before_run_id),
    ).fetchone()
    if not row:
        return None, None
    langs = dict(conn.execute(
        "SELECT language, bytes FROM repo_languages WHERE run_id = ? AND repo_name = ?",
        (row[0], repo_name),
    ).fetchall())
    return (langs or None), row[0]


def collect_and_store(username, token, cache, no_cache, reuse_missing=False):
    """Fetch all data, store in DB and cache. Returns (run_id, languages_list, totals)."""
    conn = init_db()
    ttl = CACHE_TTL_MINUTES if not no_cache else 0

    # 1. Fetch repos
    repos = fetch_all_repos(username, token)
    if not repos:
        print("❌ No repos found.", file=sys.stderr)
        conn.close()
        sys.exit(1)

    public = sum(1 for r in repos if not r.get("private", False))
    private = sum(1 for r in repos if r.get("private", False))
    print(f"   → {public} public, {private} private\n", file=sys.stderr)

    # 2. Walk repos
    totals = defaultdict(int)
    total_repos = len(repos)
    cached_hits = 0
    skipped = 0
    reused = 0
    total_stars = 0
    total_forks = 0
    total_open_issues = 0

    # Save run first (we'll update counts later)
    run_id = save_run(conn, username, total_repos, public, private, 0, 0)

    for i, repo in enumerate(repos):
        name = repo["name"]
        owner = repo["owner"]["login"]

        # Save metadata (stars/forks/issues/etc.) for EVERY repo
        save_repo_metadata(conn, run_id, repo)
        total_stars += repo.get("stargazers_count", 0) or 0
        total_forks += repo.get("forks_count", 0) or 0
        total_open_issues += repo.get("open_issues_count", 0) or 0

        # Check cache
        cached = cache_get(cache, name, ttl) if ttl > 0 else None
        if cached:
            print(f"📊 [{i+1}/{total_repos}] {name} ← cache hit (+{len(cached)} langs)", file=sys.stderr)
            save_repo_languages(conn, run_id, name, cached)
            for lang, b in cached.items():
                totals[lang] += b
            cached_hits += 1
            continue

        # Fetch from API
        print(f"📊 [{i+1}/{total_repos}] {name}...", file=sys.stderr)
        langs = fetch_repo_languages(owner, name, token)

        if not langs:
            if reuse_missing:
                prev, prev_run = latest_repo_languages(conn, name, run_id)
                if prev:
                    print(f"↩ [{i+1}/{total_repos}] {name} — API returned nothing, "
                          f"reusing run {prev_run} ({len(prev)} langs)", file=sys.stderr)
                    save_repo_languages(conn, run_id, name, prev)
                    for lang, b in prev.items():
                        totals[lang] += b
                    reused += 1
                    continue
            skipped += 1
        else:
            if ttl > 0:
                cache_put(cache, name, langs)
                save_cache(cache)
            save_repo_languages(conn, run_id, name, langs)
            for lang, b in langs.items():
                totals[lang] += b

        if (i + 1) % 5 == 0:
            time.sleep(0.5)

    sorted_langs = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    total_bytes = sum(b for _, b in sorted_langs)

    # Update run with totals
    conn.execute(
        "UPDATE runs SET total_bytes=?, language_count=?, "
        "total_stars=?, total_forks=?, total_issues=? WHERE id=?",
        (total_bytes, len(sorted_langs), total_stars, total_forks, total_open_issues, run_id),
    )
    save_language_stats(conn, run_id, sorted_langs)
    conn.commit()

    print(f"\n📊 Summary:", file=sys.stderr)
    print(f"   Repos processed: {total_repos - skipped}/{total_repos}", file=sys.stderr)
    print(f"   Cache hits:      {cached_hits}", file=sys.stderr)
    if reused:
        print(f"   Reused history:  {reused}", file=sys.stderr)
    print(f"   Skipped:         {skipped}", file=sys.stderr)
    print(f"   ⭐ Total stars:  {total_stars:,}", file=sys.stderr)
    print(f"   🍴 Total forks:  {total_forks:,}", file=sys.stderr)
    print(f"   📝 Open issues:  {total_open_issues:,}", file=sys.stderr)
    print(f"   Languages:       {len(sorted_langs)}", file=sys.stderr)
    print(f"   Total code:      {format_bytes(total_bytes)}", file=sys.stderr)
    print(f"   Run ID:          {run_id}", file=sys.stderr)

    # Print top 10
    print(f"\n📊 Top languages:", file=sys.stderr)
    for lang, b in sorted_langs[:10]:
        pct = b / total_bytes * 100 if total_bytes > 0 else 0
        print(f"   {lang:20s} {format_bytes(b):>10s}  ({pct:5.1f}%)", file=sys.stderr)

    conn.close()
    return run_id, sorted_langs, total_bytes


# ── SVG generation ────────────────────────────────────────────────

def format_bytes(b):
    if b >= 1_000_000:
        return f"{b/1_000_000:.1f} MB"
    elif b >= 1_000:
        return f"{b/1_000:.0f} KB"
    return f"{b} B"


def generate_svg(languages, username, theme_name, output_path, top_n=20):
    theme = THEMES.get(theme_name, THEMES["merko"])
    card_width = 360
    row_height = 24
    header_height = 60
    padding_x = 16
    padding_y = 12
    content_rows = min(len(languages), top_n)
    card_height = header_height + content_rows * row_height + padding_y * 2
    total_bytes = sum(b for _, b in languages[:top_n])

    bar_usable_width = card_width - padding_x * 2 - 90
    text_x = padding_x + 80

    parts = [f'''<?xml version="1.0" encoding="UTF-8"?>
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

    for idx, (lang, b) in enumerate(languages[:top_n]):
        y = header_height + idx * row_height
        pct = b / total_bytes * 100 if total_bytes > 0 else 0
        color = theme["bar_colors"][idx % len(theme["bar_colors"])]
        fill_w = max(0, int(bar_usable_width * (pct / 100)))

        parts.append(f'''
  <text x="{padding_x}" y="{y+16}" fill="{theme['text']}" font-size="12"
        dominant-baseline="middle">{xml_escape(lang)}</text>
  <rect x="{text_x}" y="{y+6}" width="{bar_usable_width}" height="10" rx="5"
        fill="{theme['bar_bg']}"/>
  <rect x="{text_x}" y="{y+6}" width="{fill_w}" height="10" rx="5" fill="{color}"/>
  <text x="{card_width-padding_x}" y="{y+16}" fill="{theme['text']}" font-size="10"
        text-anchor="end" dominant-baseline="middle" opacity="0.8">
    {pct:.1f}%  {format_bytes(b)}
  </text>
''')

    parts.append('\n</svg>')

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("".join(parts))

    print(f"✅ SVG saved to: {output_path}", file=sys.stderr)
    print(f"   {content_rows} languages shown, {format_bytes(total_bytes)} total", file=sys.stderr)


# ── Main ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="GitHub language stats — fetch, store, visualize"
    )
    parser.add_argument("-t", "--token", help="GitHub token (or GITHUB_TOKEN env var)")
    parser.add_argument("-u", "--username", default=DEFAULT_USERNAME)
    parser.add_argument("-T", "--theme", default=DEFAULT_THEME, choices=list(THEMES.keys()))
    parser.add_argument("-n", "--top", type=int, default=20)
    parser.add_argument("-o", "--output", default=SVG_PATH, help="SVG output path")
    parser.add_argument("--no-cache", action="store_true", help="Skip API cache")
    parser.add_argument("--reuse-missing", action="store_true",
                        help="If a repo's language call returns nothing, reuse its "
                             "data from the previous run instead of dropping it")
    parser.add_argument("--svg", action="store_true",
                        help="Fetch data AND generate SVG")
    parser.add_argument("--svg-only", action="store_true",
                        help="Generate SVG from latest DB data (no API calls)")
    parser.add_argument("--history", action="store_true",
                        help="Show recent run history")
    parser.add_argument("--save-json", help="Also save latest stats as JSON")

    args = parser.parse_args()

    # ── History mode ──
    if args.history:
        conn = init_db()
        runs = get_recent_runs(conn, args.username)
        conn.close()
        if not runs:
            print("No runs found.", file=sys.stderr)
            return
        print(f"\n📋 Recent runs for {args.username}:\n")
        print(f"{'ID':<6} {'Timestamp':<26} {'Repos':<7} {'⭐Stars':<8} {'🍴Forks':<8} {'📝Issues':<8} {'Langs':<6} {'Total Code'}")
        print("-" * 100)
        for r in runs:
            rid, ts, total, pub, priv, tb, lc, stars, forks, issues = r
            print(f"{rid:<6} {ts:<26} {total:<7} {stars or 0:<8,} {forks or 0:<8,} {issues or 0:<8,} {lc:<6} {format_bytes(tb or 0)}")
        return

    # ── SVG-only mode (no API) ──
    if args.svg_only:
        conn = init_db()
        langs, ts = get_latest_stats(conn, args.username)
        conn.close()
        if not langs:
            print("❌ No data in DB. Run with --token first.", file=sys.stderr)
            sys.exit(1)
        print(f"📅 Using stats from: {ts}", file=sys.stderr)
        generate_svg(langs, args.username, args.theme, args.output, args.top)
        if args.save_json:
            with open(args.save_json, "w") as f:
                json.dump({l: b for l, b in langs}, f, indent=2)
        return

    # ── Fetch mode (with or without SVG) ──
    token = args.token or os.environ.get("GITHUB_TOKEN")
    if not token:
        print("❌ GitHub token required.", file=sys.stderr)
        print("   Set GITHUB_TOKEN env var or use --token", file=sys.stderr)
        sys.exit(1)

    print(f"🔍 GitHub user:  {args.username}", file=sys.stderr)
    print(f"📁 DB:           {DB_PATH}", file=sys.stderr)
    print(f"🎨 Theme:        {args.theme}", file=sys.stderr)
    if args.svg:
        print(f"🖼️  SVG output:   {args.output}", file=sys.stderr)
    print("", file=sys.stderr)

    cache = load_cache()
    run_id, languages, total_bytes = collect_and_store(
        args.username, token, cache, args.no_cache, args.reuse_missing,
    )

    if args.svg:
        print("", file=sys.stderr)
        generate_svg(languages, args.username, args.theme, args.output, args.top)

    if args.save_json:
        with open(args.save_json, "w") as f:
            json.dump({l: b for l, b in languages}, f, indent=2)
        print(f"💾 JSON saved to: {args.save_json}", file=sys.stderr)


if __name__ == "__main__":
    main()
