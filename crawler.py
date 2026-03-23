"""
crawler.py — BFS web crawler with robots.txt support
"""
import time
import sqlite3
import hashlib
import logging
from collections import deque
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [crawler] %(message)s")
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "PythonSearchBot/1.0 (educational crawler)"}
REQUEST_TIMEOUT = 10
CRAWL_DELAY = 1.0  # seconds between requests to the same host


def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pages (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            url      TEXT UNIQUE,
            url_hash TEXT UNIQUE,
            title    TEXT,
            body     TEXT,
            html     TEXT,
            crawled_at REAL
        );
        CREATE TABLE IF NOT EXISTS crawl_queue (
            url      TEXT PRIMARY KEY,
            depth    INTEGER,
            added_at REAL
        );
        CREATE INDEX IF NOT EXISTS idx_pages_url ON pages(url);
    """)
    conn.commit()
    return conn


def url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def already_crawled(conn: sqlite3.Connection, url: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM pages WHERE url_hash = ?", (url_hash(url),)
    ).fetchone()
    return row is not None


def save_page(conn, url, title, body, html):
    conn.execute(
        """INSERT OR IGNORE INTO pages (url, url_hash, title, body, html, crawled_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (url, url_hash(url), title, body, html, time.time()),
    )
    conn.commit()


def get_robot_parser(base_url: str) -> RobotFileParser:
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
    except Exception:
        pass
    return rp


def is_same_domain(base_url: str, url: str) -> bool:
    return urlparse(base_url).netloc == urlparse(url).netloc


def fetch_page(url: str) -> requests.Response | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
            return resp
    except Exception as e:
        log.warning(f"Failed to fetch {url}: {e}")
    return None


def extract_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        full = urljoin(base_url, href)
        parsed = urlparse(full)
        if parsed.scheme in ("http", "https") and not parsed.fragment:
            links.append(full.split("?")[0])  # strip query strings
    return list(set(links))


def extract_text(html: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string.strip() if soup.title else ""
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()
    body = " ".join(soup.get_text(separator=" ").split())
    return title, body


def crawl(seed_url: str, db_path: str = "search.db", max_pages: int = 50, same_domain: bool = True):
    """
    BFS crawl starting from seed_url.
    Stores pages in SQLite at db_path.
    """
    conn = init_db(db_path)
    rp = get_robot_parser(seed_url)
    queue = deque([(seed_url, 0)])
    visited = set()
    host_last_fetch: dict[str, float] = {}
    crawled = 0

    log.info(f"Starting crawl from {seed_url} (max={max_pages})")

    while queue and crawled < max_pages:
        url, depth = queue.popleft()

        if url in visited:
            continue
        visited.add(url)

        if same_domain and not is_same_domain(seed_url, url):
            continue

        if not rp.can_fetch(HEADERS["User-Agent"], url):
            log.info(f"Blocked by robots.txt: {url}")
            continue

        # Polite delay per host
        host = urlparse(url).netloc
        elapsed = time.time() - host_last_fetch.get(host, 0)
        if elapsed < CRAWL_DELAY:
            time.sleep(CRAWL_DELAY - elapsed)

        if already_crawled(conn, url):
            log.info(f"Already in DB, skipping: {url}")
            continue

        log.info(f"[{crawled+1}/{max_pages}] Crawling (depth={depth}): {url}")
        resp = fetch_page(url)
        host_last_fetch[host] = time.time()

        if resp is None:
            continue

        title, body = extract_text(resp.text)
        save_page(conn, url, title, body, resp.text)
        crawled += 1

        # Enqueue outbound links
        for link in extract_links(resp.text, url):
            if link not in visited:
                queue.append((link, depth + 1))

    log.info(f"Crawl complete. {crawled} pages stored in {db_path}")
    conn.close()
    return crawled


if __name__ == "__main__":
    import sys
    seed = sys.argv[1] if len(sys.argv) > 1 else "https://example.com"
    max_p = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    crawl(seed, max_pages=max_p)
