#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gc
import json
import re
import sys
import textwrap
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

BASE_LIST_URL = "https://prohardver.hu/temak/notebook/listaz.php"

HSZ_URL_RE = re.compile(
    r"^(?P<prefix>https?://[^#]+?/hsz_)(?P<start>\d+)-(?P<end>\d+)(?P<suffix>\.html)(?:#msg(?P<msg>\d+))?$",
    re.IGNORECASE,
)

URL_FIELD_RE = re.compile(r'"url"\s*:\s*"([^"]+)"')
NEXT_URL_FIELD_RE = re.compile(r'"next_resume_url"\s*:\s*(?:"([^"]+)"|null)')


# -----------------------------
# Általános segédfüggvények
# -----------------------------

def build_list_url(offset: int) -> str:
    return BASE_LIST_URL if offset <= 0 else f"{BASE_LIST_URL}?offset={offset}"


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def sanitize_filename(name: str, max_len: int = 180) -> str:
    name = clean_text(name)
    if not name:
        return "ismeretlen_topic"

    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))

    for src, dst in [
        ("/", "-"),
        ("\\", "-"),
        (":", " -"),
        ("*", ""),
        ("?", ""),
        ('"', ""),
        ("<", "("),
        (">", ")"),
        ("|", "-"),
    ]:
        name = name.replace(src, dst)

    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"[. ]+$", "", name)

    if len(name) > max_len:
        name = name[:max_len].rstrip(" .")

    return name or "ismeretlen_topic"


def split_name_like_person(name: str) -> Dict[str, str]:
    name = clean_text(name)
    if not name:
        return {"name": ""}

    parts = name.split()
    if len(parts) >= 2:
        return {"family": parts[0], "given": " ".join(parts[1:])}
    return {"name": name}


def now_local_iso() -> str:
    return datetime.now().astimezone().isoformat()


def strip_fragment(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ""))


def extract_query_param(url: str, key: str) -> Optional[str]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    vals = query.get(key)
    return vals[0] if vals else None


def set_query_param(url: str, key: str, value: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query[key] = [value]
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(query, doseq=True),
            "",
        )
    )


# -----------------------------
# Playwright wrapper
# -----------------------------

class BrowserFetcher:
    def __init__(
        self,
        headless: bool = True,
        slow_mo: int = 0,
        timeout_ms: int = 90000,
        retries: int = 4,
        block_resources: bool = True,
        auto_reset_fetches: int = 120,
    ):
        self.headless = headless
        self.slow_mo = slow_mo
        self.timeout_ms = timeout_ms
        self.retries = retries
        self.block_resources = block_resources
        self.auto_reset_fetches = auto_reset_fetches

        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.fetch_counter = 0

    def __enter__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            slow_mo=self.slow_mo,
            args=[
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        self._create_context_and_page()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.page:
                self.page.close()
        except Exception:
            pass
        try:
            if self.context:
                self.context.close()
        except Exception:
            pass
        try:
            if self.browser:
                self.browser.close()
        except Exception:
            pass
        try:
            if self.playwright:
                self.playwright.stop()
        except Exception:
            pass

    def _create_context_and_page(self) -> None:
        self.context = self.browser.new_context(
            locale="hu-HU",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1600, "height": 2200},
        )

        if self.block_resources:
            def route_handler(route):
                try:
                    req = route.request
                    if req.resource_type in {"image", "media", "font"}:
                        route.abort()
                    else:
                        route.continue_()
                except Exception:
                    try:
                        route.continue_()
                    except Exception:
                        pass

            self.context.route("**/*", route_handler)

        self.page = self.context.new_page()
        self.page.set_default_timeout(self.timeout_ms)
        self.page.set_default_navigation_timeout(self.timeout_ms)

    def reset_page(self) -> None:
        try:
            if self.page:
                self.page.close()
        except Exception:
            pass

        self.page = self.context.new_page()
        self.page.set_default_timeout(self.timeout_ms)
        self.page.set_default_navigation_timeout(self.timeout_ms)
        print("[INFO] Böngészőoldal újranyitva a stabilabb működéshez.")

    def reset_context(self) -> None:
        try:
            if self.page:
                self.page.close()
        except Exception:
            pass
        self.page = None

        try:
            if self.context:
                self.context.close()
        except Exception:
            pass
        self.context = None

        self._create_context_and_page()
        gc.collect()
        print("[INFO] Browser context teljesen újranyitva memória-kíméléshez.")

    def fetch(self, url: str, wait_ms: int = 1500) -> Tuple[str, str]:
        last_exc = None

        if self.auto_reset_fetches > 0 and self.fetch_counter > 0 and self.fetch_counter % self.auto_reset_fetches == 0:
            print("[INFO] Automatikus context-reset a fetch számláló alapján.")
            self.reset_context()

        for attempt in range(1, self.retries + 1):
            try:
                print(f"[DEBUG] LETÖLTVE ({attempt}/{self.retries}): {url}")
                self.page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                self.page.wait_for_timeout(wait_ms)

                try:
                    self.page.wait_for_load_state("networkidle", timeout=5000)
                except PlaywrightTimeoutError:
                    pass

                final_url = self.page.url
                html = self.page.content()
                self.fetch_counter += 1
                return final_url, html

            except PlaywrightTimeoutError as e:
                last_exc = e
                print(f"[WARN] Timeout ({attempt}/{self.retries}) -> {url}")
            except Exception as e:
                last_exc = e
                print(f"[WARN] Fetch hiba ({attempt}/{self.retries}) -> {url} | {e}")

            if attempt < self.retries:
                backoff_ms = 3000 * attempt
                print(f"[WARN] Újrapróbálás {backoff_ms / 1000:.1f} mp múlva...")

                try:
                    self.page.wait_for_timeout(backoff_ms)
                except Exception:
                    pass

                try:
                    self.page.goto("about:blank", timeout=10000)
                except Exception:
                    pass

                try:
                    self.reset_page()
                except Exception:
                    pass

        raise last_exc


# -----------------------------
# DOM / parse segédek
# -----------------------------

def parse_topic_links(html: str, page_url: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        "div.thread-list h4 a[href*='/tema/']",
        "div.col.thread-title-thread h4 a[href*='/tema/']",
        "main h4 a[href*='/tema/']",
        "h4 a[href*='/tema/']",
        "a[href*='/tema/']",
    ]

    anchors = []
    for selector in selectors:
        anchors = soup.select(selector)
        if anchors:
            break

    topics: List[Tuple[str, str]] = []
    seen: Set[str] = set()

    for a in anchors:
        href = a.get("href")
        if not href:
            continue

        full_url = urljoin(page_url, href)
        if "/tema/" not in full_url or "/temak/" in full_url:
            continue

        title = clean_text(a.get_text(" ", strip=True))
        if not title:
            continue

        norm = normalize_topic_base_url(full_url)
        if norm in seen:
            continue

        seen.add(norm)
        topics.append((title, norm))

    return topics[:100]


def extract_topic_title(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for selector in ["meta[property='og:title']", "title", "h1"]:
        node = soup.select_one(selector)
        if not node:
            continue

        if selector.startswith("meta"):
            text = clean_text(node.get("content", ""))
        else:
            text = clean_text(node.get_text(" ", strip=True))

        text = re.sub(r"\s*-\s*PROHARDVER!.*$", "", text, flags=re.I)
        if text:
            return text

    return fallback


def page_has_messages_html(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    return bool(soup.select("li.media[data-id]"))


def is_404_html(html: str) -> bool:
    text = clean_text(BeautifulSoup(html, "html.parser").get_text(" ", strip=True)).lower()
    return "404" in text or "a kért oldal nem létezik" in text


def extract_author(post: Tag) -> str:
    header = post.select_one(".msg-header")
    if header:
        header_text = clean_text(header.get_text(" ", strip=True))
        m = re.match(r"#\d+\s+(.+?)\s*>\s*.+?#\d+", header_text)
        if m:
            author = clean_text(m.group(1))
            if author:
                return author

    for selector in [".msg-user", ".media-left"]:
        node = post.select_one(selector)
        if node:
            txt = clean_text(node.get_text("\n", strip=True))
            lines = [line.strip() for line in txt.splitlines() if line.strip()]
            if lines:
                return lines[0]

    return "ismeretlen"


def extract_comment_date(post: Tag) -> Optional[str]:
    header = post.select_one(".msg-header")
    if header:
        header_text = clean_text(header.get_text(" ", strip=True))
        patterns = [
            r"\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b",
            r"\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}\b",
            r"\b\d{4}\.\d{2}\.\d{2}\.?(?: \d{2}:\d{2}(?::\d{2})?)?\b",
            r"\bma,? \d{1,2}:\d{2}\b",
            r"\btegnap,? \d{1,2}:\d{2}\b",
        ]
        for pattern in patterns:
            m = re.search(pattern, header_text, flags=re.I)
            if m:
                return clean_text(m.group(0))

    for selector in ["time", ".msg-date", ".date"]:
        node = post.select_one(selector)
        if not node:
            continue
        text = clean_text(node.get_text(" ", strip=True) or node.get("datetime", ""))
        if text:
            return text

    return None


def extract_comment_likes(post: Tag) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    text = clean_text(post.get_text(" ", strip=True))

    likes = None
    dislikes = None

    patterns_like = [
        r"\bLike(?:ok)?\s*[:\-]?\s*(\d+)\b",
        r"\bTetszik\s*[:\-]?\s*(\d+)\b",
    ]
    patterns_dislike = [
        r"\bDislike(?:ok)?\s*[:\-]?\s*(\d+)\b",
        r"\bNem tetszik\s*[:\-]?\s*(\d+)\b",
    ]

    for pattern in patterns_like:
        m = re.search(pattern, text, flags=re.I)
        if m:
            likes = int(m.group(1))
            break

    for pattern in patterns_dislike:
        m = re.search(pattern, text, flags=re.I)
        if m:
            dislikes = int(m.group(1))
            break

    score = None
    if likes is not None or dislikes is not None:
        score = (likes or 0) - (dislikes or 0)

    return likes, dislikes, score


def extract_comment_text(post: Tag) -> str:
    for selector in [".msg-content p.mgt0", ".msg-content", "p.mgt0"]:
        nodes = post.select(selector)
        if not nodes:
            continue

        parts = []
        for node in nodes:
            text = clean_text(node.get_text("\n", strip=True))
            if text:
                parts.append(text)

        if parts:
            joined = "\n".join(parts)
            joined = re.sub(r"\n{3,}", "\n\n", joined).strip()
            if joined:
                return joined

    return ""


def comment_url_from_page(current_url: str, post_id: str) -> str:
    base = strip_fragment(current_url)
    if post_id:
        return f"{base}#msg{post_id}"
    return base


def parse_comments_from_html(html: str, current_url: str, next_resume_url: Optional[str]) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    posts = soup.select("li.media[data-id]")
    results: List[Dict] = []

    print(f"[DEBUG] Talált li.media[data-id] elemek száma: {len(posts)}")

    for index, post in enumerate(posts, start=1):
        post_id = clean_text(post.get("data-id", ""))
        author = extract_author(post)
        comment = extract_comment_text(post)
        date_text = extract_comment_date(post)
        likes, dislikes, score = extract_comment_likes(post)

        preview = comment[:120].replace("\n", " | ") if comment else "<üres>"
        print(
            f"[DEBUG] Poszt #{index} | data-id={post_id or '-'} | szerző={author} | "
            f"dátum={date_text or '-'} | like={likes} | preview={preview}"
        )

        if not comment:
            continue

        results.append(
            {
                "comment_id": post_id or None,
                "author": author,
                "date": date_text,
                "likes": likes,
                "dislikes": dislikes,
                "score": score,
                "url": comment_url_from_page(current_url, post_id),
                "page_url": strip_fragment(current_url),
                "next_resume_url": next_resume_url,
                "data": comment,
            }
        )

    print(f"[DEBUG] Kinyert kommentek ezen az oldalon: {len(results)}")
    return results


def parse_hsz_range_from_url(url: str) -> Optional[Tuple[int, int]]:
    m = HSZ_URL_RE.match(url)
    if not m:
        return None
    return int(m.group("start")), int(m.group("end"))


def build_hsz_url_with_range(current_url: str, start: int, end: int) -> Optional[str]:
    m = HSZ_URL_RE.match(strip_fragment(current_url))
    if not m:
        return None
    prefix = m.group("prefix")
    suffix = m.group("suffix")
    return f"{prefix}{start}-{end}{suffix}#msg{end + 1}"


def normalize_topic_base_url(topic_url: str) -> str:
    base = strip_fragment(topic_url).rstrip("/")
    base = re.sub(r"/friss\.html$", "", base, flags=re.I)
    base = re.sub(r"/hsz_\d+-\d+\.html$", "", base, flags=re.I)
    return base


def build_fresh_url_from_topic_url(topic_url: str) -> str:
    return f"{normalize_topic_base_url(topic_url)}/friss.html"


def build_hsz_url_from_topic_url(topic_url: str, start: int, end: int) -> str:
    base = normalize_topic_base_url(topic_url)
    return f"{base}/hsz_{start}-{end}.html#msg{end + 1}"


def build_prev_range_from_saved(saved_start: int, saved_end: int) -> Optional[Tuple[int, int]]:
    new_start = saved_start - 100
    new_end = saved_end - 100
    if new_start < 1 or new_end < 1:
        return None
    return new_start, new_end


def build_fallback_next_hsz_url(current_url: str) -> Optional[str]:
    parsed = parse_hsz_range_from_url(strip_fragment(current_url))
    if not parsed:
        return None

    start, end = parsed
    new_start = start - 100
    new_end = end - 100
    if new_start < 1 or new_end < 1:
        return None

    return build_hsz_url_with_range(current_url, new_start, new_end)


def get_next_page_href_from_html(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        "a[rel='next']",
        "li.nav-arrow a[rel='next']",
        "a[title*='Következő blokk']",
        "a[href*='/hsz_']",
    ]

    for selector in selectors:
        for a in soup.select(selector):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(current_url, href)
            if "/hsz_" in full:
                return strip_fragment(full)

    fallback_url = build_fallback_next_hsz_url(current_url)
    return strip_fragment(fallback_url) if fallback_url else None


# -----------------------------
# Fájl / output kezelés
# -----------------------------

def ensure_output_dirs(base_output: Path) -> Tuple[Path, Path, Path]:
    prohardver_dir = base_output / "prohardver"
    notebooks_dir = prohardver_dir / "notebooks"
    visited_file = prohardver_dir / "visited_notebook.txt"

    prohardver_dir.mkdir(parents=True, exist_ok=True)
    notebooks_dir.mkdir(parents=True, exist_ok=True)

    if not visited_file.exists():
        visited_file.write_text("", encoding="utf-8")

    return prohardver_dir, notebooks_dir, visited_file


def load_visited(visited_file: Path) -> Set[str]:
    if not visited_file.exists():
        return set()
    return {
        line.strip()
        for line in visited_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def append_visited(visited_file: Path, topic_url: str) -> None:
    with visited_file.open("a", encoding="utf-8") as f:
        f.write(topic_url.strip() + "\n")


def normalize_topic_url_for_visited(topic_url: str) -> str:
    return normalize_topic_base_url(topic_url)


def topic_file_path(notebooks_dir: Path, title: str) -> Path:
    return notebooks_dir / f"{sanitize_filename(title)}.json"


def read_tail_text(path: Path, max_bytes: int = 1024 * 1024) -> str:
    if not path.exists():
        return ""

    size = path.stat().st_size
    with path.open("rb") as f:
        if size > max_bytes:
            f.seek(size - max_bytes)
        data = f.read()

    return data.decode("utf-8", errors="ignore")


def file_looks_closed_json(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False

    tail = read_tail_text(path, max_bytes=65536).rstrip()
    if not tail.endswith("}"):
        return False

    required_markers = [
        '"comments": [',
        '"origin": "prohardver_forum"',
        '"scrape_status": "finished"',
    ]
    full_sample = read_tail_text(path, max_bytes=512 * 1024)
    return all(marker in tail or marker in full_sample for marker in required_markers)


def find_last_comment_url_from_file(path: Path) -> Optional[str]:
    tail = read_tail_text(path, max_bytes=2 * 1024 * 1024)
    matches = URL_FIELD_RE.findall(tail)
    if not matches:
        return None

    for url in reversed(matches):
        if "#msg" in url and ("/hsz_" in url or "/friss.html" in url):
            return url

    for url in reversed(matches):
        if "/hsz_" in url or "/friss.html" in url:
            return url

    return None


def find_last_next_resume_url_from_file(path: Path) -> Optional[str]:
    tail = read_tail_text(path, max_bytes=2 * 1024 * 1024)
    matches = NEXT_URL_FIELD_RE.findall(tail)
    if not matches:
        return None

    for value in reversed(matches):
        cleaned = clean_text(value)
        if cleaned and cleaned.lower() != "null":
            return cleaned
    return None


def init_open_json_file_if_needed(topic_file: Path, resolved_title: str, topic_url: str) -> None:
    if topic_file.exists() and topic_file.stat().st_size > 0:
        return

    header_obj = {
        "title": resolved_title,
        "authors": [],
        "data": {
            "content": resolved_title,
            "likes": None,
            "dislikes": None,
            "score": None,
            "date": None,
            "url": topic_url,
            "language": "hu",
            "tags": [],
            "rights": "PROHARDVER! fórum tartalom",
            "extra": {},
            "origin": "prohardver_forum",
        },
    }

    with topic_file.open("w", encoding="utf-8") as f:
        f.write("{\n")
        f.write(f'  "title": {json.dumps(header_obj["title"], ensure_ascii=False)},\n')
        f.write(f'  "authors": {json.dumps(header_obj["authors"], ensure_ascii=False, indent=2)},\n')
        f.write(f'  "data": {json.dumps(header_obj["data"], ensure_ascii=False, indent=2)},\n')
        f.write('  "comments": [\n')


def append_comments_page_to_open_json(topic_file: Path, comments: List[Dict], first_comment_already_written: bool) -> bool:
    if not comments:
        return first_comment_already_written

    with topic_file.open("a", encoding="utf-8") as f:
        for comment in comments:
            json_comment = {
                "authors": [split_name_like_person(comment.get("author") or "ismeretlen")],
                "data": comment.get("data"),
                "likes": comment.get("likes"),
                "dislikes": comment.get("dislikes"),
                "score": comment.get("score"),
                "date": comment.get("date"),
                "url": comment.get("url"),
                "language": "hu",
                "tags": [],
                "extra": {
                    "comment_id": comment.get("comment_id"),
                    "page_url": comment.get("page_url"),
                    "next_resume_url": comment.get("next_resume_url"),
                },
            }

            if first_comment_already_written:
                f.write(",\n")
            f.write(textwrap.indent(json.dumps(json_comment, ensure_ascii=False, indent=4), "    "))
            first_comment_already_written = True

    return first_comment_already_written


def close_topic_json_file(topic_file: Path, saved_comment_pages: int, resume_source: Optional[str]) -> None:
    with topic_file.open("a", encoding="utf-8") as f:
        f.write("\n  ],\n")
        f.write('  "origin": "prohardver_forum",\n')
        f.write('  "extra": {\n')
        f.write('    "scrape_status": "finished",\n')
        f.write(f'    "saved_comment_pages": {saved_comment_pages},\n')
        f.write(f'    "resume_source": {json.dumps(resume_source, ensure_ascii=False)},\n')
        f.write(f'    "date_modified": {json.dumps(now_local_iso(), ensure_ascii=False)}\n')
        f.write("  }\n")
        f.write("}\n")


def derive_next_page_from_comment_url(comment_url: str) -> Optional[str]:
    if not comment_url:
        return None

    base_url = strip_fragment(comment_url)
    parsed = parse_hsz_range_from_url(base_url)
    if not parsed:
        return None

    start, end = parsed
    prev_range = build_prev_range_from_saved(start, end)
    if not prev_range:
        return None

    new_start, new_end = prev_range
    return build_hsz_url_from_topic_url(base_url, new_start, new_end)


def resolve_resume_url(topic_url: str, topic_file: Path) -> Tuple[str, Optional[str], bool]:
    if topic_file.exists() and topic_file.stat().st_size > 0:
        if file_looks_closed_json(topic_file):
            return build_fresh_url_from_topic_url(topic_url), "already_closed", True

        next_resume_url = find_last_next_resume_url_from_file(topic_file)
        if next_resume_url:
            print(f"[INFO] Resume: next_resume_url alapján innen folytatva: {next_resume_url}")
            return next_resume_url, "existing_json_next_resume_url", False

        last_comment_url = find_last_comment_url_from_file(topic_file)
        if last_comment_url:
            derived = derive_next_page_from_comment_url(last_comment_url)
            if derived:
                print(f"[INFO] Resume: utolsó komment URL alapján innen folytatva: {derived}")
                return derived, "existing_json_last_comment_url", False

        print("[INFO] Van meglévő félkész fájl, de nem találtam benne használható resume pontot. Friss oldalról indul.")

    return build_fresh_url_from_topic_url(topic_url), None, False


def open_topic_start_page(fetcher: BrowserFetcher, topic_url: str, topic_file: Path, delay: float) -> Tuple[str, str, Optional[str], bool]:
    start_url, resume_source, already_closed = resolve_resume_url(topic_url, topic_file)
    if already_closed:
        return start_url, "", resume_source, True

    fresh_url = build_fresh_url_from_topic_url(topic_url)

    print(f"[DEBUG] Topic megnyitása: {start_url}")
    current_url, html = fetcher.fetch(start_url, wait_ms=int(delay * 1000))

    if is_404_html(html) or not page_has_messages_html(html):
        if start_url != fresh_url:
            print(f"[DEBUG] A resume URL nem adott használható kommentoldalt, fallback friss.html-re: {fresh_url}")
            current_url, html = fetcher.fetch(fresh_url, wait_ms=int(delay * 1000))
            resume_source = "fallback_to_fresh"

    if not page_has_messages_html(html):
        raise RuntimeError("Nem található kommentoldal a topichoz.")

    return current_url, html, resume_source, False


def scrape_topic_sequentially(fetcher: BrowserFetcher, topic_title: str, topic_url: str, topic_file: Path, delay: float, topic_reset_interval: int = 25) -> Tuple[str, bool]:
    opened_url, html, resume_source, already_closed = open_topic_start_page(fetcher, topic_url, topic_file, delay)
    if already_closed:
        print("[INFO] A topic fájl már lezárt JSON, kihagyva.")
        return topic_title, True

    print(f"[DEBUG] Ténylegesen megnyitott kezdőoldal: {opened_url}")

    resolved_title = extract_topic_title(html, topic_title)
    init_open_json_file_if_needed(topic_file, resolved_title, topic_url)

    first_comment_already_written = False
    if topic_file.exists() and topic_file.stat().st_size > 0:
        last_comment_url = find_last_comment_url_from_file(topic_file)
        if last_comment_url:
            first_comment_already_written = True

    visited_urls: Set[str] = set()
    page_index = 1
    current_url = opened_url
    current_html = html

    while True:
        current_url_base = strip_fragment(current_url)
        if current_url_base in visited_urls:
            print(f"[DEBUG] Már feldolgozott oldal, leállás: {current_url}")
            return resolved_title, False

        visited_urls.add(current_url_base)
        next_resume_url = get_next_page_href_from_html(current_html, current_url)

        print(f"[DEBUG] Kommentoldal #{page_index}: {current_url}")
        page_comments = parse_comments_from_html(current_html, current_url, next_resume_url)

        if page_comments:
            first_comment_already_written = append_comments_page_to_open_json(
                topic_file=topic_file,
                comments=page_comments,
                first_comment_already_written=first_comment_already_written,
            )

        print(
            f"[INFO] Oldal appendelve a JSON végére: {topic_file} | "
            f"oldal kommentjei: {len(page_comments)}"
        )

        if not next_resume_url:
            print("[DEBUG] Nincs több oldal, topic véglegesítése.")
            close_topic_json_file(topic_file=topic_file, saved_comment_pages=page_index, resume_source=resume_source)
            really_closed = file_looks_closed_json(topic_file)
            print(f"[INFO] Topic végleg lezárva: {topic_file} | lezárt={really_closed}")
            return resolved_title, really_closed

        print(f"[DEBUG] Következő kommentoldal: {next_resume_url}")

        if topic_reset_interval > 0 and page_index % topic_reset_interval == 0:
            print("[INFO] Hosszú topic közbeni memória-kímélő context reset.")
            fetcher.reset_context()

        try:
            next_url, next_html = fetcher.fetch(next_resume_url, wait_ms=int(delay * 1000))
        except Exception as e:
            print(f"[DEBUG] Timeout vagy navigációs hiba történt: {e}")
            return resolved_title, False

        if not page_has_messages_html(next_html):
            print("[DEBUG] A következő oldal már nem tartalmaz kommenteket, topic véglegesítése.")
            close_topic_json_file(topic_file=topic_file, saved_comment_pages=page_index, resume_source=resume_source)
            really_closed = file_looks_closed_json(topic_file)
            print(f"[INFO] Topic végleg lezárva: {topic_file} | lezárt={really_closed}")
            return resolved_title, really_closed

        current_url, current_html = next_url, next_html
        page_index += 1
        gc.collect()


def scrape_offsets(start_offset: int, end_offset: int, output_dir: str, delay: float, headless: bool, timeout_ms: int, retries: int, auto_reset_fetches: int, topic_reset_interval: int) -> None:
    base_output = Path(output_dir).expanduser().resolve()
    _, notebooks_dir, visited_file = ensure_output_dirs(base_output)

    visited_topics = load_visited(visited_file)
    visited_topics = {normalize_topic_url_for_visited(x) for x in visited_topics}

    with BrowserFetcher(
        headless=headless,
        slow_mo=0,
        timeout_ms=timeout_ms,
        retries=retries,
        block_resources=True,
        auto_reset_fetches=auto_reset_fetches,
    ) as fetcher:
        for offset in range(start_offset, end_offset + 1, 100):
            list_url = build_list_url(offset)
            print(f"\n[INFO] Listaoldal megnyitása: {list_url}")

            try:
                fetcher.reset_context()
                final_list_url, list_html = fetcher.fetch(list_url, wait_ms=int(delay * 1000))
            except Exception as e:
                print(f"[WARN] Hiba a listaoldalnál: {list_url} | {e}")
                continue

            topics = parse_topic_links(list_html, final_list_url)
            print(f"[INFO] Talált topicok száma: {len(topics)}")
            if not topics:
                continue

            for idx, (topic_title, topic_url) in enumerate(topics, start=1):
                topic_url_norm = normalize_topic_url_for_visited(topic_url)
                if topic_url_norm in visited_topics:
                    print(f"[INFO] ({idx}/{len(topics)}) Már feldolgozva, kihagyva: {topic_title}")
                    continue

                topic_file = topic_file_path(notebooks_dir, topic_title)
                print(f"\n[INFO] ({idx}/{len(topics)}) Topic: {topic_title}")

                try:
                    resolved_title, finished = scrape_topic_sequentially(
                        fetcher=fetcher,
                        topic_title=topic_title,
                        topic_url=topic_url,
                        topic_file=topic_file,
                        delay=delay,
                        topic_reset_interval=topic_reset_interval,
                    )

                    if sanitize_filename(resolved_title) != sanitize_filename(topic_title):
                        new_path = topic_file_path(notebooks_dir, resolved_title)
                        if new_path != topic_file and topic_file.exists():
                            topic_file.replace(new_path)
                            topic_file = new_path

                    print(f"[INFO] Topic fájl: {topic_file}")

                    if finished and file_looks_closed_json(topic_file):
                        append_visited(visited_file, topic_url_norm)
                        visited_topics.add(topic_url_norm)
                        print(f"[INFO] Topic teljesen feldolgozva, visitedbe írva: {resolved_title}")
                    else:
                        print(f"[INFO] Topic nincs kész vagy hibával megállt, NEM kerül visitedbe: {resolved_title}")

                except Exception as e:
                    print(f"[WARN] Váratlan hiba a topicnál: {topic_url} | {e}")

                gc.collect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PROHARDVER notebook topic scraper Playwrighttel, appendelt JSON mentéssel."
    )
    parser.add_argument("start_offset", type=int, help="Kezdő offset. Pl. 0 vagy 100")
    parser.add_argument("end_offset", type=int, help="Vég offset. Pl. 200 vagy 300")
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa. Ide jön létre a prohardver mappa. Alapértelmezett: aktuális mappa.",
    )
    parser.add_argument("--delay", type=float, default=1.2, help="Várakozás oldalak között másodpercben.")
    parser.add_argument("--headed", action="store_true", help="Látható böngészőablakkal fusson.")
    parser.add_argument("--timeout-ms", type=int, default=90000, help="Navigációs timeout ezredmásodpercben.")
    parser.add_argument("--retries", type=int, default=4, help="Ennyiszer próbálja újra a fetch műveleteket.")
    parser.add_argument("--topic-reset-interval", type=int, default=25, help="Ennyi kommentoldalanként teljes context reset hosszú topicoknál.")
    parser.add_argument("--auto-reset-fetches", type=int, default=120, help="Ennyi fetch után automatikus context reset.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.start_offset < 0 or args.end_offset < 0:
        print("A start_offset és end_offset nem lehet negatív.")
        sys.exit(1)

    if args.start_offset > args.end_offset:
        print("A start_offset nem lehet nagyobb, mint az end_offset.")
        sys.exit(1)

    if args.start_offset % 100 != 0 or args.end_offset % 100 != 0:
        print("Az offsetek legyenek 100-zal oszthatók: 0, 100, 200, ...")
        sys.exit(1)

    try:
        scrape_offsets(
            start_offset=args.start_offset,
            end_offset=args.end_offset,
            output_dir=args.output,
            delay=args.delay,
            headless=not args.headed,
            timeout_ms=args.timeout_ms,
            retries=args.retries,
            auto_reset_fetches=args.auto_reset_fetches,
            topic_reset_interval=args.topic_reset_interval,
        )
    except KeyboardInterrupt:
        print("\n[INFO] Megszakítva felhasználó által.")
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL ERROR] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
#python prohardver_server.py 0 6000 --output . --delay 1.2