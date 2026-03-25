#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import re
import sys
import textwrap
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


BASE_URL = "https://port.hu"
MAIN_FORUM_URL = "https://port.hu/forum"

TOPIC_PAGE_RE = re.compile(r"^/forum/[^/?#]+/\d+(?:\?.*)?$", re.IGNORECASE)

COMMENT_ID_RE = re.compile(r'"comment_id"\s*:\s*"([^"]+)"')
COMMENT_URL_RE = re.compile(r'"url"\s*:\s*"([^"]+)"')


# -----------------------------
# Általános segédfüggvények
# -----------------------------

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
        return "ismeretlen"

    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))

    replacements = [
        ("/", "-"),
        ("\\", "-"),
        (":", " -"),
        ("*", ""),
        ("?", ""),
        ('"', ""),
        ("<", "("),
        (">", ")"),
        ("|", "-"),
    ]
    for src, dst in replacements:
        name = name.replace(src, dst)

    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"[. ]+$", "", name)

    if len(name) > max_len:
        name = name[:max_len].rstrip(" .")

    return name or "ismeretlen"


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


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


def parse_int_from_text(text: str) -> Optional[int]:
    text = clean_text(text)
    if not text:
        return None
    normalized = text.replace(".", "").replace(" ", "")
    m = re.search(r"-?\d+", normalized)
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


def split_name_like_person(name: str) -> Dict[str, str]:
    name = clean_text(name)
    if not name:
        return {"name": ""}

    parts = name.split()
    if len(parts) >= 2:
        return {"family": parts[0], "given": " ".join(parts[1:])}
    return {"name": name}


def get_topic_base_url(url: str) -> str:
    parsed = urlparse(strip_fragment(url))
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", "", ""))


def get_topic_page_number(url: str) -> int:
    page_val = extract_query_param(url, "page")
    if page_val and page_val.isdigit():
        return int(page_val)
    return 1


def parse_comment_page_number_from_comment_url(url: str) -> int:
    return get_topic_page_number(url)


# -----------------------------
# Fájl / output kezelés
# -----------------------------

def ensure_dirs(base_output: Path) -> Tuple[Path, Path, Path]:
    port_dir = base_output / "port"
    topics_dir = port_dir / "topics"
    port_dir.mkdir(parents=True, exist_ok=True)
    topics_dir.mkdir(parents=True, exist_ok=True)

    visited_file = port_dir / "visited.txt"
    if not visited_file.exists():
        visited_file.write_text("", encoding="utf-8")

    return port_dir, topics_dir, visited_file


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


def normalize_topic_url_for_visited(url: str) -> str:
    return get_topic_base_url(url)


def topic_file_path(topics_dir: Path, topic_title: str) -> Path:
    return topics_dir / f"{sanitize_filename(topic_title)}.json"


def is_stream_json_finalized(topic_file: Path) -> bool:
    if not topic_file.exists():
        return False

    try:
        with topic_file.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            read_size = min(size, 512)
            f.seek(max(0, size - read_size))
            tail = f.read().decode("utf-8", errors="ignore").strip()
        return tail.endswith("]\n}") or tail.endswith("]\r\n}") or tail.endswith("]}")
    except Exception:
        return False


def count_existing_comments_in_stream_file(topic_file: Path) -> int:
    if not topic_file.exists():
        return 0

    count = 0
    with topic_file.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            count += line.count('"comment_id":')
    return count


def get_last_written_comment_info(topic_file: Path) -> Tuple[Optional[str], Optional[str], int]:
    """
    Visszaadja:
      - utolsó comment_id
      - utolsó komment URL
      - meglévő kommentek száma
    """
    if not topic_file.exists():
        return None, None, 0

    existing_count = count_existing_comments_in_stream_file(topic_file)

    try:
        with topic_file.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            read_size = min(size, 1024 * 1024)
            f.seek(max(0, size - read_size))
            tail = f.read().decode("utf-8", errors="ignore")
    except Exception:
        return None, None, existing_count

    comment_ids = COMMENT_ID_RE.findall(tail)
    urls = COMMENT_URL_RE.findall(tail)

    last_comment_id = comment_ids[-1] if comment_ids else None
    last_comment_url = urls[-1] if urls else None

    return last_comment_id, last_comment_url, existing_count


def write_topic_stream_header(topic_file: Path, resolved_title: str, topic_meta: Dict, topic_url: str) -> None:
    header_obj = {
        "title": resolved_title,
        "authors": [],
        "data": {
            "content": resolved_title,
            "likes": None,
            "dislikes": None,
            "score": None,
            "rating": None,
            "date": None,
            "url": get_topic_base_url(topic_url),
            "language": "hu",
            "tags": [],
            "rights": "port.hu fórum tartalom",
            "date_modified": now_iso(),
            "extra": {
                "detected_total_comments": topic_meta.get("detected_total_comments"),
                "fetched_page": topic_meta.get("fetched_page"),
            },
            "origin": "port_forum",
        },
        "origin": "port_forum",
    }

    header_json = json.dumps(header_obj, ensure_ascii=False, indent=2)
    if not header_json.endswith("}"):
        raise RuntimeError("Hibás header JSON generálás.")

    text = header_json[:-1] + ',\n  "comments": [\n'
    topic_file.write_text(text, encoding="utf-8")


def append_comment_to_stream_file(topic_file: Path, comment_item: Dict, has_existing_comments: bool) -> None:
    item_json = json.dumps(comment_item, ensure_ascii=False, indent=2)
    item_json = textwrap.indent(item_json, "    ")

    with topic_file.open("a", encoding="utf-8") as f:
        if has_existing_comments:
            f.write(",\n")
        f.write(item_json)


def finalize_stream_json(topic_file: Path) -> None:
    if is_stream_json_finalized(topic_file):
        return
    with topic_file.open("a", encoding="utf-8") as f:
        f.write("\n  ]\n}\n")


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
            viewport={"width": 1440, "height": 2200},
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

    def accept_cookies_if_present(self) -> None:
        candidates = [
            "button:has-text('Elfogadom')",
            "button:has-text('ELFOGADOM')",
            "button:has-text('Rendben')",
            "button:has-text('OK')",
            "text=Elfogadom",
            "text=ELFOGADOM",
        ]
        for selector in candidates:
            try:
                locator = self.page.locator(selector).first
                if locator.is_visible(timeout=1200):
                    locator.click(timeout=2500)
                    self.page.wait_for_timeout(1200)
                    return
            except Exception:
                pass

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

                self.accept_cookies_if_present()

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
# Főoldali topiclista parsing
# -----------------------------

def parse_topic_rows_from_main_page(html: str, page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    topics: List[Dict] = []
    seen = set()

    table = soup.select_one("table.table.table-condensed")
    if not table:
        print("[DEBUG] Nem található a topiclista táblázat.")
        del soup
        gc.collect()
        return topics

    rows = table.select("tbody tr[data-key], tbody tr")
    print(f"[DEBUG] Főoldali topic sorok száma: {len(rows)}")

    for row in rows:
        title_a = None
        for a in row.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if TOPIC_PAGE_RE.search(href):
                title_a = a
                break

        if not title_a:
            continue

        topic_title = clean_text(title_a.get_text(" ", strip=True))
        if not topic_title:
            continue

        topic_url = urljoin(page_url, title_a.get("href", ""))
        topic_url_norm = normalize_topic_url_for_visited(topic_url)
        if topic_url_norm in seen:
            continue
        seen.add(topic_url_norm)

        cells = row.find_all("td")
        comment_count = None
        view_count = None
        last_message = None
        last_user = None

        if len(cells) >= 2:
            comment_count = parse_int_from_text(cells[1].get_text(" ", strip=True))
        if len(cells) >= 3:
            view_count = parse_int_from_text(cells[2].get_text(" ", strip=True))
        if len(cells) >= 4:
            cell_text = clean_text(cells[3].get_text(" ", strip=True))
            m = re.match(r"^(.*?\d{1,2}:\d{2})\s+(.+)$", cell_text)
            if m:
                last_message = clean_text(m.group(1))
                last_user = clean_text(m.group(2))
            else:
                last_message = cell_text

        topics.append(
            {
                "title": topic_title,
                "url": topic_url_norm,
                "comment_count": comment_count,
                "view_count": view_count,
                "last_message": last_message,
                "last_user": last_user,
            }
        )

    del rows
    del soup
    gc.collect()
    return topics


def get_main_next_page_url(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    for ul in soup.select("ul.pagination"):
        for li in ul.select("li.next a[href], li.last a[href]"):
            href = (li.get("href") or "").strip()
            if href:
                del soup
                gc.collect()
                return urljoin(current_url, href)

        for a in ul.select("a[href]"):
            txt = clean_text(a.get_text(" ", strip=True))
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if txt in {">", "›", "»"}:
                del soup
                gc.collect()
                return urljoin(current_url, href)

    current_page = extract_query_param(current_url, "page")
    current_page_no = int(current_page) if current_page and current_page.isdigit() else 1
    next_page_no = current_page_no + 1

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        full = urljoin(current_url, href)
        if not full.startswith(MAIN_FORUM_URL):
            continue
        page_val = extract_query_param(full, "page")
        if page_val and page_val.isdigit() and int(page_val) == next_page_no:
            del soup
            gc.collect()
            return full

    del soup
    gc.collect()
    return set_query_param(MAIN_FORUM_URL, "page", str(next_page_no))


# -----------------------------
# Topicoldal parsing
# -----------------------------

def extract_topic_title(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        "div.main-box h1 a",
        "div.main-box h1",
        "h1 a",
        "h1",
        "title",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            text = clean_text(node.get_text(" ", strip=True))
            text = re.sub(r"^\s*Téma:\s*", "", text, flags=re.I)
            if text:
                del soup
                gc.collect()
                return text

    del soup
    gc.collect()
    return fallback


def extract_topic_meta(html: str, topic_url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    page_text = clean_text(soup.get_text("\n", strip=True))

    page_count = get_topic_page_number(topic_url)

    total_comments = None
    m = re.search(r"\((\d+)\s*/\s*(\d+)\)", page_text)
    if m:
        total_comments = parse_int_from_text(m.group(2))

    del soup
    gc.collect()

    return {
        "url": get_topic_base_url(topic_url),
        "detected_total_comments": total_comments,
        "fetched_page": page_count,
    }


def find_comment_containers(soup: BeautifulSoup) -> List[Tag]:
    containers = soup.select("div.comment-container")
    if containers:
        return containers
    return soup.select("div.comment-container, div[class*='comment-container']")


def parse_comment_index(text: str) -> Tuple[Optional[int], Optional[int]]:
    text = clean_text(text)
    m = re.search(r"\((\d+)\s*/\s*(\d+)\)", text)
    if not m:
        return None, None
    return parse_int_from_text(m.group(1)), parse_int_from_text(m.group(2))


def extract_parent_author_from_header(header_row: Optional[Tag]) -> Optional[str]:
    if not header_row:
        return None

    candidates = header_row.select("span.reply-to, span.row.reply-to, a.reply-to, span[class*='reply']")
    for node in candidates:
        txt = clean_text(node.get_text(" ", strip=True))
        if not txt:
            continue
        txt = re.sub(r"^\s*Előzmény\s*", "", txt, flags=re.I)
        txt = clean_text(txt)
        if txt:
            return txt

    txt = clean_text(header_row.get_text(" ", strip=True))
    m = re.search(r"\bElőzmény\s+(.+?)$", txt, flags=re.I)
    if m:
        return clean_text(m.group(1))

    return None


def extract_comment_from_container(container: Tag, topic_page_url: str) -> Optional[Dict]:
    anchor = container.select_one("a[name]")
    comment_id = None
    if anchor and anchor.get("name"):
        m = re.search(r"comment-(\d+)", anchor.get("name", ""))
        if m:
            comment_id = m.group(1)

    header_row = container.select_one("div.row.header")

    author = None
    date_text = None
    rating = None
    parent_author = extract_parent_author_from_header(header_row)

    if header_row:
        author_node = header_row.select_one("span.name")
        if author_node:
            author = clean_text(author_node.get_text(" ", strip=True))

        date_node = header_row.select_one("span.date")
        if date_node:
            date_text = clean_text(date_node.get_text(" ", strip=True))

        rating_node = header_row.select_one("span.user-rating")
        if rating_node:
            rating_text = clean_text(rating_node.get_text(" ", strip=True))
            m_rating = re.search(r"(\d{1,2}/10)", rating_text)
            if m_rating:
                rating = m_rating.group(1)

        if not author or not date_text:
            header_text = clean_text(header_row.get_text(" ", strip=True))

            if not author:
                m_author = re.search(
                    r"^\s*(.*?)\s+(?:\d{4}\s+)?[A-Za-zÁÉÍÓÖŐÚÜŰáéíóöőúüű]+\.\s+\d{1,2}\.\s*-\s*\d{1,2}:\d{2}:\d{2}",
                    header_text
                )
                if m_author:
                    author = clean_text(m_author.group(1))

            if not date_text:
                m_date = re.search(
                    r"((?:\d{4}\s+)?[A-Za-zÁÉÍÓÖŐÚÜŰáéíóöőúüű]+\.\s+\d{1,2}\.\s*-\s*\d{1,2}:\d{2}:\d{2})",
                    header_text
                )
                if m_date:
                    date_text = clean_text(m_date.group(1))

            if not rating:
                m_rating = re.search(r"(\d{1,2}/10)", header_text)
                if m_rating:
                    rating = m_rating.group(1)

    if not author:
        author = "ismeretlen"

    message_node = container.select_one("div.message-text")
    if not message_node:
        message_node = container.select_one("div.row.message")
    body = clean_text(message_node.get_text("\n", strip=True)) if message_node else ""

    whole_text = clean_text(container.get_text("\n", strip=True))
    comment_no, total_no = parse_comment_index(whole_text)

    num_node = container.select_one("div.comment-num")
    if num_node:
        idx_a, idx_b = parse_comment_index(num_node.get_text(" ", strip=True))
        if idx_a is not None:
            comment_no = idx_a
        if idx_b is not None:
            total_no = idx_b

    is_offtopic = "offtopic" in " ".join(container.get("class", []))
    if not is_offtopic and re.search(r"\bofftopic\b", whole_text, flags=re.I):
        is_offtopic = True

    comment_url = strip_fragment(topic_page_url)
    if comment_id:
        comment_url = f"{comment_url}#comment-{comment_id}"

    return {
        "comment_id": comment_id,
        "author": author,
        "date": date_text,
        "rating": rating,
        "parent_author": parent_author,
        "index": comment_no,
        "index_total": total_no,
        "is_offtopic": is_offtopic,
        "url": comment_url,
        "data": body,
    }


def parse_comments_from_topic_page(html: str, topic_page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    containers = find_comment_containers(soup)

    print(f"[DEBUG] Talált komment-container elemek száma: {len(containers)}")

    comments: List[Dict] = []
    for idx, container in enumerate(containers, start=1):
        parsed = extract_comment_from_container(container, topic_page_url)
        if not parsed:
            continue

        preview = (parsed["data"] or "")[:100].replace("\n", " | ")
        print(
            f"[DEBUG] Komment #{idx} | id={parsed.get('comment_id') or '-'} "
            f"| szerző={parsed.get('author')} | dátum={parsed.get('date')} "
            f"| rating={parsed.get('rating')} | preview={preview}"
        )
        comments.append(parsed)

    del containers
    del soup
    gc.collect()
    return comments


def build_comment_signature(comment: Dict) -> str:
    comment_id = str(comment.get("comment_id") or "")
    author = clean_text(comment.get("author") or "")
    date = clean_text(comment.get("date") or "")
    text = clean_text(comment.get("data") or "")[:300]
    idx = str(comment.get("index") or "")
    return f"{comment_id}|{author}|{date}|{idx}|{text}"


def build_page_fingerprint(comments: List[Dict]) -> str:
    if not comments:
        return "EMPTY"
    sigs = [build_comment_signature(c) for c in comments]
    raw = "\n".join(sigs)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def topic_has_any_comment_container(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    has_any = bool(find_comment_containers(soup))
    del soup
    gc.collect()
    return has_any


def get_topic_next_page_url(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    current_page_no = get_topic_page_number(current_url)

    for ul in soup.select("ul.pagination"):
        for li in ul.select("li.next a[href], li.last a[href]"):
            href = (li.get("href") or "").strip()
            if href:
                full = urljoin(current_url, href)
                if get_topic_page_number(full) > current_page_no:
                    del soup
                    gc.collect()
                    return full

        for a in ul.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue

            full = urljoin(current_url, href)
            full_page_no = get_topic_page_number(full)
            if full_page_no == current_page_no + 1:
                del soup
                gc.collect()
                return full

            txt = clean_text(a.get_text(" ", strip=True))
            if txt in {">", "›", "»"} and full_page_no > current_page_no:
                del soup
                gc.collect()
                return full

    next_page_no = current_page_no + 1
    del soup
    gc.collect()
    return set_query_param(get_topic_base_url(current_url), "page", str(next_page_no))


# -----------------------------
# Streamelt komment JSON item
# -----------------------------

def comment_to_output_item(c: Dict) -> Dict:
    author_name = c.get("author") or "ismeretlen"
    return {
        "authors": [split_name_like_person(author_name)] if author_name else [],
        "data": c.get("data", ""),
        "likes": None,
        "dislikes": None,
        "score": None,
        "rating": c.get("rating"),
        "date": c.get("date"),
        "url": c.get("url"),
        "language": "hu",
        "tags": ["offtopic"] if c.get("is_offtopic") else [],
        "extra": {
            "comment_id": c.get("comment_id"),
            "parent_author": c.get("parent_author"),
            "index": c.get("index"),
            "index_total": c.get("index_total"),
            "is_offtopic": c.get("is_offtopic"),
        },
    }


# -----------------------------
# Topic scrape
# -----------------------------

def scrape_topic(
    fetcher: BrowserFetcher,
    topic_title: str,
    topic_url: str,
    topic_file: Path,
    delay: float,
    topic_reset_interval: int = 25,
) -> int:
    existing_comments = 0
    resume_page_no = 1
    resume_after_comment_id = None
    need_init_file = True

    if topic_file.exists():
        if is_stream_json_finalized(topic_file):
            print("[INFO] A topic fájl már lezárt JSON, ezt késznek vesszük.")
            return count_existing_comments_in_stream_file(topic_file)

        last_comment_id, last_comment_url, existing_comments = get_last_written_comment_info(topic_file)
        if last_comment_url:
            resume_page_no = parse_comment_page_number_from_comment_url(last_comment_url)
            resume_after_comment_id = last_comment_id
            need_init_file = False
            print(
                f"[INFO] Meglévő félkész topicfájl, folytatás ugyanerről az oldalról: "
                f"page={resume_page_no}, utolsó comment_id={resume_after_comment_id}, "
                f"meglévő kommentek={existing_comments}"
            )

    # Topiconként context reset: ez a legerősebb memória-védő lépés.
    fetcher.reset_context()

    first_fetch_url = set_query_param(get_topic_base_url(topic_url), "page", str(resume_page_no))
    print(f"[INFO] Topic megnyitása: {topic_title}")
    current_url, html = fetcher.fetch(first_fetch_url, wait_ms=int(delay * 1000))

    resolved_title = extract_topic_title(html, topic_title)
    topic_meta = extract_topic_meta(html, current_url)

    if need_init_file:
        write_topic_stream_header(topic_file, resolved_title, topic_meta, topic_url)
        print(f"[INFO] Új streamelt topicfájl létrehozva: {topic_file}")

    page_no = get_topic_page_number(current_url)
    total_downloaded = existing_comments
    has_existing_comments = existing_comments > 0

    seen_page_fingerprints: Set[str] = set()
    previous_page_fingerprint: Optional[str] = None
    first_page_after_resume = True
    page_hops = 0

    while True:
        print(f"[INFO] Kommentoldal #{page_no}: {current_url}")
        page_comments = parse_comments_from_topic_page(html, current_url)

        if page_no > 1 and not page_comments:
            print("[INFO] Üres vagy nem értelmezhető kommentoldal, megállok ennél a topicnál.")
            break

        if first_page_after_resume and resume_after_comment_id:
            original_len = len(page_comments)
            seen_last = False
            filtered: List[Dict] = []

            for c in page_comments:
                if not seen_last:
                    if str(c.get("comment_id") or "") == str(resume_after_comment_id):
                        seen_last = True
                    continue
                filtered.append(c)

            if seen_last:
                print(
                    f"[INFO] Resume szűrés: az első oldalon {original_len} kommentből "
                    f"{len(filtered)} új maradt az utolsó mentett comment_id után."
                )
                page_comments = filtered
            else:
                print(
                    "[INFO] Resume módban az utolsó mentett comment_id nem található ezen az oldalon, "
                    "ezért ezt az oldalt újként kezelem."
                )

            first_page_after_resume = False
            resume_after_comment_id = None

        current_fingerprint = build_page_fingerprint(page_comments)
        print(f"[DEBUG] Oldal fingerprint: {current_fingerprint}")

        if previous_page_fingerprint is not None and current_fingerprint == previous_page_fingerprint:
            print("[INFO] A mostani kommentoldal megegyezik az előzővel, a topic véget ért.")
            break

        if current_fingerprint in seen_page_fingerprints:
            print("[INFO] Már korábban látott kommentoldal-tartalom jött vissza, a topic véget ért.")
            break

        seen_page_fingerprints.add(current_fingerprint)

        added_on_this_page = 0
        for c in page_comments:
            item = comment_to_output_item(c)
            append_comment_to_stream_file(topic_file, item, has_existing_comments)
            has_existing_comments = True
            total_downloaded += 1
            added_on_this_page += 1

        print(
            f"[INFO] Oldal hozzáfűzve a topicfájlhoz: {topic_file} | "
            f"új kommentek ezen az oldalon: {added_on_this_page} | "
            f"összes letöltött komment eddig: {total_downloaded}"
        )

        next_url = get_topic_next_page_url(html, current_url)
        if not next_url:
            print("[INFO] Nincs több kommentoldal ennél a topicnál.")
            break

        next_page_no = get_topic_page_number(next_url)
        if next_page_no <= page_no:
            print("[INFO] A következő oldal száma nem nagyobb a mostaninál, leállok.")
            break

        print(f"[INFO] Következő kommentoldal jelölt: {next_url}")

        page_hops += 1
        if topic_reset_interval > 0 and page_hops % topic_reset_interval == 0:
            print("[INFO] Hosszú topic közbeni memória-kímélő context reset.")
            fetcher.reset_context()

        try:
            current_url, html = fetcher.fetch(next_url, wait_ms=int(delay * 1000))
        except Exception as e:
            print(f"[WARN] Hiba a következő kommentoldal megnyitásakor: {e}")
            break

        if not topic_has_any_comment_container(html):
            print("[INFO] A következő oldal már nem tartalmaz kommenteket, megállok.")
            break

        previous_page_fingerprint = current_fingerprint
        page_no = get_topic_page_number(current_url)

        del page_comments
        gc.collect()

    finalize_stream_json(topic_file)
    print(f"[DEBUG] Topic letöltés kész: {resolved_title} | összes letöltött komment: {total_downloaded}")
    print(f"[INFO] Topic JSON lezárva: {topic_file}")

    gc.collect()
    return total_downloaded


# -----------------------------
# Main fórum scrape
# -----------------------------

def scrape_main(
    fetcher: BrowserFetcher,
    output_dir: str,
    delay: float,
    only_title: Optional[str],
    start_page: int,
    max_pages: Optional[int],
    topic_reset_interval: int,
) -> None:
    base_output = Path(output_dir).expanduser().resolve()
    port_dir, topics_dir, visited_file = ensure_dirs(base_output)

    visited_topics = {
        normalize_topic_url_for_visited(x)
        for x in load_visited(visited_file)
    }

    current_url = MAIN_FORUM_URL if start_page <= 1 else set_query_param(MAIN_FORUM_URL, "page", str(start_page))
    page_no = start_page
    processed_main_pages = 0

    # A fő topiclista előtt is tiszta context.
    fetcher.reset_context()

    while True:
        if max_pages is not None and processed_main_pages >= max_pages:
            print("[INFO] Elértem a max-pages limitet.")
            break

        print(f"\n[INFO] Főoldali topiclista oldal #{page_no}: {current_url}")
        final_url, html = fetcher.fetch(current_url, wait_ms=int(delay * 1000))

        topics = parse_topic_rows_from_main_page(html, final_url)
        print(f"[INFO] Talált topicok ezen az oldalon: {len(topics)}")

        if not topics:
            print("[INFO] Nem találtam topicokat ezen a lapon, leállok.")
            break

        for idx, topic in enumerate(topics, start=1):
            topic_title = topic["title"]
            topic_url = topic["url"]
            topic_url_norm = normalize_topic_url_for_visited(topic_url)

            print(f"\n[INFO] ({idx}/{len(topics)}) Topic: {topic_title}")

            if only_title and only_title.lower() not in topic_title.lower():
                print("[INFO] Szűrés miatt kihagyva.")
                continue

            if topic_url_norm in visited_topics:
                print("[INFO] Már visitedben van, kihagyva.")
                continue

            topic_json_path = topic_file_path(topics_dir, topic_title)

            try:
                total_downloaded = scrape_topic(
                    fetcher=fetcher,
                    topic_title=topic_title,
                    topic_url=topic_url_norm,
                    topic_file=topic_json_path,
                    delay=delay,
                    topic_reset_interval=topic_reset_interval,
                )

                print(f"[DEBUG] Végső komment darabszám a témához: {topic_title} | {total_downloaded}")

                append_visited(visited_file, topic_url_norm)
                visited_topics.add(topic_url_norm)

                print(f"[INFO] Topic mentve: {topic_json_path}")
                print(f"[INFO] Topic visitedbe írva: {topic_url_norm}")

            except Exception as e:
                print(f"[WARN] Hiba topic feldolgozás közben: {topic_url} | {e}")

            # Topic után is takarítunk.
            fetcher.reset_context()
            gc.collect()

        processed_main_pages += 1

        next_url = get_main_next_page_url(html, final_url)
        if not next_url:
            print("[INFO] Nincs több főoldali topiclista oldal.")
            break

        next_page_val = extract_query_param(next_url, "page")
        next_page_no = int(next_page_val) if next_page_val and next_page_val.isdigit() else page_no + 1
        if next_page_no <= page_no:
            print("[INFO] Nem léptethető tovább a főoldali lapozás.")
            break

        current_url = next_url
        page_no = next_page_no

        del topics
        gc.collect()


# -----------------------------
# CLI
# -----------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="port.hu fórum scraper Playwright + BeautifulSoup alapon, streamelt komment-append módban"
    )
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa. Ide jön létre a port/ mappa.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Várakozás oldalak között másodpercben.",
    )
    parser.add_argument(
        "--only-title",
        default=None,
        help="Csak azokat a topicokat dolgozza fel, amelyek címében ez szerepel.",
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=1,
        help="A fórum főoldali lapozásának kezdő oldala.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Legfeljebb ennyi főoldali listázóoldalt dolgoz fel.",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Látható böngészőablakkal fusson.",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=90000,
        help="Navigációs timeout ezredmásodpercben.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=4,
        help="Ennyiszer próbálja újra a fetch műveleteket.",
    )
    parser.add_argument(
        "--topic-reset-interval",
        type=int,
        default=25,
        help="Ennyi kommentoldalanként teljes context reset hosszú topicoknál.",
    )
    parser.add_argument(
        "--auto-reset-fetches",
        type=int,
        default=120,
        help="Ennyi fetch után automatikus context reset.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        with BrowserFetcher(
            headless=not args.headed,
            slow_mo=50 if args.headed else 0,
            timeout_ms=args.timeout_ms,
            retries=args.retries,
            block_resources=True,
            auto_reset_fetches=args.auto_reset_fetches,
        ) as fetcher:
            scrape_main(
                fetcher=fetcher,
                output_dir=args.output,
                delay=args.delay,
                only_title=args.only_title,
                start_page=args.start_page,
                max_pages=args.max_pages,
                topic_reset_interval=args.topic_reset_interval,
            )
    except KeyboardInterrupt:
        print("\n[INFO] Megszakítva felhasználó által.")
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL] Végzetes hiba: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
    
    #python port_scraper.py --output ./port --headed