import argparse
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

BASE_LIST_URL = "https://prohardver.hu/temak/notebook/listaz.php"
RANGE_RE = re.compile(r"^(\d+)\s*-\s*(\d+)$")


def build_list_url(offset: int) -> str:
    return BASE_LIST_URL if offset <= 0 else f"{BASE_LIST_URL}?offset={offset}"


def setup_driver(headless: bool = False) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1400,1000")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--lang=hu-HU")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(45)
    return driver


def wait_for_page(driver: webdriver.Chrome, timeout: int = 20) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )


def try_click(driver: webdriver.Chrome, element) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(0.2)
        element.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            return False


def close_skip_popup(driver: webdriver.Chrome, timeout: float = 3.0) -> None:
    xpaths = [
        "//*[self::button or self::a or self::span][normalize-space()='Lemaradok']",
        "//*[self::button or self::a or self::span][contains(normalize-space(), 'Lemaradok')]",
        "//input[@type='button' and @value='Lemaradok']",
    ]
    end_time = time.time() + timeout
    while time.time() < end_time:
        for xpath in xpaths:
            try:
                elements = driver.find_elements(By.XPATH, xpath)
                for element in elements:
                    if element.is_displayed() and try_click(driver, element):
                        time.sleep(0.6)
                        return
            except Exception:
                pass
        time.sleep(0.25)


def clean_text(text: str) -> str:
    text = text.replace("\r", "")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_topic_links(html: str, page_url: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        "div.thread-list h4 a[href*='/tema/']",
        "div.col.thread-title-thread h4 a[href*='/tema/']",
        "main h4 a[href*='/tema/']",
        "h4 a[href*='/tema/']",
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
        if "/tema/" not in href or "/temak/" in href:
            continue
        title = clean_text(a.get_text(" ", strip=True))
        if not title:
            continue
        full_url = urljoin(page_url, href)
        if full_url in seen:
            continue
        seen.add(full_url)
        topics.append((title, full_url))

    return topics[:100]


def extract_page_title(html: str, fallback: str = "") -> str:
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("title")
    if title_tag:
        title = clean_text(title_tag.get_text(" ", strip=True))
        title = re.sub(r"\s*-\s*PROHARDVER!.*$", "", title, flags=re.IGNORECASE)
        if title:
            return title

    h1 = soup.find("h1")
    if h1:
        text = clean_text(h1.get_text(" ", strip=True))
        if text:
            return text

    h4 = soup.find("h4")
    if h4:
        text = clean_text(h4.get_text(" ", strip=True))
        if text:
            return text

    return fallback.strip() or "Ismeretlen topic"


def extract_author(post) -> str:
    selectors = [
        ".msg-user",
        ".user-name",
        ".media-left",
    ]

    for selector in selectors:
        node = post.select_one(selector)
        if not node:
            continue
        text = clean_text(node.get_text("\n", strip=True))
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if lines:
            return lines[0]

    full_text = clean_text(post.get_text("\n", strip=True))
    lines = [line.strip() for line in full_text.splitlines() if line.strip()]
    return lines[0] if lines else "ismeretlen"


def extract_comment_text(post) -> str:
    selectors = [
        ".msg-content",
        "p.mgt0",
        ".media-body",
    ]

    for selector in selectors:
        node = post.select_one(selector)
        if not node:
            continue
        text = clean_text(node.get_text("\n", strip=True))
        if text:
            return text

    return ""


def extract_post_id(post) -> str:
    id_link = post.find("a", string=re.compile(r"#\d+"))
    if id_link:
        return clean_text(id_link.get_text(" ", strip=True))

    data_id = post.get("data-id")
    if data_id:
        return f"#{data_id}"

    return ""


def parse_comments_from_html(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[Dict[str, str]] = []

    posts = soup.select("li.media[data-id]")
    for post in posts:
        if not post.select_one(".msg-content") and not post.select_one("p.mgt0"):
            continue

        comment_text = extract_comment_text(post)
        if not comment_text:
            continue

        results.append(
            {
                "post_id": extract_post_id(post),
                "author": extract_author(post),
                "text": comment_text,
            }
        )

    return results


def get_topic_page_urls(driver: webdriver.Chrome, topic_url: str, delay: float) -> List[str]:
    driver.get(topic_url)
    wait_for_page(driver)
    close_skip_popup(driver)
    time.sleep(delay)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    page_entries: List[Tuple[int, str]] = []
    seen: Set[str] = set()

    for a in soup.find_all("a", href=True):
        text = clean_text(a.get_text(" ", strip=True))
        match = RANGE_RE.match(text)
        if not match:
            continue
        href = urljoin(driver.current_url, a["href"])
        if "/tema/" not in href:
            continue
        lower_bound = min(int(match.group(1)), int(match.group(2)))
        if href not in seen:
            seen.add(href)
            page_entries.append((lower_bound, href))

    if driver.current_url not in seen:
        page_entries.append((10**9, driver.current_url))

    if not page_entries:
        return [driver.current_url]

    page_entries.sort(key=lambda item: item[0])
    return [url for _, url in page_entries]


def scrape_topic(driver: webdriver.Chrome, topic_title: str, topic_url: str, delay: float) -> Tuple[str, List[Dict[str, str]]]:
    page_urls = get_topic_page_urls(driver, topic_url, delay)
    collected: List[Dict[str, str]] = []
    seen_ids: Set[str] = set()
    resolved_title = topic_title

    for page_url in page_urls:
        driver.get(page_url)
        wait_for_page(driver)
        close_skip_popup(driver)
        time.sleep(delay)

        html = driver.page_source
        resolved_title = extract_page_title(html, fallback=resolved_title)
        comments = parse_comments_from_html(html)

        for comment in comments:
            unique_key = comment["post_id"] or f"{comment['author']}::{comment['text'][:80]}"
            if unique_key in seen_ids:
                continue
            seen_ids.add(unique_key)
            collected.append(comment)

    return resolved_title, collected


def write_topic(output_path: Path, title: str, comments: List[Dict[str, str]]) -> None:
    with output_path.open("a", encoding="utf-8") as f:
        f.write("Post:\n")
        f.write(f"{title}\n")
        for comment in comments:
            author = comment["author"] or "ismeretlen"
            f.write(f"Comment by {author}:\n")
            f.write(comment["text"].strip() + "\n")
        f.write("\n" + "=" * 80 + "\n\n")


def scrape_offsets(start_offset: int, end_offset: int, output_file: str, delay: float, headless: bool) -> None:
    output_path = Path(output_file)
    output_path.write_text("", encoding="utf-8")

    driver = setup_driver(headless=headless)
    processed_topics: Set[str] = set()

    try:
        for offset in range(start_offset, end_offset + 1, 100):
            list_url = build_list_url(offset)
            print(f"\n[INFO] Offset oldal betöltése: {list_url}")

            try:
                driver.get(list_url)
                wait_for_page(driver)
            except TimeoutException:
                print(f"[WARN] Timeout a listaoldalnál: {list_url}")
                continue

            close_skip_popup(driver)
            time.sleep(delay)

            topics = parse_topic_links(driver.page_source, driver.current_url)
            print(f"[INFO] Talált topicok száma: {len(topics)}")

            if not topics:
                print("[WARN] Nem találtam topic linkeket ezen az oldalon.")
                continue

            for index, (topic_title, topic_url) in enumerate(topics, start=1):
                if topic_url in processed_topics:
                    continue

                processed_topics.add(topic_url)
                print(f"[INFO] ({index}/{len(topics)}) Topic feldolgozása: {topic_title}")

                try:
                    resolved_title, comments = scrape_topic(driver, topic_title, topic_url, delay)
                except TimeoutException:
                    print(f"[WARN] Timeout a topicnál: {topic_url}")
                    continue
                except WebDriverException as e:
                    print(f"[WARN] Selenium hiba a topicnál: {topic_url} | {e}")
                    continue
                except Exception as e:
                    print(f"[WARN] Váratlan hiba a topicnál: {topic_url} | {e}")
                    continue

                write_topic(output_path, resolved_title, comments)
                print(f"[INFO] Mentve: {resolved_title} | kommentek: {len(comments)}")
                time.sleep(delay)

    finally:
        driver.quit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PROHARDVER notebook topic scraper Seleniummal."
    )
    parser.add_argument(
        "start_offset",
        type=int,
        help="Kezdő offset. Pl. 0 vagy 100 vagy 200",
    )
    parser.add_argument(
        "end_offset",
        type=int,
        help="Vég offset. Pl. 300",
    )
    parser.add_argument(
        "--output",
        default="prohardver_notebook.txt",
        help="Kimeneti fájl neve. Alapértelmezett: prohardver_notebook.txt",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Várakozás másodpercben az oldalak között. Alapértelmezett: 1.5",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Headless mód. Popupok miatt általában nem ezt érdemes használni.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.start_offset < 0 or args.end_offset < 0:
        print("A start_offset és end_offset nem lehet negatív.")
        sys.exit(1)

    if args.start_offset > args.end_offset:
        print("A start_offset nem lehet nagyobb, mint az end_offset.")
        sys.exit(1)

    scrape_offsets(
        start_offset=args.start_offset,
        end_offset=args.end_offset,
        output_file=args.output,
        delay=args.delay,
        headless=args.headless,
    )


if __name__ == "__main__":
    main()
