import argparse
import re
import sys
import time
from pathlib import Path
from typing import List, Set, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import (
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

BASE_LIST_URL = "https://prohardver.hu/temak/notebook/listaz.php"


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


def setup_driver(headless: bool = False) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1600,1200")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--lang=hu-HU")
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver


def wait_ready(driver: webdriver.Chrome, timeout: int = 20) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )


def safe_click(driver: webdriver.Chrome, element) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(0.2)
        try:
            element.click()
        except Exception:
            driver.execute_script("arguments[0].click();", element)
        return True
    except Exception:
        return False


def click_first_visible(driver: webdriver.Chrome, xpaths: List[str], timeout: float = 5.0) -> bool:
    end_time = time.time() + timeout
    while time.time() < end_time:
        for xpath in xpaths:
            try:
                elements = driver.find_elements(By.XPATH, xpath)
            except Exception:
                elements = []

            for element in elements:
                try:
                    if not element.is_displayed():
                        continue
                except StaleElementReferenceException:
                    continue

                if safe_click(driver, element):
                    time.sleep(0.8)
                    return True
        time.sleep(0.2)
    return False


def reject_cookies(driver: webdriver.Chrome, timeout: float = 8.0) -> bool:
    xpaths = [
        "//*[self::button or self::a or self::span][normalize-space()='NEM FOGADOM EL']",
        "//*[contains(translate(normalize-space(), 'abcdefghijklmnopqrstuvwxyzáéíóöőúüű', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÖŐÚÜŰ'), 'NEM FOGADOM EL')]",
    ]
    clicked = click_first_visible(driver, xpaths, timeout=timeout)
    if clicked:
        print("[DEBUG] Sütik elutasítva.")
    return clicked


def close_skip_popup(driver: webdriver.Chrome, timeout: float = 4.0) -> bool:
    xpaths = [
        "//*[self::button or self::a or self::span][normalize-space()='Lemaradok']",
        "//*[contains(normalize-space(), 'Lemaradok')]",
        "//input[@type='button' and @value='Lemaradok']",
    ]
    clicked = click_first_visible(driver, xpaths, timeout=timeout)
    if clicked:
        print("[DEBUG] Lemaradok popup bezárva.")
    return clicked


def dismiss_known_popups(driver: webdriver.Chrome, first_page: bool = False) -> None:
    if first_page:
        reject_cookies(driver, timeout=8.0)
    close_skip_popup(driver, timeout=3.0)


def wait_for_topic_list(driver: webdriver.Chrome, timeout: int = 20) -> None:
    selectors = [
        "div.thread-list h4 a[href*='/tema/']",
        "div.col.thread-title-thread h4 a[href*='/tema/']",
        "main h4 a[href*='/tema/']",
        "h4 a[href*='/tema/']",
    ]
    for selector in selectors:
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            return
        except TimeoutException:
            pass
    raise TimeoutException("Nem található topic lista.")


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
        full_url = urljoin(page_url, href)
        if "/tema/" not in full_url or "/temak/" in full_url:
            continue

        title = clean_text(a.get_text(" ", strip=True))
        if not title:
            continue

        if full_url in seen:
            continue
        seen.add(full_url)
        topics.append((title, full_url))

    return topics[:100]


def wait_for_messages(driver: webdriver.Chrome, timeout: int = 20) -> None:
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "li.media[data-id]"))
    )


def extract_topic_title(driver: webdriver.Chrome, fallback: str) -> str:
    soup = BeautifulSoup(driver.page_source, "html.parser")

    title_selectors = [
        "meta[property='og:title']",
        "title",
        "h1",
    ]

    for selector in title_selectors:
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


def extract_author(post) -> str:
    header = post.select_one(".msg-header")
    if header:
        header_text = clean_text(header.get_text(" ", strip=True))
        m = re.match(r"#\d+\s+(.+?)\s*>\s*.+?#\d+", header_text)
        if m:
            author = clean_text(m.group(1))
            if author:
                return author

    msg_user = post.select_one(".msg-user")
    if msg_user:
        txt = clean_text(msg_user.get_text("\n", strip=True))
        lines = [line.strip() for line in txt.splitlines() if line.strip()]
        if lines:
            return lines[0]

    media_left = post.select_one(".media-left")
    if media_left:
        txt = clean_text(media_left.get_text("\n", strip=True))
        lines = [line.strip() for line in txt.splitlines() if line.strip()]
        if lines:
            return lines[0]

    return "ismeretlen"


def extract_comment_text(post) -> str:
    selectors = [
        ".msg-content p.mgt0",
        ".msg-content",
        "p.mgt0",
    ]

    for selector in selectors:
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


def parse_comments_from_html(html: str) -> List[Tuple[str, str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    posts = soup.select("li.media[data-id]")
    results: List[Tuple[str, str, str]] = []

    print(f"[DEBUG] Talált li.media[data-id] elemek száma: {len(posts)}")

    for index, post in enumerate(posts, start=1):
        post_id = clean_text(post.get("data-id", ""))
        author = extract_author(post)
        comment = extract_comment_text(post)

        preview = comment[:120].replace("\n", " | ") if comment else "<üres>"
        print(
            f"[DEBUG] Poszt #{index} | data-id={post_id or '-'} | szerző={author} | preview={preview}"
        )

        if not comment:
            continue

        results.append((post_id, author, comment))

    print(f"[DEBUG] Kinyert kommentek ezen az oldalon: {len(results)}")
    return results


def get_next_page_element(driver: webdriver.Chrome):
    xpaths = [
        "//a[@rel='next']",
        "//a[contains(@title, 'Következő blokk')]",
        "//li[contains(@class,'nav-arrow')]//a[@rel='next']",
        "//a[contains(@href, '/hsz_') and (.//span[contains(@class,'fa-forward')] or .//span[contains(@class,'fa-step-forward')])]",
    ]
    for xpath in xpaths:
        try:
            elements = driver.find_elements(By.XPATH, xpath)
        except Exception:
            elements = []

        for el in elements:
            try:
                if el.is_displayed() and el.is_enabled():
                    return el
            except StaleElementReferenceException:
                continue
    return None


def scrape_topic_sequentially(driver: webdriver.Chrome, topic_title: str, topic_url: str, delay: float):
    print(f"[DEBUG] Topic megnyitása: {topic_url}")

    driver.get(topic_url)
    wait_ready(driver)
    dismiss_known_popups(driver, first_page=False)
    wait_for_messages(driver)
    time.sleep(delay)

    resolved_title = extract_topic_title(driver, topic_title)
    all_comments: List[Tuple[str, str, str]] = []
    seen_posts: Set[str] = set()
    seen_urls: Set[str] = set()
    page_index = 1

    while True:
        current_url = driver.current_url
        if current_url in seen_urls:
            print(f"[DEBUG] Már feldolgozott oldal, leállás: {current_url}")
            break
        seen_urls.add(current_url)

        print(f"[DEBUG] Kommentoldal #{page_index}: {current_url}")

        page_comments = parse_comments_from_html(driver.page_source)

        for post_id, author, comment in page_comments:
            unique_key = post_id or f"{author}::{comment[:150]}"
            if unique_key in seen_posts:
                continue
            seen_posts.add(unique_key)
            all_comments.append((post_id, author, comment))
            print(f"[DEBUG] MENTÉS -> {author}: {comment[:120].replace(chr(10), ' | ')}")

        next_el = get_next_page_element(driver)
        if not next_el:
            print("[DEBUG] Nincs több következő oldal.")
            break

        try:
            next_href = next_el.get_attribute("href")
        except Exception:
            next_href = None

        print(f"[DEBUG] Következő oldal gomb megvan. href={next_href}")

        old_url = driver.current_url
        if not safe_click(driver, next_el):
            print("[DEBUG] Nem sikerült a következő oldal gomb kattintása.")
            break

        try:
            WebDriverWait(driver, 20).until(lambda d: d.current_url != old_url)
            wait_ready(driver)
            dismiss_known_popups(driver, first_page=False)
            wait_for_messages(driver)
            time.sleep(delay)
            page_index += 1
        except TimeoutException:
            print("[DEBUG] A következő oldal betöltése timeout miatt megszakadt.")
            break

    return resolved_title, all_comments


def write_topic(output_path: Path, title: str, comments: List[Tuple[str, str, str]]) -> None:
    with output_path.open("a", encoding="utf-8") as f:
        f.write("Topic:\n")
        f.write(f"{title}\n")

        for _, author, comment in comments:
            f.write("Comment:\n")
            f.write(f"{author}: {comment}\n\n")

        f.write("=" * 80 + "\n\n")


def scrape_offsets(start_offset: int, end_offset: int, output_file: str, delay: float, headless: bool) -> None:
    output_path = Path(output_file)
    output_path.write_text("", encoding="utf-8")

    driver = setup_driver(headless=headless)
    processed_topics: Set[str] = set()
    first_list_page = True

    try:
        for offset in range(start_offset, end_offset + 1, 100):
            list_url = build_list_url(offset)
            print(f"\n[INFO] Listaoldal megnyitása: {list_url}")

            try:
                driver.get(list_url)
                wait_ready(driver)
                dismiss_known_popups(driver, first_page=first_list_page)
                first_list_page = False
                wait_for_topic_list(driver)
                time.sleep(delay)
            except TimeoutException:
                print(f"[WARN] Timeout a listaoldalnál: {list_url}")
                continue

            topics = parse_topic_links(driver.page_source, driver.current_url)
            print(f"[INFO] Talált topicok száma: {len(topics)}")

            if not topics:
                continue

            for idx, (topic_title, topic_url) in enumerate(topics, start=1):
                if topic_url in processed_topics:
                    continue

                processed_topics.add(topic_url)
                print(f"\n[INFO] ({idx}/{len(topics)}) Topic: {topic_title}")

                try:
                    resolved_title, comments = scrape_topic_sequentially(
                        driver, topic_title, topic_url, delay
                    )
                    print(f"[INFO] Összes mentett komment: {len(comments)}")
                    write_topic(output_path, resolved_title, comments)
                    print(f"[INFO] Topic elmentve: {resolved_title}")
                except TimeoutException:
                    print(f"[WARN] Timeout a topicnál: {topic_url}")
                except WebDriverException as e:
                    print(f"[WARN] Selenium hiba a topicnál: {topic_url} | {e}")
                except Exception as e:
                    print(f"[WARN] Váratlan hiba a topicnál: {topic_url} | {e}")

                try:
                    driver.get(list_url)
                    wait_ready(driver)
                    dismiss_known_popups(driver, first_page=False)
                    wait_for_topic_list(driver)
                    time.sleep(delay)
                except Exception as e:
                    print(f"[WARN] Nem sikerült visszamenni a listaoldalra: {e}")
                    break
    finally:
        driver.quit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PROHARDVER notebook topic scraper Seleniummal.")
    parser.add_argument("start_offset", type=int, help="Kezdő offset. Pl. 0 vagy 100")
    parser.add_argument("end_offset", type=int, help="Vég offset. Pl. 200 vagy 300")
    parser.add_argument("--output", default="notebooks.txt", help="Kimeneti fájl neve.")
    parser.add_argument("--delay", type=float, default=1.2, help="Várakozás oldalak között másodpercben.")
    parser.add_argument("--headless", action="store_true", help="Headless mód.")
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

    scrape_offsets(
        start_offset=args.start_offset,
        end_offset=args.end_offset,
        output_file=args.output,
        delay=args.delay,
        headless=args.headless,
    )


if __name__ == "__main__":
    main()