import argparse
import asyncio
import random
import re
from pathlib import Path
from typing import List, Dict, Set, Optional

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

BASE_URL = "https://www.gyakorikerdesek.hu"

# Példa:
# /allatok__egyeb-kerdesek__13193139-milyen-lenne-a-vadaszat-ha-nyul-is-vissza-tudna-loni
QUESTION_PATH_RE = re.compile(
    r"^/allatok__[a-z0-9\-]+__(\d+)(?:-[^/?#]+)?$",
    re.IGNORECASE
)


def build_list_url(page_num: int) -> str:
    if page_num == 1:
        return f"{BASE_URL}/allatok"
    return f"{BASE_URL}/allatok__oldal-{page_num}"


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def extract_topic_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None

    path = url
    if url.startswith(BASE_URL):
        path = url[len(BASE_URL):]

    match = QUESTION_PATH_RE.match(path)
    if not match:
        return None

    return match.group(1)


def load_visited_topic_ids(path: Path) -> Set[str]:
    visited_ids = set()

    if not path.exists():
        return visited_ids

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Formátum:
            # topic_id<TAB>url<TAB>title
            parts = line.split("\t")
            topic_id = parts[0].strip()

            if topic_id:
                visited_ids.add(topic_id)

    return visited_ids


def append_visited_topic(path: Path, topic_id: str, topic_url: str, topic_title: str) -> None:
    topic_id = normalize_text(topic_id)
    topic_url = normalize_text(topic_url)
    topic_title = normalize_text(topic_title)

    if not topic_id:
        return

    with path.open("a", encoding="utf-8") as f:
        f.write(f"{topic_id}\t{topic_url}\t{topic_title}\n")
        f.flush()


async def human_pause(min_s: float = 0.7, max_s: float = 1.8) -> None:
    await asyncio.sleep(random.uniform(min_s, max_s))


async def move_mouse_somewhere(page) -> None:
    viewport = page.viewport_size or {"width": 1366, "height": 900}
    x = random.randint(50, max(60, viewport["width"] - 50))
    y = random.randint(80, max(100, viewport["height"] - 50))
    await page.mouse.move(x, y, steps=random.randint(8, 20))


async def human_click(page, locator) -> None:
    await locator.scroll_into_view_if_needed()
    await human_pause(0.25, 0.7)

    try:
        await locator.hover(timeout=5000)
    except Exception:
        pass

    await human_pause(0.15, 0.5)

    box = await locator.bounding_box()
    if box:
        target_x = box["x"] + box["width"] / 2 + random.uniform(-6, 6)
        target_y = box["y"] + box["height"] / 2 + random.uniform(-4, 4)

        await move_mouse_somewhere(page)
        await page.mouse.move(target_x, target_y, steps=random.randint(10, 25))
        await human_pause(0.1, 0.35)
        await page.mouse.click(target_x, target_y, delay=random.randint(60, 170))
    else:
        await locator.click(delay=random.randint(60, 170), timeout=10000)

    await human_pause(0.7, 1.4)


async def get_first_text(locator, default: str = "") -> str:
    try:
        if await locator.count() > 0:
            text = await locator.first.inner_text()
            return normalize_text(text)
    except Exception:
        pass
    return default


async def extract_author_from_box(box) -> str:
    # Először a teljes fejlécet próbáljuk olvasni
    header_text = await get_first_text(
        box.locator(".valasz_fejlec, .valaszFejlec"),
        default=""
    )

    if header_text:
        # Példák:
        # "1/5 anonim válasza:"
        # "2/5 Mad Max2 válasza:"
        m = re.search(r"^\s*\d+/\d+\s+(.+?)\s+válasza:", header_text, re.IGNORECASE)
        if m:
            return normalize_text(m.group(1))

        # Fallback: ha benne van az anonim szó
        if "anonim" in header_text.lower():
            return "anonim"

    # Második fallback: külön elemekből próbáljuk
    author = await get_first_text(
        box.locator(
            ".valasz_fejlec .anonim, "
            ".valasz_fejlec a, "
            ".valaszFejlec .anonim, "
            ".valaszFejlec a"
        ),
        default=""
    )

    if author:
        return author

    return "ismeretlen"


async def collect_topics_from_list(page) -> List[Dict[str, str]]:
    topics: List[Dict[str, str]] = []
    seen_ids_on_page = set()

    links = page.locator("a[href]")
    count = await links.count()

    for i in range(count):
        link = links.nth(i)

        href = await link.get_attribute("href")
        if not href:
            continue

        href = href.strip()

        if href.startswith("/"):
            full_url = BASE_URL + href
            path = href
        elif href.startswith(BASE_URL):
            full_url = href
            path = href.replace(BASE_URL, "", 1)
        else:
            continue

        if "__oldal-" in full_url:
            continue

        match = QUESTION_PATH_RE.match(path)
        if not match:
            continue

        topic_id = match.group(1)
        if not topic_id or topic_id in seen_ids_on_page:
            continue

        title = normalize_text(await link.inner_text())
        if not title:
            continue

        seen_ids_on_page.add(topic_id)
        topics.append({
            "id": topic_id,
            "title": title,
            "url": full_url
        })

    return topics


async def find_topic_link(page, target_url: str):
    links = page.locator("a[href]")
    count = await links.count()

    for i in range(count):
        link = links.nth(i)
        href = await link.get_attribute("href")
        if not href:
            continue

        href = href.strip()

        if href.startswith("/"):
            full_url = BASE_URL + href
        else:
            full_url = href

        if full_url == target_url:
            return link

    return None


async def open_topic_by_click(page, topic_url: str, link_locator) -> bool:
    old_url = page.url

    try:
        await human_click(page, link_locator)
    except Exception:
        return False

    for _ in range(40):
        await asyncio.sleep(0.25)
        if page.url != old_url:
            break

    if page.url == old_url:
        try:
            await page.goto(topic_url, wait_until="domcontentloaded", timeout=60000)
        except Exception:
            return False

    try:
        await page.wait_for_selector("h1", timeout=15000)
        return True
    except Exception:
        return False


async def scrape_current_topic(page) -> Optional[Dict[str, object]]:
    try:
        await page.wait_for_selector("h1", timeout=15000)
        await human_pause(0.6, 1.2)
    except Exception:
        return None

    title = await get_first_text(page.locator("h1"))
    if not title:
        return None

    comments = []

    answer_boxes = page.locator("div.valasz")
    answer_count = await answer_boxes.count()

    for i in range(answer_count):
        box = answer_boxes.nth(i)

        author = await extract_author_from_box(box)

        text = await get_first_text(
            box.locator(
                ".valasz_valasz, "
                ".valaszValasz, "
                ".valasz_szoveg, "
                ".valaszSzoveg"
            ),
            default=""
        )

        if text:
            comments.append({
                "author": author,
                "text": text
            })

    return {
        "title": title,
        "comments": comments
    }


def write_topic_to_file(output_handle, topic_title: str, comments: List[Dict[str, str]]) -> None:
    output_handle.write(f"Post: {topic_title}\n")
    for c in comments:
        output_handle.write(f"Comment by {c['author']}: {c['text']}\n")
    output_handle.write("-" * 80 + "\n\n")
    output_handle.flush()


async def go_back_to_list(page, list_url: str) -> None:
    try:
        response = await page.go_back(wait_until="domcontentloaded", timeout=15000)
        if response is None:
            await page.goto(list_url, wait_until="domcontentloaded", timeout=60000)
    except Exception:
        await page.goto(list_url, wait_until="domcontentloaded", timeout=60000)

    await human_pause(0.8, 1.8)


async def process_topic(
    page,
    list_url: str,
    topic: Dict[str, str],
    output_handle,
    visited_file: Path,
    visited_topic_ids: Set[str]
) -> None:
    topic_id = topic["id"]
    topic_title = normalize_text(topic["title"])
    topic_url = topic["url"]

    if topic_id in visited_topic_ids:
        print(f"[SKIP] Már feldolgozva (ID): {topic_id} | {topic_title}")
        return

    print(f"[INFO] Feldolgozás: {topic_title}")

    await page.goto(list_url, wait_until="domcontentloaded", timeout=60000)
    await human_pause(1.0, 2.0)

    link = await find_topic_link(page, topic_url)
    if not link:
        print(f"[WARN] Nem találom a topic linkjét: {topic_url}")
        return

    opened = await open_topic_by_click(page, topic_url, link)
    if not opened:
        print(f"[HIBA] Nem sikerült megnyitni a topicot: {topic_url}")
        return

    data = await scrape_current_topic(page)
    if not data:
        print(f"[HIBA] Nem sikerült kinyerni a topic adatait: {topic_url}")
        await go_back_to_list(page, list_url)
        return

    real_title = normalize_text(str(data["title"]))
    comments = data["comments"]

    real_topic_id = extract_topic_id_from_url(page.url) or topic_id
    real_topic_url = page.url

    if real_topic_id in visited_topic_ids:
        print(f"[SKIP] Már mentve volt közben: {real_topic_id} | {real_title}")
        await go_back_to_list(page, list_url)
        return

    write_topic_to_file(output_handle, real_title, comments)
    append_visited_topic(visited_file, real_topic_id, real_topic_url, real_title)
    visited_topic_ids.add(real_topic_id)

    print(f"[OK] Mentve: {real_title} | ID: {real_topic_id} | kommentek: {len(comments)}")

    await human_pause(1.0, 2.0)
    await go_back_to_list(page, list_url)


async def main():
    parser = argparse.ArgumentParser(description="GyakoriKérdések Állatok scraper Playwrighttal")
    parser.add_argument("--start", type=int, required=True, help="Kezdő oldal száma")
    parser.add_argument("--end", type=int, required=True, help="Utolsó oldal száma")
    parser.add_argument(
        "--output",
        type=str,
        default="scraper_output",
        help="Output mappa neve vagy útvonala"
    )
    parser.add_argument("--headless", action="store_true", help="Headless mód")
    args = parser.parse_args()

    if args.start < 1:
        raise ValueError("A --start értéke legalább 1 legyen.")
    if args.end < args.start:
        raise ValueError("A --end nem lehet kisebb, mint a --start.")

    output_dir = Path(args.output).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "allatok.txt"
    visited_file = output_dir / "visited_topics.txt"

    visited_topic_ids = load_visited_topic_ids(visited_file)

    print(f"[INFO] Output mappa: {output_dir}")
    print(f"[INFO] Output fájl: {output_file}")
    print(f"[INFO] Visited fájl: {visited_file}")
    print(f"[INFO] Már ismert topic ID-k: {len(visited_topic_ids)}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=args.headless,
            slow_mo=50
        )

        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 900}
        )

        page = await context.new_page()

        with output_file.open("a", encoding="utf-8") as output_handle:
            for page_num in range(args.start, args.end + 1):
                list_url = build_list_url(page_num)
                print(f"\n[INFO] === {page_num}. OLDAL: {list_url} ===")

                try:
                    await page.goto(list_url, wait_until="domcontentloaded", timeout=60000)
                    await human_pause(1.2, 2.5)

                    topics = await collect_topics_from_list(page)
                    print(f"[INFO] Talált topicok: {len(topics)}")

                    for idx, topic in enumerate(topics, start=1):
                        title = normalize_text(topic["title"])
                        topic_id = topic["id"]

                        print(f"[INFO] {idx}/{len(topics)} -> {title}")

                        if topic_id in visited_topic_ids:
                            print(f"[SKIP] Már visited-ben van: {topic_id} | {title}")
                            continue

                        try:
                            await process_topic(
                                page=page,
                                list_url=list_url,
                                topic=topic,
                                output_handle=output_handle,
                                visited_file=visited_file,
                                visited_topic_ids=visited_topic_ids
                            )
                        except PlaywrightTimeoutError:
                            print(f"[HIBA] Timeout ennél a topicnál: {topic['url']}")
                            try:
                                await page.goto(list_url, wait_until="domcontentloaded", timeout=60000)
                                await human_pause(1.0, 2.0)
                            except Exception:
                                pass
                        except Exception as e:
                            print(f"[HIBA] Topic feldolgozási hiba: {topic['url']} | {e}")
                            try:
                                await page.goto(list_url, wait_until="domcontentloaded", timeout=60000)
                                await human_pause(1.0, 2.0)
                            except Exception:
                                pass

                except Exception as e:
                    print(f"[HIBA] Nem sikerült betölteni az oldalt: {list_url} | {e}")

        await browser.close()

    print("\n[KÉSZ] A scraper lefutott.")


if __name__ == "__main__":
    asyncio.run(main())
    
    #TODO:
  
    #100% és stb ne legyen benne a kimeneti fájlban
    #esetleg [link] kiszedése a kommentekből
    