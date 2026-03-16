import argparse
import asyncio
import random
import re
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

BASE_URL = "https://www.gyakorikerdesek.hu"

# Példa:
# /allatok__egyeb-kerdesek__1234567-kerdes-szovege
QUESTION_PATH_RE = re.compile(
    r"^/allatok__[a-z0-9\-]+__\d+(?:-[^/?#]+)?$",
    re.IGNORECASE
)


def build_list_url(page_num: int) -> str:
    if page_num == 1:
        return f"{BASE_URL}/allatok"
    return f"{BASE_URL}/allatok__oldal-{page_num}"


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


async def human_sleep(min_s: float = 1.0, max_s: float = 2.0) -> None:
    await asyncio.sleep(random.uniform(min_s, max_s))


async def collect_question_urls(page) -> list[str]:
    urls = []
    seen = set()

    links = page.locator("a[href]")
    count = await links.count()

    for i in range(count):
        href = await links.nth(i).get_attribute("href")
        if not href:
            continue

        href = href.strip()

        if href.startswith("/"):
            path = href
            full_url = BASE_URL + href
        elif href.startswith(BASE_URL):
            full_url = href
            path = href.replace(BASE_URL, "", 1)
        else:
            continue

        if "__oldal-" in full_url:
            continue

        if QUESTION_PATH_RE.match(path) and full_url not in seen:
            seen.add(full_url)
            urls.append(full_url)

    return urls


async def get_first_text(locator, default: str = "") -> str:
    try:
        if await locator.count() > 0:
            text = await locator.first.inner_text()
            return normalize_text(text)
    except Exception:
        pass
    return default


async def scrape_question(page, url: str, file_handle) -> None:
    print(f"[INFO] --> Kérdés megnyitása: {url}")

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_selector("h1", timeout=15000)
        await human_sleep(1.2, 2.5)

        title = await get_first_text(page.locator("h1"))
        if not title:
            print(f"[WARN] Nincs cím ezen az oldalon: {url}")
            return

        file_handle.write(f"Post: {title}\n")

        answer_boxes = page.locator("div.valasz")
        answer_count = await answer_boxes.count()

        if answer_count == 0:
            print(f"[WARN] Nem találtam válaszokat: {url}")

        for i in range(answer_count):
            box = answer_boxes.nth(i)

            author = await get_first_text(
                box.locator(
                    ".valasz_fejlec .anonim, "
                    ".valasz_fejlec a, "
                    ".valaszFejlec .anonim, "
                    ".valaszFejlec a"
                ),
                default="ismeretlen"
            )

            text = await get_first_text(
                box.locator(
                    ".valasz_valasz, "
                    ".valaszValasz, "
                    ".valasz_szoveg, "
                    ".valaszSzoveg"
                )
            )

            if text:
                file_handle.write(f"Comment by {author}: {text}\n")

        file_handle.write("-" * 80 + "\n")
        file_handle.flush()

    except PlaywrightTimeoutError:
        print(f"[HIBA] Időtúllépés: {url}")
    except Exception as e:
        print(f"[HIBA] Nem sikerült beolvasni: {url} | Hiba: {e}")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("startpage", type=int, help="Kezdő oldal száma")
    parser.add_argument("endpage", type=int, help="Befejező oldal száma")
    parser.add_argument("--headless", action="store_true", help="Headless mód")
    args = parser.parse_args()

    if args.startpage < 1:
        parser.error("A startpage legalább 1 legyen.")
    if args.endpage < args.startpage:
        parser.error("Az endpage nem lehet kisebb, mint a startpage.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=args.headless)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        with open("allatok.txt", "w", encoding="utf-8") as f:
            for page_num in range(args.startpage, args.endpage + 1):
                list_url = build_list_url(page_num)
                print(f"\n[INFO] === {page_num}. OLDAL BETÖLTÉSE: {list_url} ===\n")

                try:
                    await page.goto(list_url, wait_until="domcontentloaded", timeout=60000)
                    await human_sleep(1.5, 2.5)

                    question_urls = await collect_question_urls(page)
                    print(f"[INFO] Talált kérdések: {len(question_urls)}")

                    if not question_urls:
                        continue

                    for idx, q_url in enumerate(question_urls, start=1):
                        print(f"[INFO] {idx}/{len(question_urls)} kérdés feldolgozása")
                        await scrape_question(page, q_url, f)

                        # Visszalépés helyett újratöltjük a listaoldalt.
                        # Ez stabilabb, mint a history.back().
                        await page.goto(list_url, wait_until="domcontentloaded", timeout=60000)
                        await human_sleep(0.8, 1.6)

                except PlaywrightTimeoutError:
                    print(f"[HIBA] Időtúllépés a listaoldalon: {list_url}")
                except Exception as e:
                    print(f"[HIBA] Nem sikerült feldolgozni az oldalt: {list_url} | Hiba: {e}")

        await browser.close()
        print("\n[KÉSZ] A kért oldalak feldolgozva.")


if __name__ == "__main__":
    asyncio.run(main())