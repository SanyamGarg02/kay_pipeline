import asyncio
import csv
import re
from urllib.parse import urljoin
from playwright.async_api import async_playwright

# =====================
# CONFIG
# =====================
OUTPUT_FILE = "glamira/glamira_products_list1.csv"
MAX_PRODUCTS_PER_CATEGORY = 200  # limit per category

# CSS selectors for scraping
PRODUCT_CARD = "li.product-item"
PRODUCT_LINK_IN_CARD = "a.product-link"
PRODUCT_TITLE_IN_CARD = "h2.product-name"
PRODUCT_PRICE_IN_CARD = "span.price"

# Filters
BLOCKED_KEYWORDS = ["lab-grown","synthetic","man-made","noit natural "]

# =====================
# Helpers
# =====================

async def extract_from_card(card, base_url):
    """Extract url, title, price from product card. Returns dict or None if filtered."""
    try:
        # URL
        link_el = await card.query_selector(PRODUCT_LINK_IN_CARD)
        href = await link_el.get_attribute("href") if link_el else None
        if href and href.startswith("/"):
            href = urljoin(base_url, href)

        # Title
        title_el = await card.query_selector(PRODUCT_TITLE_IN_CARD)
        title = (await title_el.inner_text()).strip() if title_el else None

        # Price
        price_el = await card.query_selector(PRODUCT_PRICE_IN_CARD)
        price_text = (await price_el.inner_text()).strip() if price_el else None

        # Validation filters
        if not (href and title and price_text):
            return None

        # Keyword filter
        lowered = title.lower()
        if any(bad in lowered for bad in BLOCKED_KEYWORDS):
            return None

        # Parse price
        match = re.search(r"[\d,.]+", price_text)
        if not match:
            return None
        price_val = float(match.group(0).replace(",", ""))

        # Price threshold
        if price_val < 999:
            return None

        return {
            "name": title,
            "price": price_text,
            "url": href,
            "source": "glamira",   # fixed source column
        }

    except Exception:
        return None


async def scrape_category(page, category_url):
    """Scrape one category (up to MAX_PRODUCTS_PER_CATEGORY)."""
    await page.goto(category_url, timeout=60000)
    await page.wait_for_selector(PRODUCT_CARD)

    products = []
    seen = set()

    while len(products) < MAX_PRODUCTS_PER_CATEGORY:
        # Scroll down
        await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        await asyncio.sleep(2)

        cards = await page.query_selector_all(PRODUCT_CARD)

        for card in cards:
            item = await extract_from_card(card, category_url)
            if item and item["url"] not in seen:
                products.append(item)
                seen.add(item["url"])

            if len(products) >= MAX_PRODUCTS_PER_CATEGORY:
                break

        # If no new cards → stop
        if len(seen) >= len(cards): 
            break

    return products


async def scrape_glamira(categories):
    """Scrape multiple categories and save into single CSV."""
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        page = await browser.new_page()

        all_products = []

        for category_url in categories:
            print(f"Scraping {category_url} ...")
            items = await scrape_category(page, category_url)
            print(f" → Got {len(items)} items")
            all_products.extend(items)

        await browser.close()

    # Save to CSV
    if all_products:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["name", "price", "url","source"])
            writer.writeheader()
            writer.writerows(all_products)
        print(f"✅ Saved {len(all_products)} products to {OUTPUT_FILE}")
    else:
        print("⚠️ No products found.")


# =====================
# Run
# =====================
if __name__ == "__main__":
    categories = [
       "https://www.glamira.com/bracelets/?product_list_dir=desc&product_list_order=price",
        "https://www.glamira.com/diamond-rings/price-1000-1814162/diamond/",
        "https://www.glamira.com/diamond-necklaces/price-1000-1410692/diamond/",
        "https://www.glamira.com/diamond-earrings/price-1000-841591/diamond/",
    ]
    asyncio.run(scrape_glamira(categories))
