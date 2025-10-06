import requests
import csv
import argparse

# Kay Jewellers Unbxd API
api_base = "https://search.unbxd.io/18d57de5077c063f9223fc48dfd31820/ss-unbxd-gus-prod-kay27631718722879/category"

# Category IDs (add as needed)
category_ids = [
    9000000070,  # women rings
    9000000071,  # men rings
    9000000118,  # women necklaces
    9000000119,  # men necklaces
    9000000197,  # women's earrings
    9000000198,  # men's earrings
    9000000157,  # women's bracelets
    9000000158,  # men's bracelets,
]

# Keywords to filter out unwanted products
EXCLUDE_KEYWORDS = ["lab-grown", "lab grown", "synthetic", "man made", "created diamond"]

MIN_PRICE = 1000  # filter: only take >= $1000


def scrape(limit: int):
    all_products = {}

    for cat_id in category_ids:
        print(f"\n🚀 Scraping category {cat_id} (limit {limit}) ...")
        start = 0
        rows = 42
        scraped_count = 0

        while scraped_count < limit:
            params = {
                "p": f"v_categoryPathId:{cat_id}",
                "pagetype": "boolean",
                "version": "V2",
                "start": str(start),
                "rows": str(rows),
                "format": "json",
                "user-type": "first-time",
                "fields": "v_title,v_price,v_url,v_productUrl,v_sku",
            }
            try:
                response = requests.get(api_base, params=params, timeout=15)
                data = response.json()
            except Exception as e:
                print(f"⚠️ Error fetching {cat_id} @ {start}: {e}")
                break

            products = data.get("response", {}).get("products", [])
            if not products:
                break

            for p in products:
                if scraped_count >= limit:
                    break

                v = p.get("variants", [{}])[0]
                title = v.get("v_title", "").strip()

                # Skip excluded keywords
                if any(kw in title.lower() for kw in EXCLUDE_KEYWORDS):
                    continue

                url = v.get("v_url") or v.get("v_productUrl")
                if not url:
                    continue
                full_url = "https://www.kay.com" + url

                # Deduplicate by URL
                if full_url in all_products:
                    continue

                try:
                    price = int(float(v.get("v_price", 0)))
                except Exception:
                    price = 0

                # Skip if price < MIN_PRICE
                if price < MIN_PRICE:
                    continue

                all_products[full_url] = {
                    "name": title,
                    "price": price,
                    "url": full_url,
                }

                scraped_count += 1

            start += rows

        print(f"✅ Finished category {cat_id} with {scraped_count} products (>= ${MIN_PRICE})")

    # Save to CSV
    with open("kay_jewelers/products_list.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "price", "url"])
        writer.writeheader()
        writer.writerows(all_products.values())

    print(f"\n🎉 Done! Total unique products scraped: {len(all_products)}")
    print("✅ CSV saved as products_list.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Kay Jewellers categories")
    parser.add_argument("--limit", type=int, default=200, help="Max products per category (default: 200)")
    args = parser.parse_args()

    scrape(limit=args.limit)
