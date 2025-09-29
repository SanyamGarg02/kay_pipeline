import requests
import csv

# RareCarat /search/jewellery API endpoint
API_URL = "https://webapi.rarecarat.com/search/jewellery"

# Category IDs and corresponding URL bases
CATEGORY_MAP = {
    "6efd564c-befd-4c96-ad24-d75f89d3da25": "https://www.rarecarat.com/wedding-ring",          # women engagement rings
    "28657fdc-9791-440d-a18d-5d48450f22bc": "https://www.rarecarat.com/wedding-ring",          # mens wedding bands
    "a131e105-dd64-4c22-ac2b-d9b956685554": "https://www.rarecarat.com/fine-jewelry",          # earrings
    "6d335c87-7718-44e0-a17e-0c1b586e92fd": "https://www.rarecarat.com/fine-jewelry",          # necklaces
    "039d9bb7-6368-492d-89c0-69c09a4b3098": "https://www.rarecarat.com/fine-jewelry"           # bracelets
}

# MAX_PER_CATEGORY = 200
MIN_PRICE = 1000

all_products = {}

for CATEGORY_ID, base_url in CATEGORY_MAP.items():
    print(f"\n🚀 Scraping category {CATEGORY_ID} ...")
    page_number = 1
    page_size = 50
    scraped_count = 0

    while True:
        payload = {
            "id": CATEGORY_ID,
            "pageNumber": page_number,
            "pageSize": page_size
        }

        try:
            response = requests.post(API_URL, json=payload, timeout=20)
            data = response.json()
        except Exception as e:
            print(f"⚠️ Error fetching page {page_number}: {e}")
            break

        products = data.get("jewelleries", [])
        if not products:
            break

        # for p in products:
        #     if scraped_count >= MAX_PER_CATEGORY:
        #         break

        for p in products:
            name = p.get("name", "").strip()
            included = False

            # Check Diamond type variants
            for v in p.get("variants", []):
                if v.get("variantLabel") == "Diamond type":
                    for vi in v.get("variantInformations", []):
                        if vi.get("variantValue", "").lower() != "natural":
                            continue

                        price = vi.get("price")
                        slug = vi.get("slug")
                        related_id = vi.get("relatedId")
                        if not slug or not related_id or not price:
                            continue
                        if price < MIN_PRICE:
                            continue

                        url = f"{base_url}/{slug}/{related_id}"

                        if url in all_products:
                            continue

                        all_products[url] = {
                            "name": name,
                            "price": price,
                            "url": url,
                            "source": "rarecarat"
                        }
                        scraped_count += 1
                        included = True

            # Fallback: no Diamond type variants, check 'lab' in name
            if not included:
                if "lab" in name.lower():
                    continue

                metals = p.get("metals", [])
                if metals:
                    m = metals[0]
                    price = m.get("price")
                    related_id = m.get("id")
                    slug = p.get("id")
                    if not slug or not related_id or not price:
                        continue
                    if price < MIN_PRICE:
                        continue

                    url = f"{base_url}/{slug}/{related_id}"
                    if url in all_products:
                        continue

                    all_products[url] = {
                        "name": name,
                        "price": price,
                        "url": url,
                        "source": "rarecarat"
                    }
                    scraped_count += 1

        print(f"Category {CATEGORY_ID}, Page {page_number}: total scraped so far {scraped_count}")
        page_number += 1

print(f"\n✅ Finished scraping all categories, total products: {len(all_products)}")

# Save CSV
with open("rarecarat/products_list.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["name", "price", "url", "source"])
    writer.writeheader()
    writer.writerows(all_products.values())

print("✅ CSV saved as rarecarat/products_list.csv")
