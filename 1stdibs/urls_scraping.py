# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# import time
# import csv

# # --- CONFIG ---
# BASE_URL = "https://www.1stdibs.com/jewelry/rings/?currency=usd&price=[1000%20TO%20*]"
# TARGET_PRODUCTS = 10  # changeable
# PAGE_PAUSE = 2        # seconds to wait after opening each page

# # --- SETUP ---
# options = Options()
# options.add_argument("--window-size=1920,1080")
# # options.add_argument("--headless=new")
# driver = webdriver.Chrome(options=options)

# all_products = []
# seen_urls = set()
# page = 1

# while len(all_products) < TARGET_PRODUCTS:
#     url = BASE_URL if page == 1 else f"https://www.1stdibs.com/jewelry/rings/?currency=usd&page={page}&price=[1000%20TO%20*]"
#     print(f"Opening page {page} -> {url}")
#     driver.get(url)
#     time.sleep(PAGE_PAUSE)  # wait for products to load

#     # Wait for at least one product to appear
#     try:
#         WebDriverWait(driver, 10).until(
#             EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-tn='item-tile-wrapper']"))
#         )
#     except:
#         print("No products found on this page. Stopping.")
#         break

#     # Collect products (no slow scrolling needed)
#     products = driver.find_elements(By.CSS_SELECTOR, "div[data-tn='item-tile-wrapper']")
#     print(f"Page {page}: found {len(products)} products")

#     for p in products:
#         try:
#             url_el = p.find_element(By.CSS_SELECTOR, "a[data-tn^='item-tile-title-anchor']")
#             url = url_el.get_attribute("href")
#             if url in seen_urls:
#                 continue  # skip duplicates

#             title_el = p.find_element(By.CSS_SELECTOR, "a[data-tn^='item-tile-title-anchor'] h2")
#             price_el = p.find_element(By.CSS_SELECTOR, "div[data-tn='price']")

#             title = title_el.text.strip()
#             price = price_el.text.strip()
#             source = "1stdibs"

#             all_products.append((title, price, url, source))
#             seen_urls.add(url)

#             if len(all_products) >= TARGET_PRODUCTS:
#                 break
#         except:
#             continue

#     print(f"Total products scraped so far: {len(all_products)}")

#     if len(products) == 0 or len(all_products) >= TARGET_PRODUCTS:
#         break  # stop if no products found or target reached

#     page += 1  # next page

# # --- SAVE CSV ---
# with open("1stdibs/products_list.csv", "w", newline="", encoding="utf-8") as f:
#     writer = csv.writer(f)
#     writer.writerow(["Title", "Price", "URL", "Source"])
#     writer.writerows(all_products)

# print(f"Scraping completed. Total products: {len(all_products)}")
# driver.quit()



from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse
import time
import csv

# --- CONFIG ---
CATEGORY_URLS = [
    "https://www.1stdibs.com/jewelry/rings/?currency=usd&price=[1000 TO *]",
    "https://www.1stdibs.com/jewelry/earrings/?currency=usd&price=[1000 TO *]",
    "https://www.1stdibs.com/jewelry/bracelets/?currency=usd&price=[1000 TO *]",
    "https://www.1stdibs.com/jewelry/necklaces/?currency=usd&price=[1000 TO *]"
]
TARGET_PRODUCTS_PER_CATEGORY = 300  # changeable
PAGE_PAUSE = 2  # seconds to wait after opening each page
EXCLUDE_KEYWORDS = ["synthetic", "man-made", "lab grown"]

# --- SETUP SELENIUM ---
options = Options()
options.add_argument("--window-size=1920,1080")
# options.add_argument("--headless=new")
driver = webdriver.Chrome(options=options)

all_products = []
seen_urls = set()

# --- HELPER TO BUILD PAGE URL SAFELY ---
def build_page_url(base_url, page_number):
    parsed = urlparse(base_url)
    query = parse_qs(parsed.query)
    query["page"] = [str(page_number)]
    new_query = urlencode(query, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

# --- LOOP THROUGH CATEGORIES ---
for category_url in CATEGORY_URLS:
    page = 1
    category_scraped = 0

    while category_scraped < TARGET_PRODUCTS_PER_CATEGORY:
        url = build_page_url(category_url, page)
        print(f"Opening page {page} -> {url}")
        driver.get(url)
        time.sleep(PAGE_PAUSE)

        # Wait for products to appear
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-tn='item-tile-wrapper']"))
            )
        except:
            print("No products found on this page. Stopping category.")
            break

        products = driver.find_elements(By.CSS_SELECTOR, "div[data-tn='item-tile-wrapper']")
        if not products:
            break

        for p in products:
            try:
                url_el = p.find_element(By.CSS_SELECTOR, "a[data-tn^='item-tile-title-anchor']")
                url = url_el.get_attribute("href")
                if url in seen_urls:
                    continue  # skip duplicates

                title_el = p.find_element(By.CSS_SELECTOR, "a[data-tn^='item-tile-title-anchor'] h2")
                price_el = p.find_element(By.CSS_SELECTOR, "div[data-tn='price']")

                title = title_el.text.strip()
                price = price_el.text.strip()
                source = "1stdibs"

                # Filter out synthetic/lab-grown diamonds
                if any(keyword.lower() in title.lower() for keyword in EXCLUDE_KEYWORDS):
                    continue

                all_products.append((title, price, url, source))
                seen_urls.add(url)
                category_scraped += 1

                if category_scraped >= TARGET_PRODUCTS_PER_CATEGORY:
                    break
            except:
                continue

        if category_scraped >= TARGET_PRODUCTS_PER_CATEGORY:
            break

        page += 1  # next page

# --- SAVE CSV ---
with open("1stdibs/products_list.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Title", "Price", "URL", "Source"])
    writer.writerows(all_products)

print(f"Scraping completed. Total products: {len(all_products)}")
driver.quit()
