# import csv
# import json
# import time
# import random
# from concurrent.futures import ThreadPoolExecutor
# from queue import Queue
# from threading import Lock
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.common.exceptions import TimeoutException
# from bs4 import BeautifulSoup
# from tqdm import tqdm
# import os

# # --- CONFIG ---
# INPUT_CSV = "rarecarat/products_list.csv"      # input file with product URLs
# OUTPUT_CSV = "rarecarat/product_details.csv"       # results file
# FAILED_CSV = "rarecarat/failed_urls.csv"          # failed URLs will be retried here
# MAX_RETRIES = 3
# NUM_THREADS = 3
# BATCH_SIZE = 30

# lock = Lock()
# progress_lock = Lock()

# # --- Selenium driver setup ---
# def init_driver():
#     options = webdriver.ChromeOptions()
#     options.add_argument(
#         "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
#         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36"
#     )
#     options.add_argument("--disable-blink-features=AutomationControlled")
#     options.add_argument("--disable-gpu")
#     options.add_argument("--disable-extensions")
#     options.add_argument("--no-sandbox")
#     options.add_argument("--headless=new")
#     driver = webdriver.Chrome(options=options)
#     return driver

# def scroll_into_view_safely(driver, element):
#     driver.execute_script(
#         "arguments[0].scrollIntoView({behavior:'smooth', block:'center'});"
#     )
#     time.sleep(random.uniform(0.5, 1.0))

# def expand_sections(driver):
#     """Expand all collapsible sections and wait for content to load."""
#     sections = driver.find_elements(By.CSS_SELECTOR, "div.q-expansion-item__container")
#     for section in sections:
#         try:
#             header = section.find_element(By.CSS_SELECTOR, "div.q-item--clickable")
#             if header.get_attribute("aria-expanded") == "false":
#                 driver.execute_script("arguments[0].click();", header)
#                 # Wait until at least one product-faq-item appears inside this section
#                 WebDriverWait(section, 5).until(
#                     EC.presence_of_element_located(
#                         (By.CSS_SELECTOR, "div.product-faq-item")
#                     )
#                 )
#                 time.sleep(random.uniform(0.5, 1.0))
#         except TimeoutException:
#             continue
#         except:
#             continue

# def scrape_single_product(driver, row):
#     url = row["url"]
#     for attempt in range(1, MAX_RETRIES + 1):
#         try:
#             driver.get(url)
#             WebDriverWait(driver, 15).until(
#                 EC.presence_of_element_located((By.TAG_NAME, "body"))
#             )

#             expand_sections(driver)

#             # Wait for at least one detail item or metal info
#             try:
#                 WebDriverWait(driver, 10).until(
#                     EC.presence_of_element_located(
#                         (By.CSS_SELECTOR, "div.product-faq-item, p.ng-mt-16 span.ng-text-18-24")
#                     )
#                 )
#             except TimeoutException:
#                 print(f"⚠️ {url} timed out waiting for details, retrying...")
#                 time.sleep(random.uniform(1,2))
#                 continue

#             soup = BeautifulSoup(driver.page_source, "html.parser")

#             # Extract gold_carat and metal_type
#             details = {}
#             metal_info = soup.select_one("p.ng-mt-16 span.ng-text-18-24")
#             if metal_info:
#                 metal_text = metal_info.get_text(strip=True)
#                 if " " in metal_text:
#                     gold_carat, metal_type = metal_text.split(" ", 1)
#                 else:
#                     gold_carat, metal_type = metal_text, ""
#                 details["gold_carat"] = gold_carat
#                 details["metal_type"] = metal_type

#             # Extract all product-faq-item details
#             for item in soup.select("div.product-faq-item"):
#                 try:
#                     key = item.find_all("div")[0].get_text(strip=True).rstrip(":")
#                     value = item.find_all("div")[1].get_text(strip=True)
#                     details[key] = value
#                 except:
#                     continue

#             # --- Retry if only gold/metal info present ---
#             if len(details) <= 2:  # only gold_carat and metal_type
#                 return None

#             return {
#                 "name": row.get("name", "N/A"),
#                 "price": row.get("price", "N/A"),
#                 "url": url,
#                 "details": json.dumps(details, ensure_ascii=False)
#             }

#         except Exception as e:
#             print(f"❌ {url} attempt {attempt} failed: {e}")
#             time.sleep(random.uniform(1, 2))

#     return None

# def worker(queue: Queue, writer, failed_writer, pbar):
#     processed = 0
#     driver = init_driver()
#     while not queue.empty():
#         row = queue.get()
#         result = scrape_single_product(driver, row)

#         with lock:
#             if result:
#                 writer.writerow(result)
#             else:
#                 failed_writer.writerow(row)

#         with progress_lock:
#             pbar.update(1)

#         processed += 1
#         queue.task_done()

#         if processed % BATCH_SIZE == 0:
#             driver.quit()
#             driver = init_driver()
#     driver.quit()

# def process_csv(csv_file, output_file, failed_file):
#     if not os.path.exists(csv_file) or os.path.getsize(csv_file) == 0:
#         print(f"⚠️ Skipping {csv_file}, file missing or empty")
#         return

#     with open(csv_file, newline="", encoding="utf-8") as f:
#         reader = list(csv.DictReader(f))
#     if not reader:
#         print(f"⚠️ No rows found in {csv_file}")
#         return

#     task_queue = Queue()
#     for row in reader:
#         task_queue.put(row)

#     with open(output_file, "a", newline="", encoding="utf-8") as out_f, \
#          open(failed_file, "w", newline="", encoding="utf-8") as fail_f:

#         fieldnames = ["name", "price", "url", "details"]
#         writer = csv.DictWriter(out_f, fieldnames=fieldnames)
#         failed_writer = csv.DictWriter(fail_f, fieldnames=reader[0].keys())

#         if out_f.tell() == 0:
#             writer.writeheader()
#         failed_writer.writeheader()

#         pbar = tqdm(total=len(reader), desc=f"Processing CSV", ncols=100)

#         with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
#             for _ in range(NUM_THREADS):
#                 executor.submit(worker, task_queue, writer, failed_writer, pbar)

#         task_queue.join()
#         pbar.close()

# if __name__ == "__main__":
#     process_csv(INPUT_CSV, OUTPUT_CSV, FAILED_CSV)

#     # Retry failed URLs
#     attempt = 1
#     while os.path.exists(FAILED_CSV) and os.path.getsize(FAILED_CSV) > 0:
#         print(f"\n🔁 Retry attempt {attempt} for failed URLs...")
#         process_csv(FAILED_CSV, OUTPUT_CSV, FAILED_CSV)
#         attempt += 1
#         if attempt > 5:
#             print("⚠️ Max retry attempts reached. Some URLs may still fail.")
#             break

#     print(f"\n🎉 All done! Results → {OUTPUT_CSV}, remaining failed URLs → {FAILED_CSV}")


import asyncio
import csv
import json
import random
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup
from tqdm import tqdm
import os

# --- CONFIG ---
INPUT_CSV = "rarecarat/products_list.csv"
OUTPUT_CSV = "rarecarat/product_details.csv"
FAILED_CSV = "rarecarat/failed_urls.csv"

MAX_RETRIES = 3
CONCURRENT_PAGES = 10  # number of parallel pages
INITIAL_WAIT = 3       # seconds, first attempt
RETRY_WAIT = 8         # seconds, for retries

lock = asyncio.Lock()

async def extract_product_details(page, row, retry=False):
    url = row["url"]
    wait_time = RETRY_WAIT if retry else INITIAL_WAIT

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await page.goto(url, timeout=60000)
            await page.wait_for_load_state("domcontentloaded", timeout=60000)
            await asyncio.sleep(wait_time + random.uniform(0.2, 0.5))

            # Expand collapsible sections
            expand_buttons = await page.query_selector_all("div.q-item--clickable")
            for btn in expand_buttons:
                aria_expanded = await btn.get_attribute("aria-expanded")
                if aria_expanded == "false":
                    try:
                        await btn.click()
                        await asyncio.sleep(0.3 + random.random() * 0.2)
                    except:
                        continue

            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")

            details = {}

            # Gold / Metal
            metal_info = soup.select_one("p.ng-mt-16 span.ng-text-18-24")
            if metal_info:
                metal_text = metal_info.get_text(strip=True)
                if " " in metal_text:
                    gold_carat, metal_type = metal_text.split(" ", 1)
                else:
                    gold_carat, metal_type = metal_text, ""
                details["gold_carat"] = gold_carat
                details["metal_type"] = metal_type

            # Product details from collapsible section
            for item in soup.select("div.product-faq-item"):
                try:
                    key = item.find_all("div")[0].get_text(strip=True).rstrip(":")
                    value = item.find_all("div")[1].get_text(strip=True)
                    details[key] = value
                except:
                    continue

            if len(details) <= 2:
                # Only gold/metal info → mark for retry
                return None

            return {
                "name": row.get("name", "N/A"),
                "price": row.get("price", "N/A"),
                "url": url,
                "details": json.dumps(details, ensure_ascii=False)
            }

        except PlaywrightTimeoutError:
            print(f"⏱ Timeout: {url} attempt {attempt}")
        except Exception as e:
            print(f"❌ {url} attempt {attempt} failed: {e}")

        await asyncio.sleep(random.uniform(1, 2))
    return None

async def worker(task_queue, writer, failed_writer, pbar, retry=False):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        pages = [await browser.new_page() for _ in range(CONCURRENT_PAGES)]

        while task_queue:
            tasks = []
            current_rows = []
            for page in pages:
                if not task_queue:
                    break
                row = task_queue.pop(0)
                current_rows.append(row)
                tasks.append(extract_product_details(page, row, retry))

            results = await asyncio.gather(*tasks)
            async with lock:
                for row, result in zip(current_rows, results):
                    if result:
                        writer.writerow(result)
                    else:
                        failed_writer.writerow(row)
                    pbar.update(1)

        await browser.close()

async def process_csv(csv_file, output_file, failed_file, retry=False):
    if not os.path.exists(csv_file) or os.path.getsize(csv_file) == 0:
        print(f"⚠️ Skipping {csv_file}, missing or empty")
        return

    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
    if not reader:
        print(f"⚠️ No rows in {csv_file}")
        return

    task_queue = reader.copy()

    with open(output_file, "a", newline="", encoding="utf-8") as out_f, \
         open(failed_file, "w", newline="", encoding="utf-8") as fail_f:

        fieldnames = ["name", "price", "url", "details"]
        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        failed_writer = csv.DictWriter(fail_f, fieldnames=reader[0].keys())

        if out_f.tell() == 0:
            writer.writeheader()
        failed_writer.writeheader()

        pbar = tqdm(total=len(reader), desc=f"Processing {os.path.basename(csv_file)}", ncols=100)
        await worker(task_queue, writer, failed_writer, pbar, retry)
        pbar.close()

async def main():
    # 1️⃣ Fast initial pass
    await process_csv(INPUT_CSV, OUTPUT_CSV, FAILED_CSV, retry=False)

    # 2️⃣ Retry failed URLs with longer waits
    attempt = 1
    while os.path.exists(FAILED_CSV) and os.path.getsize(FAILED_CSV) > 0:
        print(f"\n🔁 Retry attempt {attempt} for failed URLs...")
        await process_csv(FAILED_CSV, OUTPUT_CSV, FAILED_CSV, retry=True)
        attempt += 1
        if attempt > 5:
            print("⚠️ Max retries reached. Some URLs may still fail.")
            break

    print(f"\n🎉 Done! Results → {OUTPUT_CSV}, remaining failed URLs → {FAILED_CSV}")

if __name__ == "__main__":
    asyncio.run(main())
