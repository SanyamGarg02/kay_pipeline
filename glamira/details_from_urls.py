import csv
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Lock
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from tqdm import tqdm
import os

# --- CONFIG ---
INPUT_CSV = "glamira/glamira_products_list.csv"      # input file with product URLs
OUTPUT_CSV = "glamira/poc_glamira_final.csv"         # main results file
FAILED_CSV = "glamira/failed_urls.csv"               # failed URLs will be retried here
MAX_RETRIES = 3                              # per URL
NUM_THREADS = 3                              # adjust based on machine
BATCH_SIZE = 30                              # refresh driver after this many

lock = Lock()
progress_lock = Lock()

# --- Selenium driver setup ---
def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36"
    )
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.add_argument("--headless=new")
    driver = webdriver.Chrome(options=options)
    return driver

def scroll_page(driver):
    """Scroll through the page to trigger lazy loading."""
    last_height = driver.execute_script("return document.body.scrollHeight")
    for y in range(0, last_height, 400):
        driver.execute_script(f"window.scrollTo(0, {y});")
        time.sleep(random.uniform(0.3, 0.6))

def scrape_single_product(driver, row):
    url = row["url"]
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            driver.get(url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            scroll_page(driver)

            soup = BeautifulSoup(driver.page_source, "html.parser")

            # 🔍 Glamira product details are in .detail-content-wrap
            detail_wraps = soup.select("div.detail-content-wrap table.data-table.table-detail")
            product_details = {}

            for table in detail_wraps:
                # Some tables don’t have headings, so we just use "General", "Stone", etc.
                section_title = table.find_previous("span", class_="title")
                header = section_title.get_text(strip=True) if section_title else "Unknown Section"

                rows = {}
                for tr in table.select("tr"):
                    label = tr.select_one("td.detail-label")
                    value = tr.select_one("td.detail-value")
                    if label and value:
                        rows[label.get_text(strip=True)] = value.get_text(strip=True)
                if rows:
                    product_details[header] = rows

            if not product_details:
                raise ValueError("No product details extracted")

            return {
                "name": row["name"],
                "price": row.get("price", "N/A"),
                "url": url,
                "details": json.dumps(product_details, ensure_ascii=False),
            }

        except Exception as e:
            print(f"❌ {url} attempt {attempt} failed: {e}")
            time.sleep(random.uniform(1, 2))
    return None

def worker(queue: Queue, writer, failed_writer, pbar):
    processed = 0
    driver = init_driver()
    while not queue.empty():
        row = queue.get()
        result = scrape_single_product(driver, row)

        with lock:
            if result:
                writer.writerow(result)
            else:
                failed_writer.writerow(row)

        with progress_lock:
            pbar.update(1)

        processed += 1
        queue.task_done()

        if processed % BATCH_SIZE == 0:
            driver.quit()
            driver = init_driver()
    driver.quit()

def process_csv(csv_file, output_file, failed_file):
    if not os.path.exists(csv_file) or os.path.getsize(csv_file) == 0:
        print(f"⚠️ Skipping {csv_file}, file missing or empty")
        return

    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
    if not reader:
        print(f"⚠️ No rows found in {csv_file}")
        return

    task_queue = Queue()
    for row in reader:
        task_queue.put(row)

    with open(output_file, "a", newline="", encoding="utf-8") as out_f, \
         open(failed_file, "w", newline="", encoding="utf-8") as fail_f:  # overwrite fails

        fieldnames = ["name", "price", "url", "details"]
        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        failed_writer = csv.DictWriter(fail_f, fieldnames=reader[0].keys())

        if out_f.tell() == 0:
            writer.writeheader()
        failed_writer.writeheader()

        pbar = tqdm(total=len(reader), desc=f"Processing {os.path.basename(csv_file)}", ncols=100)

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            for _ in range(NUM_THREADS):
                executor.submit(worker, task_queue, writer, failed_writer, pbar)

        task_queue.join()
        pbar.close()

if __name__ == "__main__":
    process_csv(INPUT_CSV, OUTPUT_CSV, FAILED_CSV)

    # Retry failed URLs until none remain or max attempts reached
    attempt = 1
    while os.path.exists(FAILED_CSV) and os.path.getsize(FAILED_CSV) > 0:
        print(f"\n🔁 Retry attempt {attempt} for failed URLs...")
        process_csv(FAILED_CSV, OUTPUT_CSV, FAILED_CSV)
        attempt += 1
        if attempt > 5:  # safeguard
            print("⚠️ Max retry attempts reached. Some URLs may still fail.")
            break

    print(f"\n🎉 All done! Results → {OUTPUT_CSV}, remaining failed URLs → {FAILED_CSV}")
