import csv
import time
import random
import re
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
INPUT_CSV = "1stdibs/products_list.csv"
OUTPUT_CSV = "1stdibs/poc_1stdibs_normalized.csv"
FAILED_CSV = "1stdibs/failed_urls.csv"
MAX_RETRIES = 3
NUM_THREADS = 3
BATCH_SIZE = 30  # restart driver after this many processed by a worker

lock = Lock()
progress_lock = Lock()

# Final normalized output columns (order matters!)
FIELDNAMES = [
    "name",
    "price",
    "url",
    "stone_type",
    "stone_shape",
    "stone_clarity",
    "stone_color",
    "stone_carat_weight",
    "metal_type",
    "metal_color",
    "gold_karat",
    "ring_size",
    "category",
    "source",
]

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
    # options.add_argument("--headless=new")  # comment this if you want to SEE Chrome
    driver = webdriver.Chrome(options=options)
    return driver

def scroll_page(driver):
    last_height = driver.execute_script("return document.body.scrollHeight") or 0
    step = 400
    for y in range(0, last_height + step, step):
        driver.execute_script(f"window.scrollTo(0, {y});")
        time.sleep(random.uniform(0.2, 0.5))
    time.sleep(random.uniform(0.5, 1.0))

# --- Regex patterns ---
CLARITY_RE = re.compile(r"\b(IF|VVS1|VVS2|VS1|VS2|SI1|SI2|I1|I2|I3)\b", re.I)
CARAT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:ct|carat)", re.I)
GOLD_KARAT_RE = re.compile(r"(\d+)\s?[kK]\b")
COLOR_GRADE_RE = re.compile(r"\b([D-M])\s*(?:color)?\b", re.I)

def text_or_none(elem):
    return elem.get_text(" ", strip=True) if elem else ""

def infer_category(row):
    name = (row.get("Title") or "").lower()
    url = (row.get("URL") or "").lower()
    text = f"{name} {url}"

    # Order matters: check earrings first (to avoid 'ring' confusion)
    if any(word in text for word in ["earring", "stud", "hoop", "drop"]):
        return "Earring"
    elif any(word in text for word in ["bracelet", "bangle", "cuff"]):
        return "Bracelet"
    elif any(word in text for word in ["necklace", "pendant", "chain"]):
        return "Necklace"
    elif any(word in text for word in ["ring", "engagement", "band"]):
        return "Ring"
    else:
        return "Unknown"

def infer_from_text(combined, dest):
    # gold karat
    if not dest.get("gold_karat"):
        m = GOLD_KARAT_RE.search(combined)
        if m:
            dest["gold_karat"] = f"{m.group(1)}k"

    # stone carat weight
    if not dest.get("stone_carat_weight"):
        m = CARAT_RE.search(combined)
        if m:
            dest["stone_carat_weight"] = m.group(1)

    # clarity
    if not dest.get("stone_clarity"):
        m = CLARITY_RE.search(combined)
        if m:
            dest["stone_clarity"] = m.group(1).upper()

    # color grade
    if not dest.get("stone_color"):
        m = COLOR_GRADE_RE.search(combined)
        if m:
            dest["stone_color"] = m.group(1).upper()

def parse_details_section(soup, title_text):
    out = {k: "" for k in FIELDNAMES}  # init with empty strings
    details_container = soup.find(attrs={"data-tn": "pdp-details"})
    if not details_container:
        details_container = soup.find("section", {"data-test": "item-details"}) or soup

    details_text_accum = []

    # ring size
    ring_elem = details_container.find(attrs={"data-tn": "pdp-spec-ring-size"})
    if ring_elem:
        text = text_or_none(ring_elem)
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)", text)
        if m:
            out["ring_size"] = m.group(1)
        details_text_accum.append(text)

    # metal
    metal_elem = details_container.find(attrs={"data-tn": "pdp-spec-metal"})
    if metal_elem:
        metal_text = text_or_none(metal_elem)
        out["metal_type"] = metal_text
        lower = metal_text.lower()
        if "white" in lower:
            out["metal_color"] = "White"
        elif "yellow" in lower:
            out["metal_color"] = "Yellow"
        elif "rose" in lower:
            out["metal_color"] = "Rose"
        m = GOLD_KARAT_RE.search(metal_text)
        if m:
            out["gold_karat"] = f"{m.group(1)}k"
        if "platinum" in lower:
            out["metal_type"] = "Platinum"
        details_text_accum.append(metal_text)

    # stone type
    stone_elem = details_container.find(attrs={"data-tn": "pdp-spec-stone"})
    if stone_elem:
        stone_text = text_or_none(stone_elem)
        out["stone_type"] = stone_text
        details_text_accum.append(stone_text)

    # stone cut / shape
    shape_elem = details_container.find(attrs={"data-tn": "pdp-spec-stone-cut"}) or \
                 details_container.find(attrs={"data-tn": "pdp-spec-stone-shape"})
    if shape_elem:
        shape_text = text_or_none(shape_elem)
        out["stone_shape"] = re.sub(r"\bcut\b", "", shape_text, flags=re.I).strip()
        details_text_accum.append(shape_text)

    # parse <li> items
    for li in details_container.find_all("li"):
        li_txt = text_or_none(li)
        if li_txt:
            details_text_accum.append(li_txt)
            if not out["stone_clarity"]:
                m = CLARITY_RE.search(li_txt)
                if m:
                    out["stone_clarity"] = m.group(1).upper()
            if not out["stone_carat_weight"]:
                m = CARAT_RE.search(li_txt)
                if m:
                    out["stone_carat_weight"] = m.group(1)
            if not out["stone_color"]:
                m = re.search(r"\b([D-M])\s*(?:color)?\b", li_txt, re.I)
                if m:
                    out["stone_color"] = m.group(1).upper()

    combined = f"{title_text} {' '.join(details_text_accum)}"
    infer_from_text(combined, out)

    return out

# --- Scraping logic ---
def scrape_single_product(driver, row):
    url = row["URL"]
    title_text = row.get("Title", "")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            driver.get(url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            scroll_page(driver)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            normalized = parse_details_section(soup, title_text)

            result = {
                "name": row.get("Title", ""),
                "price": row.get("Price", ""),
                "url": url,
                "stone_type": normalized.get("stone_type", ""),
                "stone_shape": normalized.get("stone_shape", ""),
                "stone_clarity": normalized.get("stone_clarity", ""),
                "stone_color": normalized.get("stone_color", ""),
                "stone_carat_weight": normalized.get("stone_carat_weight", ""),
                "metal_type": normalized.get("metal_type", ""),
                "metal_color": normalized.get("metal_color", ""),
                "gold_karat": normalized.get("gold_karat", ""),
                "ring_size": normalized.get("ring_size", ""),
                "category": infer_category(row),
                "source": row.get("Source", ""),
            }
            return result

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
            try:
                driver.quit()
            except:
                pass
            driver = init_driver()
    try:
        driver.quit()
    except:
        pass

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

    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(failed_file) or ".", exist_ok=True)

    with open(output_file, "a", newline="", encoding="utf-8") as out_f, \
         open(failed_file, "w", newline="", encoding="utf-8") as fail_f:

        writer = csv.DictWriter(out_f, fieldnames=FIELDNAMES)
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

    attempt = 1
    while os.path.exists(FAILED_CSV) and os.path.getsize(FAILED_CSV) > 0:
        print(f"\n🔁 Retry attempt {attempt} for failed URLs...")
        process_csv(FAILED_CSV, OUTPUT_CSV, FAILED_CSV)
        attempt += 1
        if attempt > 5:
            print("⚠️ Max retry attempts reached. Some URLs may still fail.")
            break

    print(f"\n🎉 All done! Results → {OUTPUT_CSV}, remaining failed URLs → {FAILED_CSV}")
